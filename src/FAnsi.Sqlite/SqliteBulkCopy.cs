using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using FAnsi.Connections;
using FAnsi.Discovery;
using Microsoft.Data.Sqlite;

namespace FAnsi.Implementations.Sqlite;

/// <summary>
///     Bulk insert implementation for SQLite. Inserts rows into SQLite tables using batched parameterized
///     INSERT commands for better performance and safety.
/// </summary>
/// <param name="targetTable">The table to insert into</param>
/// <param name="connection">The managed database connection</param>
/// <param name="culture">The culture for value conversion</param>
/// <remarks>
///     <para>
///         SQLite doesn't have native bulk copy like SQL Server. This implementation batches multiple
///         INSERT statements in transactions for improved performance.
///     </para>
///     <para>Key features:</para>
///     <list type="bullet">
///         <item>
///             <description>Batched inserts (default 1000 rows per batch)</description>
///         </item>
///         <item>
///             <description>Parameterized queries for safety</description>
///         </item>
///         <item>
///             <description>Line-by-line error diagnostics on failures</description>
///         </item>
///         <item>
///             <description>Automatic type conversion for SQLite compatibility</description>
///         </item>
///     </list>
/// </remarks>
public sealed class SqliteBulkCopy(DiscoveredTable targetTable, IManagedConnection connection, CultureInfo culture)
    : BulkCopy(targetTable, connection, culture)
{
    /// <summary>
    ///     Gets or sets the timeout in seconds for batch insert operations. Default is 30 seconds.
    /// </summary>
    public static int BulkInsertBatchTimeoutInSeconds { get; set; } = 30;

    /// <summary>
    ///     Gets or sets the number of rows to insert per batch. Default is 5000.
    /// </summary>
    /// <remarks>
    ///     SQLite can handle larger batches than some databases due to its simpler architecture.
    ///     5000 rows provides a good balance between performance and memory usage.
    ///     Increase for better performance with large datasets, decrease if experiencing memory issues.
    /// </remarks>
    public static int BatchSize { get; set; } = 5000; // Optimized for SQLite performance

    public override int UploadImpl(DataTable dt)
    {
        // Single-pass validation: empty string to NULL, string length (no decimal validation for SQLite's dynamic typing)
        var mapping = GetMapping(dt.Columns.Cast<DataColumn>());
        PreProcessAndValidate(dt, mapping, true, false);

        try
        {
            return BulkInsertImpl(dt);
        }
        catch (SqliteException e)
        {
            // Attempt line-by-line insert to identify the problematic row
            Exception better;
            try
            {
                better = AttemptLineByLineInsert(e, dt);
            }
            catch (Exception exception)
            {
                // CodeQL[cs/catch-of-all-exceptions]: Intentional - wrapping any investigation failure as AggregateException
                throw new AggregateException(
                    "Failed to bulk insert batch. Line-by-line investigation also failed. InnerException[0] is the original Exception, InnerException[1] is the line-by-line failure.",
                    e, exception);
            }

            throw better;
        }
        catch (DbException e)
        {
            // Attempt line-by-line insert to identify the problematic row
            Exception better;
            try
            {
                better = AttemptLineByLineInsert(e, dt);
            }
            catch (Exception exception)
            {
                // CodeQL[cs/catch-of-all-exceptions]: Intentional - wrapping any investigation failure as AggregateException
                throw new AggregateException(
                    "Failed to bulk insert batch. Line-by-line investigation also failed. InnerException[0] is the original Exception, InnerException[1] is the line-by-line failure.",
                    e, exception);
            }

            throw better;
        }
    }

    private int BulkInsertImpl(DataTable dt)
    {
        var ourTrans = Connection.Transaction == null
            ? Connection.Connection.BeginTransaction(IsolationLevel.ReadUncommitted)
            : null;
        var matchedColumns = GetMapping(dt.Columns.Cast<DataColumn>());
        var affected = 0;

        using var cmd = new SqliteCommand("", (SqliteConnection)Connection.Connection);
        cmd.Transaction = (SqliteTransaction?)(Connection.Transaction ?? ourTrans);

        if (BulkInsertBatchTimeoutInSeconds != 0)
            cmd.CommandTimeout = BulkInsertBatchTimeoutInSeconds;

        // Pre-build static parts of the INSERT statement
        var syntax = TargetTable.GetQuerySyntaxHelper();
        var columnNames = string.Join(",", matchedColumns.Values.Select(c => syntax.EnsureWrapped(c.GetRuntimeName())));
        var baseCommand = $"INSERT INTO {TargetTable.GetFullyQualifiedName()}({columnNames}) VALUES ";

        // Pre-calculate values to avoid repeated calculations in the main loop
        var totalRows = dt.Rows.Count;
        var columnCount = matchedColumns.Count;
        var columnEntries = matchedColumns.ToArray(); // Convert to array for faster enumeration

        // SQLite has a limit on the number of parameters (default 999, configurable up to 32766)
        // We'll use a conservative limit of 900 to stay well under the default limit
        // Calculate effective batch size based on column count to avoid "too many SQL variables" error
        const int maxSqliteParameters = 900;
        var effectiveBatchSize = columnCount > 0 ? Math.Min(BatchSize, maxSqliteParameters / columnCount) : BatchSize;
        effectiveBatchSize = Math.Max(1, effectiveBatchSize); // Ensure at least 1 row per batch

        // Pre-allocate StringBuilder with estimated capacity for better performance
        // Estimate: (parameter_placeholder + comma) * columnCount * batchSize + overhead
        var estimatedClauseCapacity =
            Math.Max(1024, 3 * columnCount * effectiveBatchSize + 100); // @pX format is about 3 chars
        var valueClauses = new StringBuilder(estimatedClauseCapacity);

        var batchRows = 0;
        var parameterIndex = 0;

        // Cache Last() check result to avoid expensive enumeration in each iteration
        var lastRowIndex = totalRows - 1;

        for (var rowIndex = 0; rowIndex < totalRows; rowIndex++)
        {
            var dr = dt.Rows[rowIndex];

            if (batchRows > 0)
                valueClauses.Append(',');

            valueClauses.Append('(');

            // Reuse the pre-allocated column array for faster access
            for (var colIndex = 0; colIndex < columnCount; colIndex++)
            {
                var kvp = columnEntries[colIndex];

                if (colIndex > 0)
                    valueClauses.Append(',');

                var paramName = $"@p{parameterIndex}";
                valueClauses.Append(paramName);

                // Create parameter and add directly to command
                var value = ConvertValueForSQLite(dr[kvp.Key.Ordinal], kvp.Value.DataType?.SQLType);
                var parameter = new SqliteParameter(paramName, value);

                // For DateTime values, explicitly set SqliteType to Text to ensure proper round-trip
                // Microsoft.Data.Sqlite will store as ISO8601 and can retrieve as DateTime
                if (value is DateTime) parameter.SqliteType = SqliteType.Text;

                cmd.Parameters.Add(parameter);

                parameterIndex++;
            }

            valueClauses.Append(')');

            batchRows++;

            // Execute batch when we reach effective batch size or it's the last row
            if (batchRows >= effectiveBatchSize || rowIndex == lastRowIndex)
            {
                cmd.CommandText = baseCommand + valueClauses;
                affected += cmd.ExecuteNonQuery();

                // Reset for next batch - reuse existing objects where possible
                cmd.Parameters.Clear();
                valueClauses.Clear();
                batchRows = 0;
                parameterIndex = 0;
            }
        }

        ourTrans?.Commit();
        return affected;
    }

    /// <summary>
    ///     Creates a new transaction and does one line at a time bulk insertions to determine which line (and value)
    ///     is causing the problem. Transaction is always rolled back.
    /// </summary>
    /// <param name="originalException">The original exception that triggered line-by-line investigation</param>
    /// <param name="dt">The DataTable being inserted</param>
    /// <returns>Enhanced exception with row and column details</returns>
    private Exception AttemptLineByLineInsert(Exception originalException, DataTable dt)
    {
        var line = 1;
        var firstPass = ExceptionToListOfInnerMessages(originalException, true);
        firstPass = firstPass.Replace(Environment.NewLine, $"{Environment.NewLine}\t");
        firstPass = Environment.NewLine + "First Pass Exception:" + Environment.NewLine + firstPass;

        var matchedColumns = GetMapping(dt.Columns.Cast<DataColumn>());

        // Create new connection for investigation to avoid transaction issues
        using var con = (SqliteConnection)TargetTable.Database.Server.GetConnection();
        con.Open();
        var investigationTransaction = con.BeginTransaction(IsolationLevel.ReadUncommitted);

        using var cmd = new SqliteCommand("", con, investigationTransaction);
        if (BulkInsertBatchTimeoutInSeconds != 0)
            cmd.CommandTimeout = BulkInsertBatchTimeoutInSeconds;

        // Pre-build static parts of the single-row INSERT statement for better performance
        var syntax = TargetTable.GetQuerySyntaxHelper();
        var columnNames = string.Join(",", matchedColumns.Values.Select(c => syntax.EnsureWrapped(c.GetRuntimeName())));
        var paramNames = string.Join(",", matchedColumns.Keys.Select((_, i) => $"@p{i}"));
        cmd.CommandText = $"INSERT INTO {TargetTable.GetFullyQualifiedName()}({columnNames}) VALUES ({paramNames})";

        // Pre-allocate parameter list with known capacity for this investigation
        var columnCount = matchedColumns.Count;
        var columnEntries = matchedColumns.ToArray(); // Convert to array for faster enumeration

        // Try each row individually
        var totalRows = dt.Rows.Count;
        for (var rowIndex = 0; rowIndex < totalRows; rowIndex++)
        {
            var dr = dt.Rows[rowIndex];
            try
            {
                cmd.Parameters.Clear();

                // Use pre-allocated column array and indexed loop for better performance
                for (var colIndex = 0; colIndex < columnCount; colIndex++)
                {
                    var kvp = columnEntries[colIndex];
                    var paramName = $"@p{colIndex}";
                    var value = ConvertValueForSQLite(dr[kvp.Key.Ordinal], kvp.Value.DataType?.SQLType);
                    var parameter = new SqliteParameter(paramName, value);

                    // For DateTime values, explicitly set SqliteType to Text to ensure proper round-trip
                    // Microsoft.Data.Sqlite will store as ISO8601 and can retrieve as DateTime
                    if (value is DateTime) parameter.SqliteType = SqliteType.Text;

                    cmd.Parameters.Add(parameter);
                }

                cmd.ExecuteNonQuery();
                line++;
            }
            catch (SqliteException exception)
            {
                // Try to identify which column caused the problem
                var badColumnInfo = IdentifyBadColumn(exception, dr, matchedColumns);

                if (!string.IsNullOrEmpty(badColumnInfo))
                    return new FileLoadException(
                        $"BulkInsert failed on data row {line}. {badColumnInfo}{Environment.NewLine}First Pass: {firstPass}",
                        exception);

                // Fallback: report row number and all values - use pre-allocated array for performance
                var rowValues = string.Join(Environment.NewLine,
                    columnEntries.Select(kvp =>
                        $"  [{kvp.Key.ColumnName}] = {ValueToString(dr[kvp.Key.Ordinal])}"));

                return new FileLoadException(
                    $"Second Pass Exception: Failed to load data row {line}, the following values were rejected by the database:{Environment.NewLine}{rowValues}{firstPass}",
                    exception);
            }
        }

        // All rows worked individually - unexpected!
        investigationTransaction.Rollback();
        return new InvalidOperationException(
            $"Second Pass Exception: Bulk insert failed but when we tried to repeat it a line at a time it worked{firstPass}",
            originalException);
    }

    /// <summary>
    ///     Attempts to identify which column caused the insert failure by examining the exception message
    /// </summary>
    private string? IdentifyBadColumn(Exception exception, DataRow dr,
        Dictionary<DataColumn, DiscoveredColumn> matchedColumns)
    {
        var message = exception.Message;
        var columnEntries = matchedColumns.ToArray(); // Convert to array for better performance

        // SQLite error messages often include column information
        // Common patterns: "constraint failed: table.column", "datatype mismatch"
        foreach (var kvp in columnEntries)
        {
            var sourceColumn = kvp.Key.ColumnName;
            var destColumn = kvp.Value.GetRuntimeName();

            // Check if error message mentions this column
            if (message.Contains(destColumn, StringComparison.OrdinalIgnoreCase) ||
                message.Contains(sourceColumn, StringComparison.OrdinalIgnoreCase))
            {
                var sourceValue = dr[kvp.Key.Ordinal];
                var destDataType = kvp.Value.DataType!.SQLType;

                return $"The complaint was about source column '{sourceColumn}' " +
                       $"which had value '{ValueToString(sourceValue)}', " +
                       $"destination data type was '{destDataType}'. " +
                       $"Error: {message}";
            }
        }

        // Check for common constraint violations - reuse the pre-allocated array
        if (message.Contains("constraint", StringComparison.OrdinalIgnoreCase))
            // Try to extract which column from constraint name or message
            foreach (var kvp in columnEntries)
            {
                var destColumn = kvp.Value.GetRuntimeName();
                if (message.Contains(destColumn, StringComparison.OrdinalIgnoreCase))
                {
                    var sourceValue = dr[kvp.Key.Ordinal];
                    return $"Constraint violation on column '{kvp.Key.ColumnName}' " +
                           $"with value '{ValueToString(sourceValue)}'. Error: {message}";
                }
            }

        return null;
    }

    /// <summary>
    ///     Converts a value to a readable string for error messages
    /// </summary>
    private static string ValueToString(object? value)
    {
        if (value == null || value == DBNull.Value)
            return "<NULL>";

        if (value is byte[] bytes)
            return $"<byte[{bytes.Length}]>";

        var str = value.ToString() ?? "<NULL>";
        return str.Length > 100 ? string.Concat(str.AsSpan(0, 100), "...") : str;
    }

    /// <summary>
    ///     Recursively extracts all exception messages including inner exceptions
    /// </summary>
    private static string ExceptionToListOfInnerMessages(Exception e, bool includeStackTrace = false)
    {
        var message = new StringBuilder(e.Message);
        if (includeStackTrace)
        {
            message.AppendLine();
            message.Append(e.StackTrace);
        }

        if (e.InnerException == null)
            return message.ToString();

        message.AppendLine();
        message.Append(ExceptionToListOfInnerMessages(e.InnerException, includeStackTrace));
        return message.ToString();
    }

    private static object ConvertValueForSQLite(object value, string? targetSqlType = null)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Handle string values that should be parsed as DateTime for datetime/timestamp columns
        if (value is string stringValue && !string.IsNullOrWhiteSpace(stringValue) &&
            !string.IsNullOrEmpty(targetSqlType))
        {
            var normalizedType = targetSqlType.ToUpperInvariant().Split('(')[0].Trim();
            // SQLite uses TEXT for datetime columns
            if (normalizedType is "TEXT" or "DATETIME" or "TIMESTAMP" or "DATE"
                && DateTime.TryParse(stringValue, CultureInfo.InvariantCulture, DateTimeStyles.None,
                    out var parsedDate))
                // Return the DateTime value - Microsoft.Data.Sqlite will handle it correctly
                return parsedDate;
        }

        // Microsoft.Data.Sqlite handles DateTime natively and will convert it appropriately
        // for the column type. It stores as TEXT in ISO8601 format but preserves type information
        // so it can be read back as DateTime.
        // We do NOT convert DateTime to string manually - let the provider handle it.
        return value switch
        {
            // DateTime is handled natively by Microsoft.Data.Sqlite - don't convert to string
            DateTime => value,
            DateOnly dateOnly => dateOnly.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            TimeOnly timeOnly => timeOnly.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture),
            bool b => b ? 1 : 0, // SQLite stores booleans as integers
            Guid g => g.ToString(), // Store GUIDs as text
            _ => value
        };
    }

    /// <summary>
    ///     Uploads data from a DataTable to the target SQLite table.
    /// </summary>
    /// <param name="dt">The DataTable containing data to upload</param>
    /// <returns>The number of rows inserted</returns>
    /// <remarks>
    ///     <para>This method:</para>
    ///     <list type="number">
    ///         <item>
    ///             <description>Converts empty strings to nulls</description>
    ///         </item>
    ///         <item>
    ///             <description>Converts string types to hard types where appropriate</description>
    ///         </item>
    ///         <item>
    ///             <description>Batches inserts for performance</description>
    ///         </item>
    ///         <item>
    ///             <description>Uses parameterized queries for safety</description>
    ///         </item>
    ///         <item>
    ///             <description>Provides detailed error diagnostics on failure</description>
    ///         </item>
    ///     </list>
    /// </remarks>
    public override int Upload(DataTable dt)
    {
        if (dt.Rows.Count == 0)
            return 0;

        // Call base class Upload to get ConvertStringTypesToHardTypes behavior
        return base.Upload(dt);
    }

    // BulkCopy base class handles disposal
}
