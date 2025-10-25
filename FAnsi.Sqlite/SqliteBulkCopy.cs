using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using FAnsi.Connections;
using FAnsi.Discovery;
using Microsoft.Data.Sqlite;

namespace FAnsi.Implementations.Sqlite;

/// <summary>
/// Bulk insert implementation for SQLite. Inserts rows into SQLite tables using batched parameterized
/// INSERT commands for better performance and safety.
/// </summary>
/// <param name="targetTable">The table to insert into</param>
/// <param name="connection">The managed database connection</param>
/// <param name="culture">The culture for value conversion</param>
/// <remarks>
/// <para>SQLite doesn't have native bulk copy like SQL Server. This implementation batches multiple
/// INSERT statements in transactions for improved performance.</para>
/// <para>Key features:</para>
/// <list type="bullet">
/// <item><description>Batched inserts (default 1000 rows per batch)</description></item>
/// <item><description>Parameterized queries for safety</description></item>
/// <item><description>Line-by-line error diagnostics on failures</description></item>
/// <item><description>Automatic type conversion for SQLite compatibility</description></item>
/// </list>
/// </remarks>
public sealed class SqliteBulkCopy(DiscoveredTable targetTable, IManagedConnection connection, CultureInfo culture)
    : BulkCopy(targetTable, connection, culture)
{
    /// <summary>
    /// Gets or sets the timeout in seconds for batch insert operations. Default is 30 seconds.
    /// </summary>
    public static int BulkInsertBatchTimeoutInSeconds { get; set; } = 30;

    /// <summary>
    /// Gets or sets the number of rows to insert per batch. Default is 1000.
    /// </summary>
    /// <remarks>
    /// SQLite can handle larger batches than some databases due to its simpler architecture.
    /// Increase for better performance with large datasets, decrease if experiencing memory issues.
    /// </remarks>
    public static int BatchSize { get; set; } = 1000; // SQLite can handle larger batches than some databases

    public override int UploadImpl(DataTable dt)
    {
        EmptyStringsToNulls(dt);
        ConvertStringTypesToHardTypes(dt);

        try
        {
            return BulkInsertImpl(dt);
        }
        catch (Exception e)
        {
            // Attempt line-by-line insert to identify the problematic row
            Exception better;
            try
            {
                better = AttemptLineByLineInsert(e, dt);
            }
            catch (Exception exception)
            {
                throw new AggregateException(
                    "Failed to bulk insert batch. Line-by-line investigation also failed. InnerException[0] is the original Exception, InnerException[1] is the line-by-line failure.",
                    e, exception);
            }
            throw better;
        }
    }

    private int BulkInsertImpl(DataTable dt)
    {
        var ourTrans = Connection.Transaction == null ? Connection.Connection.BeginTransaction(IsolationLevel.ReadUncommitted) : null;
        var matchedColumns = GetMapping(dt.Columns.Cast<DataColumn>());
        var affected = 0;

        using var cmd = new SqliteCommand("", (SqliteConnection)Connection.Connection);
        cmd.Transaction = (SqliteTransaction?)(Connection.Transaction ?? ourTrans);

        if (BulkInsertBatchTimeoutInSeconds != 0)
            cmd.CommandTimeout = BulkInsertBatchTimeoutInSeconds;

        // Build the parameterized INSERT statement
        var columnNames = string.Join(",", matchedColumns.Values.Select(c => $"[{c.GetRuntimeName()}]"));
        var baseCommand = $"INSERT INTO {TargetTable.GetFullyQualifiedName()}({columnNames}) VALUES ";

        var batchRows = 0;
        var valueClauses = new StringBuilder();
        var parameterIndex = 0;

        foreach (DataRow dr in dt.Rows)
        {
            if (batchRows > 0)
                valueClauses.Append(',');

            valueClauses.Append('(');
            var columnIndex = 0;
            foreach (var kvp in matchedColumns)
            {
                if (columnIndex > 0)
                    valueClauses.Append(',');

                var paramName = $"@p{parameterIndex}";
                valueClauses.Append(paramName);

                var parameter = new SqliteParameter(paramName, ConvertValueForSQLite(dr[kvp.Key.Ordinal]));
                cmd.Parameters.Add(parameter);

                parameterIndex++;
                columnIndex++;
            }
            valueClauses.Append(')');

            batchRows++;

            // Execute batch when we reach batch size or it's the last row
            if (batchRows >= BatchSize || dr == dt.Rows.Cast<DataRow>().Last())
            {
                cmd.CommandText = baseCommand + valueClauses.ToString();
                affected += cmd.ExecuteNonQuery();

                // Reset for next batch
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
    /// Creates a new transaction and does one line at a time bulk insertions to determine which line (and value)
    /// is causing the problem. Transaction is always rolled back.
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

        // Build single-row INSERT statement
        var columnNames = string.Join(",", matchedColumns.Values.Select(c => $"[{c.GetRuntimeName()}]"));
        var paramNames = string.Join(",", matchedColumns.Keys.Select((_, i) => $"@p{i}"));
        cmd.CommandText = $"INSERT INTO {TargetTable.GetFullyQualifiedName()}({columnNames}) VALUES ({paramNames})";

        // Try each row individually
        foreach (DataRow dr in dt.Rows)
        {
            try
            {
                cmd.Parameters.Clear();
                var parameterIndex = 0;

                foreach (var kvp in matchedColumns)
                {
                    var paramName = $"@p{parameterIndex}";
                    var value = ConvertValueForSQLite(dr[kvp.Key.Ordinal]);
                    cmd.Parameters.Add(new SqliteParameter(paramName, value));
                    parameterIndex++;
                }

                cmd.ExecuteNonQuery();
                line++;
            }
            catch (Exception exception)
            {
                // Try to identify which column caused the problem
                var badColumnInfo = IdentifyBadColumn(exception, dr, matchedColumns);

                if (!string.IsNullOrEmpty(badColumnInfo))
                {
                    return new FileLoadException(
                        $"BulkInsert failed on data row {line}. {badColumnInfo}{Environment.NewLine}First Pass: {firstPass}",
                        exception);
                }

                // Fallback: report row number and all values
                var rowValues = string.Join(Environment.NewLine,
                    matchedColumns.Select(kvp =>
                        $"  [{kvp.Key.ColumnName}] = {ValueToString(dr[kvp.Key.Ordinal])}"));

                return new FileLoadException(
                    $"Second Pass Exception: Failed to load data row {line}, the following values were rejected by the database:{Environment.NewLine}{rowValues}{firstPass}",
                    exception);
            }
        }

        // All rows worked individually - unexpected!
        investigationTransaction.Rollback();
        return new Exception(
            $"Second Pass Exception: Bulk insert failed but when we tried to repeat it a line at a time it worked{firstPass}",
            originalException);
    }

    /// <summary>
    /// Attempts to identify which column caused the insert failure by examining the exception message
    /// </summary>
    private string? IdentifyBadColumn(Exception exception, DataRow dr, System.Collections.Generic.Dictionary<DataColumn, DiscoveredColumn> matchedColumns)
    {
        var message = exception.Message;

        // SQLite error messages often include column information
        // Common patterns: "constraint failed: table.column", "datatype mismatch"
        foreach (var kvp in matchedColumns)
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

        // Check for common constraint violations
        if (message.Contains("constraint", StringComparison.OrdinalIgnoreCase))
        {
            // Try to extract which column from constraint name or message
            foreach (var kvp in matchedColumns)
            {
                var destColumn = kvp.Value.GetRuntimeName();
                if (message.Contains(destColumn, StringComparison.OrdinalIgnoreCase))
                {
                    var sourceValue = dr[kvp.Key.Ordinal];
                    return $"Constraint violation on column '{kvp.Key.ColumnName}' " +
                           $"with value '{ValueToString(sourceValue)}'. Error: {message}";
                }
            }
        }

        return null;
    }

    /// <summary>
    /// Converts a value to a readable string for error messages
    /// </summary>
    private static string ValueToString(object? value)
    {
        if (value == null || value == DBNull.Value)
            return "<NULL>";

        if (value is byte[] bytes)
            return $"<byte[{bytes.Length}]>";

        var str = value.ToString() ?? "<NULL>";
        return str.Length > 100 ? str.Substring(0, 100) + "..." : str;
    }

    /// <summary>
    /// Recursively extracts all exception messages including inner exceptions
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

    /// <summary>
    /// Converts empty strings to DBNull to match SQL Server behavior
    /// </summary>
    private static void EmptyStringsToNulls(DataTable dt)
    {
        foreach (var col in dt.Columns.Cast<DataColumn>().Where(static c => c.DataType == typeof(string)))
            foreach (var row in dt.Rows.Cast<DataRow>()
                         .Select(row => new { row, o = row[col] })
                         .Where(static t => t.o != DBNull.Value && t.o != null && string.IsNullOrWhiteSpace(t.o.ToString()))
                         .Select(static t => t.row))
                row[col] = DBNull.Value;
    }

    private static object ConvertValueForSQLite(object value)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // SQLite handles most types natively, but we'll handle some special cases
        return value switch
        {
            DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss.fff"),
            DateOnly dateOnly => dateOnly.ToString("yyyy-MM-dd"),
            TimeOnly timeOnly => timeOnly.ToString("HH:mm:ss.fff"),
            bool b => b ? 1 : 0,  // SQLite stores booleans as integers
            Guid g => g.ToString(),  // Store GUIDs as text
            _ => value
        };
    }

    /// <summary>
    /// Uploads data from a DataTable to the target SQLite table.
    /// </summary>
    /// <param name="dt">The DataTable containing data to upload</param>
    /// <returns>The number of rows inserted</returns>
    /// <remarks>
    /// <para>This method:</para>
    /// <list type="number">
    /// <item><description>Converts empty strings to nulls</description></item>
    /// <item><description>Converts string types to hard types where appropriate</description></item>
    /// <item><description>Batches inserts for performance</description></item>
    /// <item><description>Uses parameterized queries for safety</description></item>
    /// <item><description>Provides detailed error diagnostics on failure</description></item>
    /// </list>
    /// </remarks>
    public override int Upload(DataTable dt)
    {
        if (dt.Rows.Count == 0)
            return 0;

        return UploadImpl(dt);
    }

    // BulkCopy base class handles disposal
}
