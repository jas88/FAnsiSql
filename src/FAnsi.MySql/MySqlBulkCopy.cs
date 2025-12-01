using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using FAnsi.Connections;
using FAnsi.Discovery;
using MySqlConnector;

namespace FAnsi.Implementations.MySql;

/// <summary>
/// High-performance bulk insert implementation for MySQL using MySqlConnector's native MySqlBulkCopy API.
/// This provides performance similar to SQL Server's SqlBulkCopy by leveraging MySQL's optimized bulk loading protocol.
/// Falls back to batched parameterized INSERT statements when LOAD DATA LOCAL INFILE is disabled.
/// </summary>
public sealed class MySqlBulkCopy(DiscoveredTable targetTable, IManagedConnection connection, CultureInfo culture)
    : BulkCopy(targetTable, connection, culture), IDisposable
{
    public static int BulkInsertBatchTimeoutInSeconds { get; set; } = 0;

    /// <summary>
    /// Number of rows to insert per batch when using fallback mode (batched INSERT statements).
    /// Default is 1000 rows per batch, which balances performance with MySQL's max_allowed_packet limit.
    /// </summary>
    public static int FallbackBatchSize { get; set; } = 1000;

    private readonly MySqlConnector.MySqlBulkCopy _bulkCopy = new((MySqlConnection)connection.Connection, (MySqlTransaction?)connection.Transaction)
    {
        DestinationTableName = targetTable.GetFullyQualifiedName()
    };

    private bool _disposed;
    private bool _localInfileDisabled;

    public override int UploadImpl(DataTable dt)
    {
        ThrowIfDisposed();

        if (dt == null)
            throw new ArgumentNullException(nameof(dt));

        if (dt.Rows.Count == 0)
            return 0;

        // Enable strict mode to throw exceptions for data violations (like SQL Server does)
        // This makes MySQL reject invalid data (overflow, truncation, NULL in NOT NULL) instead of silently truncating
        EnsureStrictMode();

        // Pre-process data
        EmptyStringsToNulls(dt);
        ValidateDecimalPrecisionAndScale(dt);

        // If local_infile is known to be disabled, skip directly to fallback
        if (_localInfileDisabled)
            return FallbackBatchedInsert(dt);

        // Set timeout if configured
        _bulkCopy.BulkCopyTimeout = BulkInsertBatchTimeoutInSeconds != 0
            ? BulkInsertBatchTimeoutInSeconds
            : Timeout;

        // Clear and set up column mappings
        _bulkCopy.ColumnMappings.Clear();
        foreach (var (key, value) in GetMapping(dt.Columns.Cast<DataColumn>()))
            _bulkCopy.ColumnMappings.Add(new MySqlConnector.MySqlBulkCopyColumnMapping(
                key.Ordinal,
                value.GetRuntimeName(),
                expression: null));

        return BulkInsertWithBetterErrorMessages(_bulkCopy, dt);
    }

    private bool _strictModeSet;

    /// <summary>
    /// Ensures MySQL strict mode is enabled for the current session.
    /// This makes MySQL throw exceptions for data violations instead of silently truncating.
    /// </summary>
    private void EnsureStrictMode()
    {
        if (_strictModeSet)
            return;

        var conn = (MySqlConnection)Connection.Connection;
        using var cmd = new MySqlCommand("SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'", conn, (MySqlTransaction?)Connection.Transaction);
        cmd.ExecuteNonQuery();
        _strictModeSet = true;
    }

    private int BulkInsertWithBetterErrorMessages(MySqlConnector.MySqlBulkCopy insert, DataTable dt)
    {
        try
        {
            // Use native MySqlBulkCopy for optimal performance
            var result = insert.WriteToServer(dt);
            return result.RowsInserted;
        }
        catch (NotSupportedException ex) when (IsLocalInfileError(ex))
        {
            // Client-side local_infile disabled - fall back to batched inserts
            _localInfileDisabled = true;
            return FallbackBatchedInsert(dt);
        }
        catch (MySqlException ex) when (IsLocalInfileError(ex))
        {
            // Server-side local_infile disabled - fall back to batched inserts
            _localInfileDisabled = true;
            return FallbackBatchedInsert(dt);
        }
        catch (MySqlException ex)
        {
            // Enhance error message with more context
            var enhancedMessage = EnhanceErrorMessage(ex, dt);
            throw new InvalidOperationException(enhancedMessage, ex);
        }
    }

    /// <summary>
    /// Checks if an exception is related to local_infile being disabled (client or server side).
    /// </summary>
    private static bool IsLocalInfileError(Exception ex)
    {
        var message = ex.Message;
        // Client-side: "To use MySqlBulkLoader.Local=true, set AllowLoadLocalInfile=true in the connection string"
        // Server-side: "Loading local data is disabled; this must be enabled on both the client and server sides"
        // MySQL error code 1148: ER_NOT_ALLOWED_COMMAND / ER_CLIENT_LOCAL_FILES_DISABLED
        return message.Contains("AllowLoadLocalInfile", StringComparison.OrdinalIgnoreCase) ||
               message.Contains("Loading local data is disabled", StringComparison.OrdinalIgnoreCase) ||
               message.Contains("local_infile", StringComparison.OrdinalIgnoreCase) ||
               (ex is MySqlException { Number: 1148 or 3948 }); // 1148 = ER_NOT_ALLOWED_COMMAND, 3948 = new error code
    }

    /// <summary>
    /// Fallback implementation using batched parameterized INSERT statements when LOAD DATA LOCAL INFILE is disabled.
    /// </summary>
    private int FallbackBatchedInsert(DataTable dt)
    {
        var matchedColumns = GetMapping(dt.Columns.Cast<DataColumn>());
        var syntax = TargetTable.GetQuerySyntaxHelper();
        var columnNames = string.Join(",", matchedColumns.Values.Select(c => syntax.EnsureWrapped(c.GetRuntimeName())));
        var baseCommand = $"INSERT INTO {TargetTable.GetFullyQualifiedName()}({columnNames}) VALUES ";

        var conn = (MySqlConnection)Connection.Connection;
        var totalRows = dt.Rows.Count;
        var columnCount = matchedColumns.Count;
        var columnEntries = matchedColumns.ToArray();
        var affected = 0;

        // Calculate effective batch size (MySQL has parameter limits)
        var effectiveBatchSize = Math.Max(1, Math.Min(FallbackBatchSize, 65535 / Math.Max(1, columnCount)));

        using var cmd = new MySqlCommand { Connection = conn, Transaction = (MySqlTransaction?)Connection.Transaction };
        if (Timeout > 0)
            cmd.CommandTimeout = Timeout;

        var valueClauses = new StringBuilder();
        var batchRows = 0;
        var parameterIndex = 0;

        for (var rowIndex = 0; rowIndex < totalRows; rowIndex++)
        {
            var dr = dt.Rows[rowIndex];

            if (batchRows > 0)
                valueClauses.Append(',');

            valueClauses.Append('(');

            for (var colIndex = 0; colIndex < columnCount; colIndex++)
            {
                var kvp = columnEntries[colIndex];

                if (colIndex > 0)
                    valueClauses.Append(',');

                var paramName = $"@p{parameterIndex}";
                valueClauses.Append(paramName);
                cmd.Parameters.AddWithValue(paramName, dr[kvp.Key.Ordinal] ?? DBNull.Value);
                parameterIndex++;
            }
            valueClauses.Append(')');
            batchRows++;

            // Execute batch when we reach effective batch size or it's the last row
            if (batchRows >= effectiveBatchSize || rowIndex == totalRows - 1)
            {
                cmd.CommandText = baseCommand + valueClauses;
                affected += cmd.ExecuteNonQuery();

                // Reset for next batch
                cmd.Parameters.Clear();
                valueClauses.Clear();
                batchRows = 0;
                parameterIndex = 0;
            }
        }

        return affected;
    }

    /// <summary>
    /// Enhances MySQL error messages with more specific context about what failed.
    /// </summary>
    private string EnhanceErrorMessage(MySqlException ex, DataTable dt)
    {
        var message = ex.Message;
        var mapping = GetMapping(dt.Columns.Cast<DataColumn>());

        // Try to provide more specific context for common MySQL errors
        if (message.Contains("Data too long for column", StringComparison.OrdinalIgnoreCase))
        {
            // Try to identify which column and row caused the issue
            var rowIndex = 0;
            foreach (DataRow dataRow in dt.Rows)
            {
                foreach (var (dataColumn, discoveredColumn) in mapping)
                {
                    var value = dataRow[dataColumn];
                    if (value is string stringValue && !string.IsNullOrEmpty(stringValue))
                    {
                        var maxLength = discoveredColumn.DataType?.GetLengthIfString();
                        if (maxLength.HasValue && maxLength.Value > 0 && stringValue.Length > maxLength.Value)
                        {
                            return $"Bulk insert failed on data row {rowIndex + 1}: source column '{dataColumn.ColumnName}' has value '{stringValue}' (length: {stringValue.Length}) which exceeds max length of {maxLength.Value} for destination column '{discoveredColumn.GetRuntimeName()}' of type '{discoveredColumn.DataType?.SQLType}'. Original MySQL error: {message}";
                        }
                    }
                }
                rowIndex++;
            }
        }

        if (message.Contains("Out of range value", StringComparison.OrdinalIgnoreCase) ||
            message.Contains("Incorrect", StringComparison.OrdinalIgnoreCase))
        {
            // Try to identify which column has out of range value
            var rowIndex = 0;
            foreach (DataRow dataRow in dt.Rows)
            {
                foreach (var (dataColumn, discoveredColumn) in mapping)
                {
                    var value = dataRow[dataColumn];
                    if (value != null && value != DBNull.Value)
                    {
                        return $"Bulk insert failed on data row {rowIndex + 1}: source column '{dataColumn.ColumnName}' has value '{value}' which may be out of range for destination column '{discoveredColumn.GetRuntimeName()}' of type '{discoveredColumn.DataType?.SQLType}'. Original MySQL error: {message}";
                    }
                }
                rowIndex++;
            }
        }

        return $"Bulk insert failed. Original MySQL error: {message}";
    }

    /// <summary>
    /// Converts empty strings to DBNull for proper NULL handling in the database.
    /// </summary>
    private static void EmptyStringsToNulls(DataTable dt)
    {
        foreach (var col in dt.Columns.Cast<DataColumn>().Where(static c => c.DataType == typeof(string)))
        {
            foreach (DataRow row in dt.Rows)
            {
                var value = row[col];
                if (value != DBNull.Value && value != null && string.IsNullOrWhiteSpace(value.ToString()))
                    row[col] = DBNull.Value;
            }
        }
    }

    /// <summary>
    /// Validates that decimal values in the DataTable fit within the precision and scale constraints
    /// of their target database columns. Throws exception if any value exceeds the allowed precision or scale.
    /// </summary>
    /// <param name="dt">DataTable to validate</param>
    private void ValidateDecimalPrecisionAndScale(DataTable dt)
    {
        var mapping = GetMapping(dt.Columns.Cast<DataColumn>());

        foreach (var (dataColumn, discoveredColumn) in mapping)
        {
            // Only check decimal columns
            if (dataColumn.DataType != typeof(decimal) && dataColumn.DataType != typeof(decimal?))
                continue;

            var decimalSize = discoveredColumn.DataType?.GetDecimalSize();
            if (decimalSize == null)
                continue;

            var precision = decimalSize.Precision;
            var scale = decimalSize.Scale;

            // Calculate max value: for decimal(5,2), max is 999.99
            // Max integer part = 10^(precision - scale) - 1
            // With scale decimal places
            var maxIntegerPart = (int)Math.Pow(10, precision - scale) - 1;
            var maxValue = maxIntegerPart + (decimal)((Math.Pow(10, scale) - 1) / Math.Pow(10, scale));

            for (var rowIndex = 0; rowIndex < dt.Rows.Count; rowIndex++)
            {
                var value = dt.Rows[rowIndex][dataColumn];
                if (value == DBNull.Value || value == null)
                    continue;

                var decimalValue = Math.Abs((decimal)value);

                // Check if value exceeds precision/scale
                if (decimalValue > maxValue)
                {
                    throw new InvalidOperationException(
                        string.Format(CultureInfo.InvariantCulture,
                            "Value {0} in column '{1}' (row {2}) exceeds the maximum allowed for decimal({3},{4}). Maximum value is {5}.",
                            value, dataColumn.ColumnName, rowIndex + 1, precision, scale, maxValue));
                }

                // Check scale (number of decimal places)
                var valueString = decimalValue.ToString(CultureInfo.InvariantCulture);
                if (valueString.Contains('.', StringComparison.Ordinal))
                {
                    var decimalPlaces = valueString.Split('.')[1].TrimEnd('0').Length;
                    if (decimalPlaces > scale)
                    {
                        throw new InvalidOperationException(
                            string.Format(CultureInfo.InvariantCulture,
                                "Value {0} in column '{1}' (row {2}) has {3} decimal places, but column is defined as decimal({4},{5}) which allows only {5} decimal places.",
                                value, dataColumn.ColumnName, rowIndex + 1, decimalPlaces, precision, scale));
                    }
                }
            }
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(MySqlBulkCopy));
    }

    /// <summary>
    /// Releases all resources used by the MySqlBulkCopy.
    /// </summary>
    public new void Dispose()
    {
        if (!_disposed)
        {
            base.Dispose();
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }
}
