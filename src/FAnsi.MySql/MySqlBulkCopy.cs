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

        // Get mapping once and pre-process/validate all data in a single pass
        var mapping = GetMapping(dt.Columns.Cast<DataColumn>());
        PreProcessAndValidate(dt, mapping);

        // If local_infile is known to be disabled, skip directly to fallback
        if (_localInfileDisabled)
            return FallbackBatchedInsert(dt);

        // Set timeout if configured
        _bulkCopy.BulkCopyTimeout = BulkInsertBatchTimeoutInSeconds != 0
            ? BulkInsertBatchTimeoutInSeconds
            : Timeout;

        // Clear and set up column mappings
        _bulkCopy.ColumnMappings.Clear();
        foreach (var (key, value) in mapping)
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
    /// Pre-processes and validates all data in a single pass through the DataTable.
    /// Performs: empty string to NULL conversion, string length validation, decimal validation,
    /// integer range validation, and NOT NULL constraint validation.
    /// </summary>
    private static void PreProcessAndValidate(DataTable dt, Dictionary<DataColumn, DiscoveredColumn> mapping)
    {
        // Pre-compute validation rules for each column (once per table, not per row)
        var rules = new ColumnValidationRule[mapping.Count];
        var ruleIndex = 0;

        foreach (var (dataColumn, discoveredColumn) in mapping)
        {
            var isString = dataColumn.DataType == typeof(string);
            var isDecimal = dataColumn.DataType == typeof(decimal) || dataColumn.DataType == typeof(decimal?);
            var isInteger = IsIntegerType(dataColumn.DataType);

            int maxStringLength = 0;
            int decimalPrecision = 0, decimalScale = 0;
            decimal maxDecimalValue = 0;
            long intMin = long.MinValue, intMax = long.MaxValue;
            var hasIntRange = false;

            if (isString)
            {
                var len = discoveredColumn.DataType?.GetLengthIfString();
                if (len.HasValue && len.Value > 0)
                    maxStringLength = len.Value;
            }

            if (isDecimal)
            {
                var sz = discoveredColumn.DataType?.GetDecimalSize();
                if (sz != null)
                {
                    decimalPrecision = sz.Precision;
                    decimalScale = sz.Scale;
                    var maxInt = (int)Math.Pow(10, sz.Precision - sz.Scale) - 1;
                    maxDecimalValue = maxInt + (decimal)((Math.Pow(10, sz.Scale) - 1) / Math.Pow(10, sz.Scale));
                }
            }

            if (isInteger)
            {
                var sqlType = discoveredColumn.DataType?.SQLType?.ToUpperInvariant();
                if (!string.IsNullOrEmpty(sqlType))
                {
                    (intMin, intMax) = GetIntegerRange(sqlType);
                    hasIntRange = intMin != long.MinValue || intMax != long.MaxValue;
                }
            }

            rules[ruleIndex++] = new ColumnValidationRule(
                dataColumn, discoveredColumn, dataColumn.Ordinal,
                isString, maxStringLength,
                isDecimal, decimalPrecision, decimalScale, maxDecimalValue,
                hasIntRange, intMin, intMax,
                !discoveredColumn.AllowNulls);
        }

        // Single pass through all rows using pre-computed rules
        var rowCount = dt.Rows.Count;
        for (var rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            var row = dt.Rows[rowIndex];

            for (var i = 0; i < rules.Length; i++)
            {
                var rule = rules[i];
                var value = row[rule.Ordinal];
                var isNull = value == DBNull.Value || value == null;

                // String: convert empty to NULL, validate length
                if (rule.IsString && value is string s)
                {
                    if (string.IsNullOrWhiteSpace(s))
                    {
                        row[rule.Ordinal] = DBNull.Value;
                        isNull = true;
                    }
                    else if (rule.MaxStringLength > 0 && s.Length > rule.MaxStringLength)
                    {
                        throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture,
                            "Bulk insert failed on data row {0}: source column <<{1}>> has value <<{2}>> (length {3}) which exceeds maximum length {4} for destination column <<{5}>>.",
                            rowIndex + 1, rule.SourceName, s, s.Length, rule.MaxStringLength, rule.DestName));
                    }
                }

                // Decimal validation
                if (rule.IsDecimal && !isNull && rule.MaxDecimalValue > 0)
                {
                    var d = Math.Abs(Convert.ToDecimal(value, CultureInfo.InvariantCulture));
                    if (d > rule.MaxDecimalValue)
                        throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture,
                            "Value {0} in column '{1}' (row {2}) exceeds the maximum allowed for decimal({3},{4}). Maximum value is {5}.",
                            value, rule.SourceName, rowIndex + 1, rule.DecimalPrecision, rule.DecimalScale, rule.MaxDecimalValue));

                    var vs = d.ToString(CultureInfo.InvariantCulture);
                    if (vs.Contains('.', StringComparison.Ordinal))
                    {
                        var places = vs.Split('.')[1].TrimEnd('0').Length;
                        if (places > rule.DecimalScale)
                            throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture,
                                "Value {0} in column '{1}' (row {2}) has {3} decimal places, but column is defined as decimal({4},{5}) which allows only {5} decimal places.",
                                value, rule.SourceName, rowIndex + 1, places, rule.DecimalPrecision, rule.DecimalScale));
                    }
                }

                // Integer range validation
                if (rule.HasIntegerRange && !isNull)
                {
                    var v = Convert.ToInt64(value, CultureInfo.InvariantCulture);
                    if (v < rule.IntegerMin || v > rule.IntegerMax)
                        throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture,
                            "Value {0} in column '{1}' (row {2}) is out of range for column '{3}' of type '{4}'.",
                            value, rule.SourceName, rowIndex + 1, rule.DestName, rule.SqlType));
                }

                // NOT NULL validation (after empty string conversion)
                if (rule.RequiresNotNull && isNull)
                    throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture,
                        "NULL value in column '{0}' (row {1}) violates NOT NULL constraint on column '{2}'.",
                        rule.SourceName, rowIndex + 1, rule.DestName));
            }
        }
    }

    /// <summary>
    /// Pre-computed validation rules for a column. Avoids repeated lookups during row iteration.
    /// </summary>
    private readonly struct ColumnValidationRule(
        DataColumn dataColumn, DiscoveredColumn discoveredColumn, int ordinal,
        bool isString, int maxStringLength,
        bool isDecimal, int decimalPrecision, int decimalScale, decimal maxDecimalValue,
        bool hasIntegerRange, long integerMin, long integerMax,
        bool requiresNotNull)
    {
        public readonly int Ordinal = ordinal;
        public readonly string SourceName = dataColumn.ColumnName;
        public readonly string DestName = discoveredColumn.GetRuntimeName();
        public readonly string? SqlType = discoveredColumn.DataType?.SQLType;
        public readonly bool IsString = isString;
        public readonly int MaxStringLength = maxStringLength;
        public readonly bool IsDecimal = isDecimal;
        public readonly int DecimalPrecision = decimalPrecision;
        public readonly int DecimalScale = decimalScale;
        public readonly decimal MaxDecimalValue = maxDecimalValue;
        public readonly bool HasIntegerRange = hasIntegerRange;
        public readonly long IntegerMin = integerMin;
        public readonly long IntegerMax = integerMax;
        public readonly bool RequiresNotNull = requiresNotNull;
    }

    private static bool IsIntegerType(Type type) =>
        type == typeof(byte) || type == typeof(sbyte) ||
        type == typeof(short) || type == typeof(ushort) ||
        type == typeof(int) || type == typeof(uint) ||
        type == typeof(long) || type == typeof(ulong) ||
        (Nullable.GetUnderlyingType(type) is { } underlying && IsIntegerType(underlying));

    private static (long min, long max) GetIntegerRange(string sqlType) =>
        sqlType switch
        {
            "TINYINT" => (-128, 127),
            "TINYINT UNSIGNED" => (0, 255),
            "SMALLINT" => (short.MinValue, short.MaxValue),
            "SMALLINT UNSIGNED" => (0, ushort.MaxValue),
            "MEDIUMINT" => (-8388608, 8388607),
            "MEDIUMINT UNSIGNED" => (0, 16777215),
            "INT" or "INTEGER" => (int.MinValue, int.MaxValue),
            "INT UNSIGNED" or "INTEGER UNSIGNED" => (0, uint.MaxValue),
            "BIGINT" => (long.MinValue, long.MaxValue),
            _ => (long.MinValue, long.MaxValue)
        };

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
