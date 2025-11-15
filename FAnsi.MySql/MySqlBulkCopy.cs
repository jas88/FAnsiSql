using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using FAnsi.Connections;
using FAnsi.Discovery;
using MySqlConnector;

namespace FAnsi.Implementations.MySql;

/// <summary>
/// High-performance bulk insert implementation for MySQL using parameterized batch INSERT statements.
/// Replaces the previous string concatenation approach with proper parameterized queries for
/// better security, performance, and memory efficiency.
/// </summary>
public sealed partial class MySqlBulkCopy(DiscoveredTable targetTable, IManagedConnection connection, CultureInfo culture)
    : BulkCopy(targetTable, connection, culture), IDisposable
{
    public static int BulkInsertBatchTimeoutInSeconds { get; set; } = 0;

    /// <summary>
    /// Default batch size for parameterized inserts. This balances performance with memory usage.
    /// </summary>
    private const int DefaultBatchSize = 1000;

    /// <summary>
    /// Maximum number of parameters allowed by MySQL. We use a conservative limit to avoid issues.
    /// </summary>
    private const int MaxParameters = 30000;

    private bool _disposed;

    public override int UploadImpl(DataTable dt)
    {
        ThrowIfDisposed();

        if (dt == null)
            throw new ArgumentNullException(nameof(dt));

        if (dt.Rows.Count == 0)
            return 0;

        using var ourTrans = Connection.Transaction == null ? Connection.Connection.BeginTransaction(IsolationLevel.ReadUncommitted) : null;
        var matchedColumns = GetMapping(dt.Columns.Cast<DataColumn>());
        var affected = 0;

        try
        {
            using var cmd = new MySqlCommand((MySqlConnection)Connection.Connection, (MySqlTransaction?)(Connection.Transaction ?? ourTrans));
            if (BulkInsertBatchTimeoutInSeconds != 0)
                cmd.CommandTimeout = BulkInsertBatchTimeoutInSeconds;

            // Calculate optimal batch size based on column count and row count
            var columnCount = matchedColumns.Count;
            var optimalBatchSize = Math.Min(DefaultBatchSize, MaxParameters / Math.Max(1, columnCount));
            optimalBatchSize = Math.Max(1, optimalBatchSize); // Ensure at least 1

            var columnNames = matchedColumns.Values.Select(c => $"`{c.GetRuntimeName()}`").ToArray();
            var columnList = string.Join(", ", columnNames);

            // Process data in batches
            for (int batchStart = 0; batchStart < dt.Rows.Count; batchStart += optimalBatchSize)
            {
                var batchEnd = Math.Min(batchStart + optimalBatchSize, dt.Rows.Count);
                var batchCount = batchEnd - batchStart;

                affected += ExecuteBatchInsert(cmd, dt, matchedColumns, columnList, batchStart, batchCount, columnCount);
            }

            ourTrans?.Commit();
            return affected;
        }
        catch
        {
            ourTrans?.Rollback();
            throw;
        }
    }

    /// <summary>
    /// Executes a batch insert using parameterized queries for optimal performance and security.
    /// </summary>
    private int ExecuteBatchInsert(MySqlCommand cmd, DataTable dt, Dictionary<DataColumn, DiscoveredColumn> matchedColumns,
        string columnList, int batchStart, int batchSize, int columnCount)
    {
        try
        {
            // Build parameterized INSERT statement
            var valuePlaceholders = Enumerable.Range(0, batchSize)
                .Select(row => $"({string.Join(",", Enumerable.Range(0, columnCount).Select(col => $"@p{row}_{col}"))})")
                .ToArray();

            cmd.CommandText = $"INSERT INTO {TargetTable.GetFullyQualifiedName()} ({columnList}) VALUES {string.Join(",", valuePlaceholders)}";

            // Clear previous parameters
            cmd.Parameters.Clear();

            // Add parameters for each row and column
            var columnArray = matchedColumns.Keys.ToArray();
            for (int rowIndex = 0; rowIndex < batchSize; rowIndex++)
            {
                var dataRow = dt.Rows[batchStart + rowIndex];
                for (int colIndex = 0; colIndex < columnCount; colIndex++)
                {
                    var dataColumn = columnArray[colIndex];
                    var discoveredColumn = matchedColumns[dataColumn];
                    var parameterName = $"@p{rowIndex}_{colIndex}";

                    var parameter = cmd.Parameters.Add(parameterName, GetMySqlDbType(discoveredColumn.DataType?.SQLType));
                    parameter.Value = ConvertValueForParameter(dataRow[dataColumn], discoveredColumn.DataType?.SQLType);
                }
            }

            return cmd.ExecuteNonQuery();
        }
        catch (MySqlException ex)
        {
            // Enhance error messages with more context about what failed
            var enhancedMessage = EnhanceErrorMessage(ex, dt, matchedColumns, batchStart, batchSize);
            throw new InvalidOperationException(enhancedMessage, ex);
        }
    }

    /// <summary>
    /// Enhances MySQL error messages with more specific context about what failed.
    /// </summary>
    private static string EnhanceErrorMessage(MySqlException ex, DataTable dt, Dictionary<DataColumn, DiscoveredColumn> matchedColumns, int batchStart, int batchSize)
    {
        var message = ex.Message;

        // Try to provide more specific context for common MySQL errors
        if (message.Contains("Data too long for column"))
        {
            // Try to identify which column and row caused the issue
            for (int rowIndex = 0; rowIndex < batchSize; rowIndex++)
            {
                var actualRow = batchStart + rowIndex;
                if (actualRow >= dt.Rows.Count) break;

                var dataRow = dt.Rows[actualRow];
                foreach (var kvp in matchedColumns)
                {
                    var dataColumn = kvp.Key;
                    var discoveredColumn = kvp.Value;
                    var value = dataRow[dataColumn];

                    if (value is string stringValue && !string.IsNullOrEmpty(stringValue))
                    {
                        var maxLength = discoveredColumn.DataType?.GetLengthIfString();
                        if (maxLength.HasValue && maxLength.Value > 0 && stringValue.Length > maxLength.Value)
                        {
                            return $"Bulk insert failed on data row {actualRow + 1} the complaint was about source column <<{dataColumn.ColumnName}>> which had value <<{stringValue}>> destination data type was <<{discoveredColumn.DataType?.SQLType}>>. Original MySQL error: {message}";
                        }
                    }
                }
            }
        }

        if (message.Contains("Out of range value for column"))
        {
            // Try to identify numeric overflow issues
            for (int rowIndex = 0; rowIndex < batchSize; rowIndex++)
            {
                var actualRow = batchStart + rowIndex;
                if (actualRow >= dt.Rows.Count) break;

                var dataRow = dt.Rows[actualRow];
                foreach (var kvp in matchedColumns)
                {
                    var dataColumn = kvp.Key;
                    var discoveredColumn = kvp.Value;
                    var value = dataRow[dataColumn];

                    if (value != null && value != DBNull.Value)
                    {
                        return $"Bulk insert failed on data row {actualRow + 1} the complaint was about source column <<{dataColumn.ColumnName}>> which had value <<{value}>> destination data type was <<{discoveredColumn.DataType?.SQLType}>>. Original MySQL error: {message}";
                    }
                }
            }
        }

        return $"Bulk insert failed on batch starting at row {batchStart + 1} (batch size: {batchSize}). Original MySQL error: {message}";
    }

    /// <summary>
    /// Converts a value for use with MySqlConnector parameters, handling null values and type conversions.
    /// </summary>
    private static object ConvertValueForParameter(object value, string? sqlType)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // MySqlConnector handles DateTime objects directly
        if (value is DateTime)
            return value;

        // Handle string values - MySqlConnector will properly escape them when using parameters
        if (value is string stringValue)
        {
            if (string.IsNullOrWhiteSpace(stringValue))
                return DBNull.Value;

            return stringValue;
        }

        // Handle other numeric and basic types
        return value;
    }

    /// <summary>
    /// Maps SQL type strings to MySqlConnector parameter types.
    /// </summary>
    private static MySqlDbType GetMySqlDbType(string? sqlType)
    {
        if (string.IsNullOrEmpty(sqlType))
            return MySqlDbType.VarChar;

        var normalizedType = sqlType.ToUpperInvariant().Split('(')[0].Trim();

        return normalizedType switch
        {
            // Integer types
            "TINYINT" => MySqlDbType.Byte,
            "SMALLINT" => MySqlDbType.Int16,
            "INT" or "INTEGER" => MySqlDbType.Int32,
            "MEDIUMINT" => MySqlDbType.Int24,
            "BIGINT" => MySqlDbType.Int64,

            // Floating point types
            "FLOAT" => MySqlDbType.Float,
            "DOUBLE" => MySqlDbType.Double,
            "DECIMAL" or "NUMERIC" => MySqlDbType.Decimal,

            // String types
            "CHAR" => MySqlDbType.String,
            "VARCHAR" => MySqlDbType.VarChar,
            "TEXT" => MySqlDbType.Text,
            "TINYTEXT" => MySqlDbType.TinyText,
            "MEDIUMTEXT" => MySqlDbType.MediumText,
            "LONGTEXT" => MySqlDbType.LongText,

            // Binary types
            "BINARY" => MySqlDbType.Binary,
            "VARBINARY" => MySqlDbType.VarBinary,
            "BLOB" => MySqlDbType.Blob,
            "TINYBLOB" => MySqlDbType.TinyBlob,
            "MEDIUMBLOB" => MySqlDbType.MediumBlob,
            "LONGBLOB" => MySqlDbType.LongBlob,

            // Date/Time types
            "DATE" => MySqlDbType.Date,
            "DATETIME" => MySqlDbType.DateTime,
            "TIMESTAMP" => MySqlDbType.Timestamp,
            "TIME" => MySqlDbType.Time,
            "YEAR" => MySqlDbType.Year,

            // Boolean/Bit
            "BIT" or "BOOLEAN" => MySqlDbType.Bit,

            // Special types
            "ENUM" or "SET" => MySqlDbType.VarChar,

            // Default fallback
            _ => MySqlDbType.VarChar
        };
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
    }

    [GeneratedRegex("\\(.*\\)")]
    private static partial Regex BracketsRe();
}
