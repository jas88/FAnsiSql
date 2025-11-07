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

        var ourTrans = Connection.Transaction == null ? Connection.Connection.BeginTransaction(IsolationLevel.ReadUncommitted) : null;
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

            var tableName = TargetTable.GetFullyQualifiedName();
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
        finally
        {
            if (ourTrans != null)
            {
                ourTrans.Dispose();
            }
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
            // For MySQL schema errors, preserve original exception type and message
            // Tests expect exact MySQL error messages like "Data too long for column 'Name' at row 4"
            if (IsMySqlSchemaError(ex.Message))
            {
                throw; // Re-throw original MySqlException unchanged
            }

            // For other errors, enhance with context about batch position
            var enhancedMessage = EnhanceErrorMessage(ex, dt, matchedColumns, batchStart, batchSize);
            throw new Exception(enhancedMessage, ex);
        }
    }

    /// <summary>
    /// Determines if a MySQL error message is a schema validation error that should be preserved unchanged.
    /// These errors have exact message expectations in tests.
    /// </summary>
    private static bool IsMySqlSchemaError(string message)
    {
        return message.Contains("Data too long for column") ||
               message.Contains("Out of range value for column");
    }

    /// <summary>
    /// Enhances MySQL error messages with more specific context about what failed.
    /// For non-schema errors, adds context about batch position.
    /// </summary>
    private static string EnhanceErrorMessage(MySqlException ex, DataTable dt, Dictionary<DataColumn, DiscoveredColumn> matchedColumns, int batchStart, int batchSize)
    {
        var message = ex.Message;

        // For other errors, enhance with context about batch position
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

        switch (normalizedType)
        {
            // Integer types
            case "TINYINT":
                return MySqlDbType.Byte;
            case "SMALLINT":
                return MySqlDbType.Int16;
            case "INT":
            case "INTEGER":
                return MySqlDbType.Int32;
            case "MEDIUMINT":
                return MySqlDbType.Int24;
            case "BIGINT":
                return MySqlDbType.Int64;

            // Floating point types
            case "FLOAT":
                return MySqlDbType.Float;
            case "DOUBLE":
                return MySqlDbType.Double;
            case "DECIMAL":
            case "NUMERIC":
                return MySqlDbType.Decimal;

            // String types
            case "CHAR":
                return MySqlDbType.String;
            case "VARCHAR":
                return MySqlDbType.VarChar;
            case "TEXT":
                return MySqlDbType.Text;
            case "TINYTEXT":
                return MySqlDbType.TinyText;
            case "MEDIUMTEXT":
                return MySqlDbType.MediumText;
            case "LONGTEXT":
                return MySqlDbType.LongText;

            // Binary types
            case "BINARY":
                return MySqlDbType.Binary;
            case "VARBINARY":
                return MySqlDbType.VarBinary;
            case "BLOB":
                return MySqlDbType.Blob;
            case "TINYBLOB":
                return MySqlDbType.TinyBlob;
            case "MEDIUMBLOB":
                return MySqlDbType.MediumBlob;
            case "LONGBLOB":
                return MySqlDbType.LongBlob;

            // Date/Time types
            case "DATE":
                return MySqlDbType.Date;
            case "DATETIME":
                return MySqlDbType.DateTime;
            case "TIMESTAMP":
                return MySqlDbType.Timestamp;
            case "TIME":
                return MySqlDbType.Time;
            case "YEAR":
                return MySqlDbType.Year;

            // Boolean/Bit
            case "BIT":
            case "BOOLEAN":
                return MySqlDbType.Bit;

            // Special types
            case "ENUM":
            case "SET":
                return MySqlDbType.VarChar;

            // Default fallback
            default:
                return MySqlDbType.VarChar;
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
    }

    [GeneratedRegex("\\(.*\\)")]
    private static partial Regex BracketsRe();
}
