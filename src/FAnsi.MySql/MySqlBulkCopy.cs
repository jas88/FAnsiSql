using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery;
using MySqlConnector;

namespace FAnsi.Implementations.MySql;

/// <summary>
/// High-performance bulk insert implementation for MySQL using MySqlConnector's native MySqlBulkCopy API.
/// This provides performance similar to SQL Server's SqlBulkCopy by leveraging MySQL's optimized bulk loading protocol.
/// </summary>
public sealed class MySqlBulkCopy(DiscoveredTable targetTable, IManagedConnection connection, CultureInfo culture)
    : BulkCopy(targetTable, connection, culture), IDisposable
{
    public static int BulkInsertBatchTimeoutInSeconds { get; set; } = 0;

    private readonly MySqlConnector.MySqlBulkCopy _bulkCopy = new((MySqlConnection)connection.Connection, (MySqlTransaction?)connection.Transaction)
    {
        DestinationTableName = targetTable.GetFullyQualifiedName()
    };

    private bool _disposed;

    public override int UploadImpl(DataTable dt)
    {
        ThrowIfDisposed();

        if (dt == null)
            throw new ArgumentNullException(nameof(dt));

        if (dt.Rows.Count == 0)
            return 0;

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

    private int BulkInsertWithBetterErrorMessages(MySqlConnector.MySqlBulkCopy insert, DataTable dt)
    {
        EmptyStringsToNulls(dt);
        ConvertStringTypesToHardTypes(dt);
        ValidateDecimalPrecisionAndScale(dt);

        try
        {
            // Use native MySqlBulkCopy for optimal performance
            var result = insert.WriteToServer(dt);
            return result.RowsInserted;
        }
        catch (MySqlException ex)
        {
            // Enhance error message with more context
            var enhancedMessage = EnhanceErrorMessage(ex, dt);
            throw new InvalidOperationException(enhancedMessage, ex);
        }
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
