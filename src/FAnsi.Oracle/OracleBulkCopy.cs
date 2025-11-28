using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery;
using Oracle.ManagedDataAccess.Client;

namespace FAnsi.Implementations.Oracle;

internal sealed class OracleBulkCopy(DiscoveredTable targetTable, IManagedConnection connection, CultureInfo culture) : BulkCopy(targetTable, connection, culture)
{
    private readonly DiscoveredServer _server = targetTable.Database.Server;
    private bool _disposed;

    public override int UploadImpl(DataTable dt)
    {
        ThrowIfDisposed();

        //don't run an insert if there are 0 rows
        if (dt.Rows.Count == 0)
            return 0;

        ValidateDecimalPrecisionAndScale(dt);

        var syntaxHelper = _server.GetQuerySyntaxHelper();
        var tt = syntaxHelper.TypeTranslater;

        //if the column name is a reserved keyword e.g. "Comment" we need to give it a new name
        var parameterNames = syntaxHelper.GetParameterNamesFor(dt.Columns.Cast<DataColumn>().ToArray(), static c => c.ColumnName);

        var affectedRows = 0;

        var mapping = GetMapping(dt.Columns.Cast<DataColumn>());

        var dateColumns = new HashSet<DataColumn>();

        var sql = string.Format(CultureInfo.InvariantCulture, "INSERT INTO " + TargetTable.GetFullyQualifiedName() + "({0}) VALUES ({1})",
            string.Join(",", mapping.Values.Select(static c => c.GetWrappedName())),
            string.Join(",", mapping.Keys.Select(c => parameterNames[c]))
        );


        using var cmd = (OracleCommand)_server.GetCommand(sql, Connection);
        //send all the data at once
        cmd.ArrayBindCount = dt.Rows.Count;

        foreach (var (dataColumn, discoveredColumn) in mapping)
        {
            var p = _server.AddParameterWithValueToCommand(parameterNames[dataColumn]!, cmd, DBNull.Value);
            p.DbType = tt.GetDbTypeForSQLDBType(discoveredColumn.DataType!.SQLType);

            switch (p.DbType)
            {
                case DbType.DateTime:
                    dateColumns.Add(dataColumn);
                    break;
                case DbType.Boolean:
                    p.DbType = DbType.Int32; // JS 2023-05-11 special case since we don't have a true boolean type in Oracle, but use 0/1 instead
                    break;
                case DbType.Object:
                    // DbType.Object is used for byte[] - Oracle needs OracleDbType.Blob for array binding
                    if (dataColumn.DataType == typeof(byte[]))
                    {
                        ((OracleParameter)p).OracleDbType = OracleDbType.Blob;
                    }
                    break;
            }
        }

        var values = mapping.Keys.ToDictionary(static c => c, static _ => new List<object?>());

        foreach (DataRow dataRow in dt.Rows)
            //populate parameters for current row
            foreach (var col in mapping.Keys)
            {
                var val = dataRow[col];

                if (val is string stringVal && string.IsNullOrWhiteSpace(stringVal))
                    val = null;
                else if (val == DBNull.Value)
                    val = null;
                else if (dateColumns.Contains(col))
                    val = val is string s ? (DateTime?)DateTimeDecider.Parse(s) : Convert.ToDateTime(dataRow[col], CultureInfo.InvariantCulture);

                if (col.DataType == typeof(bool) && val is bool b)
                    values[col].Add(b ? 1 : 0);
                else
                    values[col].Add(val);
            }

        foreach (var col in mapping.Keys)
        {
            var param = cmd.Parameters[parameterNames[col]!];
            param.Value = values[col].ToArray();
        }

        //send query
        affectedRows += cmd.ExecuteNonQuery();
        return affectedRows;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(OracleBulkCopy));
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

    /// <summary>
    /// Releases all resources used by the OracleBulkCopy.
    /// </summary>
    public new void Dispose()
    {
        if (!_disposed)
        {
            base.Dispose();
            _disposed = true;
        }
    }
}
