using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using FAnsi.Connections;
using FAnsi.Discovery;
using Npgsql;

namespace FAnsi.Implementations.PostgreSql;

public sealed class PostgreSqlBulkCopy(DiscoveredTable discoveredTable, IManagedConnection connection, CultureInfo culture) : BulkCopy(discoveredTable, connection, culture)
{
    private bool _disposed;

    public override int UploadImpl(DataTable dt)
    {
        ThrowIfDisposed();
        ValidateDecimalPrecisionAndScale(dt);

        var con = (NpgsqlConnection)Connection.Connection;

        var matchedColumns = GetMapping(dt.Columns.Cast<DataColumn>());

        //see https://www.npgsql.org/doc/copy.html
        var sb = new StringBuilder();

        sb.Append("COPY ");
        sb.Append(TargetTable.GetFullyQualifiedName());
        sb.Append(" (");
        sb.AppendJoin(",", matchedColumns.Values.Select(static v => v.GetWrappedName()));
        sb.Append(')');
        sb.Append(" FROM STDIN (FORMAT BINARY)");

        var tt = PostgreSqlTypeTranslater.Instance;

        var dataColumns = matchedColumns.Keys.ToArray();
        var types = matchedColumns.Keys.Select(v => tt.GetNpgsqlDbTypeForCSharpType(v.DataType)).ToArray();

        using (var import = con.BeginBinaryImport(sb.ToString()))
        {
            foreach (DataRow r in dt.Rows)
            {
                import.StartRow();

                for (var index = 0; index < dataColumns.Length; index++)
                {
                    var dc = dataColumns[index];
                    if (r[dc] == DBNull.Value)
                        import.WriteNull();
                    else
                        import.Write(r[dc], types[index]);
                }
            }

            import.Complete();
        }

        return dt.Rows.Count;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(PostgreSqlBulkCopy));
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
    /// Releases all resources used by the PostgreSqlBulkCopy.
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
