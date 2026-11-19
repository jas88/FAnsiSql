using System.Data;
using System.Globalization;
using System.Text;
using FAnsi.Connections;
using FAnsi.Discovery;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace FAnsi.Implementations.Oracle;

internal sealed class OracleBulkCopy(DiscoveredTable targetTable, IManagedConnection connection, CultureInfo culture)
    : BulkCopy(targetTable, connection, culture)
{
    private static readonly CompositeFormat
        FormatInsertSql = CompositeFormat.Parse("INSERT INTO {0}({1}) VALUES ({2})");

    private readonly DiscoveredServer _server = targetTable.Database.Server;
    private bool _disposed;

    public override int UploadImpl(DataTable dt)
    {
        ThrowIfDisposed();

        //don't run an insert if there are 0 rows
        if (dt.Rows.Count == 0)
            return 0;

        var syntaxHelper = _server.GetQuerySyntaxHelper();
        var tt = syntaxHelper.TypeTranslater;

        //if the column name is a reserved keyword e.g. "Comment" we need to give it a new name
        var parameterNames =
            syntaxHelper.GetParameterNamesFor(dt.Columns.Cast<DataColumn>().ToArray(), static c => c.ColumnName);

        var affectedRows = 0;

        var mapping = GetMapping(dt.Columns.Cast<DataColumn>());

        // Single-pass validation: empty string to NULL, string length, decimal precision/scale
        PreProcessAndValidate(dt, mapping);

        var dateColumns = new HashSet<DataColumn>();
        var numberColumns = new HashSet<DataColumn>();

        var sql = string.Format(CultureInfo.InvariantCulture, FormatInsertSql,
            TargetTable.GetFullyQualifiedName(),
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
            var isFloatingPoint = dataColumn.DataType == typeof(double) || dataColumn.DataType == typeof(float);

            switch (p.DbType)
            {
                case DbType.DateTime:
                    dateColumns.Add(dataColumn);
                    break;
                case DbType.Boolean:
                    p.DbType = DbType
                        .Int32; // JS 2023-05-11 special case since we don't have a true boolean type in Oracle, but use 0/1 instead
                    break;
                case DbType.Object:
                    // DbType.Object is used for byte[] - Oracle needs OracleDbType.Blob for array binding
                    if (dataColumn.DataType == typeof(byte[])) ((OracleParameter)p).OracleDbType = OracleDbType.Blob;
                    break;
                // A float or double going into a NUMBER(p,s) column, see ToNumber
                case DbType.Decimal when isFloatingPoint && discoveredColumn.DataType.GetDecimalSize() != null:
                    numberColumns.Add(dataColumn);
                    break;
            }
        }

        var values = mapping.Keys.ToDictionary(static c => c, static _ => new List<object?>());

        foreach (DataRow dataRow in dt.Rows)
            //populate parameters for current row
            foreach (var col in mapping.Keys)
            {
                var val = dataRow[col];

                // PreProcessAndValidate already converted empty strings to DBNull.Value
                if (val == DBNull.Value)
                    val = null;
                else if (dateColumns.Contains(col))
                    val = val is string s
                        ? (DateTime?)DateTimeDecider.Parse(s)
                        : Convert.ToDateTime(dataRow[col], CultureInfo.InvariantCulture);
                else if (numberColumns.Contains(col))
                    val = ToNumber(val);

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

    /// <summary>
    ///     Returns a float or double for a NUMBER(p,s) column as the NUMBER of the shortest text that reads back as it,
    ///     which is what bulk insert checks the value as and what MySQL is sent. ODP.NET would convert it to a NUMBER
    ///     of 15 significant digits, which rounds the double just below 10^29, 9.999999999999997E+28, up to 10^29, too
    ///     large for NUMBER(29,0), and 0.12499999999999999 up to 0.125.
    /// </summary>
    /// <remarks>
    ///     NaN, infinity and numbers outside the range of NUMBER are left for ODP.NET to convert as before. ODP.NET
    ///     cannot parse a number with an exponent, so the NUMBER is made from the digits of the text and the place of
    ///     its decimal point.
    /// </remarks>
    internal static object ToNumber(object value)
    {
        var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
        var magnitude = Math.Abs(number);
        if (!(magnitude >= 1e-129 && magnitude < 1e125))
            return value;

        var text = value is float single
            ? single.ToString("R", CultureInfo.InvariantCulture)
            : number.ToString("R", CultureInfo.InvariantCulture);

        var e = text.IndexOf('E', StringComparison.Ordinal);
        var exponent = e < 0
            ? 0
            : int.Parse(text.AsSpan(e + 1), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
        var digits = e < 0 ? text : text[..e];

        var point = digits.IndexOf('.', StringComparison.Ordinal);
        if (point >= 0)
        {
            exponent -= digits.Length - point - 1;
            digits = digits.Remove(point, 1);
        }

        // At most 17 significant digits, which a long holds
        var mantissa = long.Parse(digits, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
        return OracleDecimal.Shift(new OracleDecimal(mantissa), exponent);
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    /// <summary>
    ///     Releases all resources used by the OracleBulkCopy.
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
