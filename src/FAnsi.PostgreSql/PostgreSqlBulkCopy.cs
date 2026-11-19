using System.Data;
using System.Globalization;
using System.Numerics;
using System.Text;
using FAnsi.Connections;
using FAnsi.Discovery;
using Npgsql;
using NpgsqlTypes;

namespace FAnsi.Implementations.PostgreSql;

public sealed class PostgreSqlBulkCopy(
    DiscoveredTable discoveredTable,
    IManagedConnection connection,
    CultureInfo culture) : BulkCopy(discoveredTable, connection, culture)
{
    /// <summary>
    ///     2^96, the smallest double that a decimal cannot hold
    /// </summary>
    private const double DecimalOverflow = 79228162514264337593543950336d;

    private bool _disposed;

    public override int UploadImpl(DataTable dt)
    {
        ThrowIfDisposed();

        var con = (NpgsqlConnection)Connection.Connection;

        var matchedColumns = GetMapping(dt.Columns.Cast<DataColumn>());

        // Single-pass validation: empty string to NULL, string length, decimal precision/scale
        PreProcessAndValidate(dt, matchedColumns);

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

        // The server does not convert values sent in the binary format, each one has to be written as the type of the
        // column it is going into whatever the type of the DataColumn it came from.  The type of the DataColumn is
        // only used for the column types that take values of just the one C# type.
        var types = matchedColumns.Select(kvp =>
            (kvp.Value.DataType == null ? null : tt.GetNpgsqlDbTypeForSqlType(kvp.Value.DataType.SQLType)) ??
            tt.GetNpgsqlDbTypeForCSharpType(kvp.Key.DataType)).ToArray();

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
                        import.Write(ConvertToColumnType(r[dc], types[index]), types[index]);
                }
            }

            import.Complete();
        }

        return dt.Rows.Count;
    }

    /// <summary>
    ///     Converts <paramref name="value" /> where Npgsql would not itself write it as the <paramref name="columnType" />.
    ///     Npgsql writes any number to the integer and numeric types, except a float or double too large for a decimal
    ///     (see <see cref="ToNumeric" />), and a float or double to real and double precision, the conversions here are
    ///     the ones it leaves out.
    /// </summary>
    private static object ConvertToColumnType(object value, NpgsqlDbType columnType) =>
        columnType switch
        {
            NpgsqlDbType.Char or NpgsqlDbType.Varchar or NpgsqlDbType.Text => value switch
            {
                //the text the server itself produces when it casts these types to a string
                DateTime dateTime => dateTime.ToString("yyyy-MM-dd HH:mm:ss.FFFFFF", CultureInfo.InvariantCulture),
                DateOnly date => date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                TimeOnly time => time.ToString("HH:mm:ss.FFFFFF", CultureInfo.InvariantCulture),
                bool boolean => boolean ? "true" : "false",
                IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
                _ => value
            },
            NpgsqlDbType.Double when value is not (double or float) =>
                Convert.ToDouble(value, CultureInfo.InvariantCulture),
            NpgsqlDbType.Real when value is not (double or float) =>
                Convert.ToSingle(value, CultureInfo.InvariantCulture),
            NpgsqlDbType.Numeric when value is double or float => ToNumeric(value),
            NpgsqlDbType.Timestamp => value switch
            {
                DateTime dateTime => PostgreSqlTypeTranslater.ToTimestamp(dateTime),
                //a date is midnight at the start of that day
                DateOnly date => PostgreSqlTypeTranslater.ToTimestamp(date.ToDateTime(TimeOnly.MinValue)),
                _ => value
            },
            NpgsqlDbType.TimestampTz => value switch
            {
                DateTime dateTime => PostgreSqlTypeTranslater.ToTimestampTz(dateTime),
                //a date is midnight at the start of that day
                DateOnly date => PostgreSqlTypeTranslater.ToTimestampTz(date.ToDateTime(TimeOnly.MinValue)),
                _ => value
            },
            _ => value
        };

    /// <summary>
    ///     Returns a float or double for a numeric column in a form Npgsql can write. Npgsql writes a float or double
    ///     to numeric through a decimal, which cannot hold one of 2^96 (about 7.9e28) or more: such a value, which is a
    ///     whole number, is written as the <see cref="BigInteger" /> of its shortest text, so that 1e30 is 10^30.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    ///     The value is NaN or infinite, which Npgsql cannot write to numeric
    /// </exception>
    private static object ToNumeric(object value)
    {
        var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
        if (!double.IsFinite(number))
            throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture,
                "Value {0} cannot be bulk inserted into a numeric column: Npgsql writes numeric values from decimals",
                value));

        if (Math.Abs(number) < DecimalOverflow)
            return value;

        var text = value is float single
            ? single.ToString("R", CultureInfo.InvariantCulture)
            : number.ToString("R", CultureInfo.InvariantCulture);
        return BigInteger.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture);
    }

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(_disposed, this);

    /// <summary>
    ///     Releases all resources used by the PostgreSqlBulkCopy.
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
