using System.Data;
using System.Globalization;
using System.Text;
using FAnsi.Connections;
using FAnsi.Discovery;
using Npgsql;

namespace FAnsi.Implementations.PostgreSql;

public sealed class PostgreSqlBulkCopy(
    DiscoveredTable discoveredTable,
    IManagedConnection connection,
    CultureInfo culture) : BulkCopy(discoveredTable, connection, culture)
{
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
