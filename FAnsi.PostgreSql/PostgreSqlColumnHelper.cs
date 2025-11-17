using System.Globalization;
using System.Text;
using FAnsi.Discovery;
using FAnsi.Naming;

namespace FAnsi.Implementations.PostgreSql;

public sealed class PostgreSqlColumnHelper : IDiscoveredColumnHelper
{
    public static readonly PostgreSqlColumnHelper Instance = new();
    private PostgreSqlColumnHelper() { }
    public string GetTopXSqlForColumn(IHasRuntimeName database, IHasFullyQualifiedNameToo table,
        IHasRuntimeName column, int topX,
        bool discardNulls)
    {
        var syntax = PostgreSqlSyntaxHelper.Instance;

        var sql = new StringBuilder($"SELECT {syntax.EnsureWrapped(column.GetRuntimeName())} FROM {table.GetFullyQualifiedName()}");

        if (discardNulls)
            sql.Append(CultureInfo.InvariantCulture, $" WHERE {syntax.EnsureWrapped(column.GetRuntimeName())} IS NOT NULL");

        sql.Append(CultureInfo.InvariantCulture, $" fetch first {topX} rows only");
        return sql.ToString();
    }

    public string GetAlterColumnToSql(DiscoveredColumn column, string newType, bool allowNulls)
    {
        var syntax = column.Table.Database.Server.GetQuerySyntaxHelper();

        var sb = new StringBuilder($@"ALTER TABLE {column.Table.GetFullyQualifiedName()} ALTER COLUMN {syntax.EnsureWrapped(column.GetRuntimeName())} TYPE {newType};");

        if (allowNulls != column.AllowNulls)
        {
            var nullabilityClause = allowNulls ? "DROP NOT NULL" : "SET NOT NULL";
            sb.AppendFormat(CultureInfo.InvariantCulture,
                $@"ALTER TABLE {column.Table.GetFullyQualifiedName()} ALTER COLUMN {syntax.EnsureWrapped(column.GetRuntimeName())} {nullabilityClause}");
        }
        return sb.ToString();
    }
}
