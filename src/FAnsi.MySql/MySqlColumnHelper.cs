using System.Globalization;
using System.Text;
using FAnsi.Discovery;
using FAnsi.Naming;

namespace FAnsi.Implementations.MySql;

public sealed class MySqlColumnHelper : IDiscoveredColumnHelper
{
    public static readonly MySqlColumnHelper Instance = new();

    private MySqlColumnHelper()
    {
    }

    public string GetTopXSqlForColumn(IHasRuntimeName database, IHasFullyQualifiedNameToo table, IHasRuntimeName column,
        int topX, bool discardNulls)
    {
        var syntax = MySqlQuerySyntaxHelper.Instance;

        var sql = new StringBuilder();

        sql.Append(CultureInfo.InvariantCulture,
            $"SELECT {syntax.EnsureWrapped(column.GetRuntimeName())} FROM {table.GetFullyQualifiedName()}");

        if (discardNulls)
            sql.Append(CultureInfo.InvariantCulture,
                $" WHERE {syntax.EnsureWrapped(column.GetRuntimeName())} IS NOT NULL");

        sql.Append(CultureInfo.InvariantCulture, $" LIMIT {topX}");
        return sql.ToString();
    }

    public string GetAlterColumnToSql(DiscoveredColumn column, string newType, bool allowNulls)
    {
        var syntax = column.Table.Database.Server.GetQuerySyntaxHelper();
        return
            $"ALTER TABLE {column.Table.GetFullyQualifiedName()} MODIFY COLUMN {syntax.EnsureWrapped(column.GetRuntimeName())} {newType} {(allowNulls ? "NULL" : "NOT NULL")}";
    }
}
