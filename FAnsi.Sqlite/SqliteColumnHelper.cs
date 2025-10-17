using System.Text;
using FAnsi.Discovery;
using FAnsi.Naming;

namespace FAnsi.Implementations.Sqlite;

public sealed class SqliteColumnHelper : IDiscoveredColumnHelper
{
    public string GetTopXSqlForColumn(IHasRuntimeName database, IHasFullyQualifiedNameToo table, IHasRuntimeName column, int topX, bool discardNulls)
    {
        var syntax = SqliteQuerySyntaxHelper.Instance;

        var sql = new StringBuilder();

        sql.Append($"SELECT {syntax.EnsureWrapped(column.GetRuntimeName())} FROM {table.GetFullyQualifiedName()}");

        if (discardNulls)
            sql.Append($" WHERE {syntax.EnsureWrapped(column.GetRuntimeName())} IS NOT NULL");

        sql.Append($" LIMIT {topX}");
        return sql.ToString();
    }

    public string GetAlterColumnToSql(DiscoveredColumn column, string newType, bool allowNulls)
    {
        // SQLite doesn't support ALTER COLUMN directly - would need to recreate table
        throw new System.NotSupportedException("SQLite does not support altering column types directly. Table recreation would be required.");
    }
}