using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using Microsoft.Data.Sqlite;

namespace FAnsi.Implementations.Sqlite;

public sealed class SqliteImplementation()
    : Implementation<SqliteConnectionStringBuilder>(DatabaseType.Sqlite)
{
    public override IDiscoveredServerHelper GetServerHelper() => SqliteServerHelper.Instance;

    public override bool IsFor(DbConnection conn) => conn is SqliteConnection;

    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => SqliteQuerySyntaxHelper.Instance;
}