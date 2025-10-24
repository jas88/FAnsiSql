using System.Data.Common;
using System.Runtime.CompilerServices;
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

internal static class AutoRegister
{
    [ModuleInitializer]
#pragma warning disable CS0618 // Type or member is obsolete
    internal static void Initialize() => ImplementationManager.Load<SqliteImplementation>();
#pragma warning restore CS0618 // Type or member is obsolete
}