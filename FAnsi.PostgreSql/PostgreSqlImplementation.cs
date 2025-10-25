using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using Npgsql;

namespace FAnsi.Implementations.PostgreSql;

public sealed class PostgreSqlImplementation() : Implementation<NpgsqlConnectionStringBuilder>(DatabaseType.PostgreSql)
{
#pragma warning disable CS0618 // Type or member is obsolete
    static PostgreSqlImplementation() => ImplementationManager.Load<PostgreSqlImplementation>();
#pragma warning restore CS0618 // Type or member is obsolete

    public override IDiscoveredServerHelper GetServerHelper() => PostgreSqlServerHelper.Instance;

    public override bool IsFor(DbConnection connection) => connection is NpgsqlConnection;

    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => PostgreSqlSyntaxHelper.Instance;
}
