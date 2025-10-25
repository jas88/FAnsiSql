using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using Oracle.ManagedDataAccess.Client;

namespace FAnsi.Implementations.Oracle;

public sealed class OracleImplementation() : Implementation<OracleConnectionStringBuilder>(DatabaseType.Oracle)
{
#pragma warning disable CS0618 // Type or member is obsolete
    static OracleImplementation() => ImplementationManager.Load<OracleImplementation>();
#pragma warning restore CS0618 // Type or member is obsolete

    public override IDiscoveredServerHelper GetServerHelper() => OracleServerHelper.Instance;

    public override bool IsFor(DbConnection connection) => connection is OracleConnection;

    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => OracleQuerySyntaxHelper.Instance;
}
