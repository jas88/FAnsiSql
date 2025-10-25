using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using Microsoft.Data.SqlClient;

namespace FAnsi.Implementations.MicrosoftSQL;

public sealed class MicrosoftSQLImplementation()
    : Implementation<SqlConnectionStringBuilder>(DatabaseType.MicrosoftSQLServer)
{
#pragma warning disable CS0618 // Type or member is obsolete
    static MicrosoftSQLImplementation() => ImplementationManager.Load<MicrosoftSQLImplementation>();
#pragma warning restore CS0618 // Type or member is obsolete

    public override IDiscoveredServerHelper GetServerHelper() => MicrosoftSQLServerHelper.Instance;

    public override bool IsFor(DbConnection conn) => conn is SqlConnection;

    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => MicrosoftQuerySyntaxHelper.Instance;
}
