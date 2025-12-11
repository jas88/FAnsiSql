using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;

namespace FAnsi.Implementation;

public interface IImplementation
{
    /// <summary>
    ///     The database type this implementation supports
    /// </summary>
    DatabaseType SupportedDatabaseType { get; }

    /// <summary>
    ///     The connection string builder type this implementation supports
    /// </summary>
    Type ConnectionStringBuilderType { get; }

    /// <summary>
    ///     The connection type this implementation supports
    /// </summary>
    Type ConnectionType { get; }

    DbConnectionStringBuilder GetBuilder();
    IDiscoveredServerHelper GetServerHelper();

    bool IsFor(DatabaseType databaseType);
    bool IsFor(DbConnectionStringBuilder builder);
    bool IsFor(DbConnection connection);

    IQuerySyntaxHelper GetQuerySyntaxHelper();
}
