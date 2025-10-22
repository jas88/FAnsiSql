using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using FAnsi.Connections;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;

namespace FAnsi.Discovery;

/// <summary>
/// Contains all the DatabaseType specific implementation logic required by DiscoveredServer.
/// </summary>
public interface IDiscoveredServerHelper
{
    /// <summary>
    /// Creates a new DbCommand for the specified database type.
    /// </summary>
    /// <include file='../../CommonMethods.doc.xml' path='Methods/Method[@name="GetCommand"]'/>
    DbCommand GetCommand(string s, DbConnection con, DbTransaction? transaction = null);

    DbDataAdapter GetDataAdapter(DbCommand cmd);
    DbCommandBuilder GetCommandBuilder(DbCommand cmd);
    DbParameter GetParameter(string parameterName);
    DbConnection GetConnection(DbConnectionStringBuilder builder);

    DbConnectionStringBuilder GetConnectionStringBuilder(string? connectionString);

    /// <summary>
    /// Returns a new connection string builder with the supplied parameters.  Note that if a concept is not supported in the
    /// <see cref="DbConnectionStringBuilder"/> implementation then the value will not appear in the connection string (e.g. Oracle
    /// does not support specifying a <paramref name="database"/> to connect to).
    /// </summary>
    /// <param name="server">The server/datasource to connect to e.g. "localhost\sqlexpress"</param>
    /// <param name="database">Optional database to connect to e.g. "master"</param>
    /// <param name="username">Optional username to set in connection string (otherwise integrated security will be used - if supported)</param>
    /// <param name="password">Optional password to set in connection string (otherwise integrated security will be used - if supported)</param>
    /// <returns></returns>
    DbConnectionStringBuilder GetConnectionStringBuilder(string server, string? database, string username, string password);

    string? GetServerName(DbConnectionStringBuilder builder);
    DbConnectionStringBuilder ChangeServer(DbConnectionStringBuilder builder, string newServer);

    string? GetCurrentDatabase(DbConnectionStringBuilder builder);
    DbConnectionStringBuilder ChangeDatabase(DbConnectionStringBuilder builder, string newDatabase);

    DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder);

    IEnumerable<string> ListDatabases(DbConnectionStringBuilder builder);
    IEnumerable<string> ListDatabases(DbConnection con);
    IAsyncEnumerable<string> ListDatabasesAsync(DbConnectionStringBuilder builder, CancellationToken token);

    IDiscoveredDatabaseHelper GetDatabaseHelper();
    IQuerySyntaxHelper GetQuerySyntaxHelper();

    void CreateDatabase(DbConnectionStringBuilder builder, IHasRuntimeName newDatabaseName);

    ManagedTransaction BeginTransaction(DbConnectionStringBuilder builder);
    DatabaseType DatabaseType { get; }
    Dictionary<string, string> DescribeServer(DbConnectionStringBuilder builder);
    bool RespondsWithinTime(DbConnectionStringBuilder builder, int timeoutInSeconds, out Exception? exception);

    string? GetExplicitUsernameIfAny(DbConnectionStringBuilder builder);
    string? GetExplicitPasswordIfAny(DbConnectionStringBuilder builder);
    Version? GetVersion(DiscoveredServer server);

    /// <summary>
    /// Validates that a connection is still alive and usable.
    /// </summary>
    /// <param name="connection">The connection to validate</param>
    /// <returns>True if the connection is alive and usable</returns>
    bool IsConnectionAlive(DbConnection connection);

    /// <summary>
    /// Checks if the connection has a dangling transaction from previous use.
    /// </summary>
    /// <param name="connection">The connection to check</param>
    /// <returns>True if the connection has an uncommitted transaction</returns>
    bool HasDanglingTransaction(DbConnection connection);

    /// <summary>
    /// Efficiently checks if a database exists using a direct SQL query instead of listing all databases.
    /// </summary>
    /// <param name="database">The database to check for existence</param>
    /// <returns>True if the database exists, false otherwise</returns>
    bool DatabaseExists(DiscoveredDatabase database);

    /// <summary>
    /// Gets a server-level connection string key by removing database-specific information.
    /// Used for server-level connection pooling where one connection is reused across databases.
    /// </summary>
    /// <param name="connectionString">The full connection string</param>
    /// <returns>Connection string with database name removed, or original if not supported</returns>
    string GetServerLevelConnectionKey(string connectionString);

    /// <summary>
    /// Creates a database-specific SQL query builder for LINQ-to-SQL translation.
    /// </summary>
    /// <returns>A query builder instance for this database type</returns>
    QueryableAbstraction.ISqlQueryBuilder GetQueryBuilder();
}
