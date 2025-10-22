using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using FAnsi.Connections;
using FAnsi.Discovery.ConnectionStringDefaults;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;

namespace FAnsi.Discovery;

/// <summary>
/// DBMS specific implementation of all functionality that relates to interacting with existing server (testing connections, creating databases, etc).
/// </summary>
public abstract partial class DiscoveredServerHelper(DatabaseType databaseType) : IDiscoveredServerHelper
{
    private static readonly Dictionary<DatabaseType, ConnectionStringKeywordAccumulator> ConnectionStringKeywordAccumulators = [];

    /// <summary>
    /// Register a system-wide rule that all connection strings of <paramref name="databaseType"/> should include the given <paramref name="keyword"/>.
    /// </summary>
    /// <param name="databaseType"></param>
    /// <param name="keyword"></param>
    /// <param name="value"></param>
    /// <param name="priority">Resolves conflicts when multiple calls are made for the same <paramref name="keyword"/> at different times</param>
    public static void AddConnectionStringKeyword(DatabaseType databaseType, string keyword, string value, ConnectionStringKeywordPriority priority)
    {
        if (!ConnectionStringKeywordAccumulators.ContainsKey(databaseType))
            ConnectionStringKeywordAccumulators.Add(databaseType, new ConnectionStringKeywordAccumulator(databaseType));

        ConnectionStringKeywordAccumulators[databaseType].AddOrUpdateKeyword(keyword, value, priority);
    }

    /// <summary>
    /// Creates a new DbCommand for the specified database type.
    /// </summary>
    /// <include file='../../CommonMethods.doc.xml' path='Methods/Method[@name="GetCommand"]'/>
    public abstract DbCommand GetCommand(string s, DbConnection con, DbTransaction? transaction = null);

    /// <summary>
    /// Creates a new DbDataAdapter for the specified database type.
    /// </summary>
    /// <include file='../../CommonMethods.doc.xml' path='Methods/Method[@name="GetDataAdapter"]'/>
    public abstract DbDataAdapter GetDataAdapter(DbCommand cmd);

    /// <summary>
    /// Creates a new DbCommandBuilder for the specified database type.
    /// </summary>
    /// <include file='../../CommonMethods.doc.xml' path='Methods/Method[@name="GetCommandBuilder"]'/>
    public abstract DbCommandBuilder GetCommandBuilder(DbCommand cmd);

    /// <summary>
    /// Creates a new DbParameter for the specified database type.
    /// </summary>
    /// <include file='../../CommonMethods.doc.xml' path='Methods/Method[@name="GetParameter"]'/>
    public abstract DbParameter GetParameter(string parameterName);

    public abstract DbConnection GetConnection(DbConnectionStringBuilder builder);

    public DbConnectionStringBuilder GetConnectionStringBuilder(string? connectionString)
    {
        var builder = GetConnectionStringBuilderImpl(connectionString);
        EnforceKeywords(builder);

        return builder;
    }

    /// <inheritdoc/>
    public DbConnectionStringBuilder GetConnectionStringBuilder(string server, string? database, string username, string password)
    {
        var builder = GetConnectionStringBuilderImpl(server, database, username, password);
        EnforceKeywords(builder);
        return builder;
    }

    /// <summary>
    /// Modifies the <paramref name="builder"/> with the connection string keywords
    /// specified in <see cref="ConnectionStringKeywordAccumulators"/>.  Override to
    /// perform last second changes to connection strings.
    /// </summary>
    /// <param name="builder"></param>
    protected virtual void EnforceKeywords(DbConnectionStringBuilder builder)
    {
        //if we have any keywords to enforce
        if (ConnectionStringKeywordAccumulators.TryGetValue(DatabaseType, out var accumulator))
            accumulator.EnforceOptions(builder);
    }

    protected abstract DbConnectionStringBuilder GetConnectionStringBuilderImpl(string connectionString, string? database, string username, string password);
    protected abstract DbConnectionStringBuilder GetConnectionStringBuilderImpl(string? connectionString);


    protected abstract string ServerKeyName { get; }
    protected abstract string DatabaseKeyName { get; }
    protected virtual string ConnectionTimeoutKeyName => "ConnectionTimeout";

    public string? GetServerName(DbConnectionStringBuilder builder)
    {
        var s = (string)builder[ServerKeyName];
        return string.IsNullOrWhiteSpace(s) ? null : s;
    }

    public DbConnectionStringBuilder ChangeServer(DbConnectionStringBuilder builder, string newServer)
    {
        builder[ServerKeyName] = newServer;
        return builder;
    }

    public virtual string? GetCurrentDatabase(DbConnectionStringBuilder builder) => (string)builder[DatabaseKeyName];

    public virtual DbConnectionStringBuilder ChangeDatabase(DbConnectionStringBuilder builder, string newDatabase)
    {
        var newBuilder = GetConnectionStringBuilder(builder.ConnectionString);
        newBuilder[DatabaseKeyName] = newDatabase;
        return newBuilder;
    }

    public abstract IEnumerable<string> ListDatabases(DbConnectionStringBuilder builder);
    public abstract IEnumerable<string> ListDatabases(DbConnection con);

    public async IAsyncEnumerable<string> ListDatabasesAsync(DbConnectionStringBuilder builder, [EnumeratorCancellation] CancellationToken token)
    {
        //list the database on the server
        await using var con = GetConnection(builder);

        //this will work or timeout
        await con.OpenAsync(token);

        foreach (var db in ListDatabases(con))
            yield return db;
    }

    public abstract DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder);

    public abstract IDiscoveredDatabaseHelper GetDatabaseHelper();
    public abstract IQuerySyntaxHelper GetQuerySyntaxHelper();

    public abstract void CreateDatabase(DbConnectionStringBuilder builder, IHasRuntimeName newDatabaseName);

    public ManagedTransaction BeginTransaction(DbConnectionStringBuilder builder)
    {
        var con = GetConnection(builder);
        con.Open();
        var transaction = con.BeginTransaction();

        return new ManagedTransaction(con, transaction);
    }

    public DatabaseType DatabaseType { get; private set; } = databaseType;
    public abstract Dictionary<string, string> DescribeServer(DbConnectionStringBuilder builder);

    public bool RespondsWithinTime(DbConnectionStringBuilder builder, int timeoutInSeconds, out Exception? exception)
    {
        try
        {
            var copyBuilder = GetConnectionStringBuilder(builder.ConnectionString);
            copyBuilder[ConnectionTimeoutKeyName] = timeoutInSeconds;

            using var con = GetConnection(copyBuilder);
            con.Open();

            con.Close();

            exception = null;
            return true;
        }
        catch (Exception e)
        {
            exception = e;
            return false;
        }
    }

    public abstract string? GetExplicitUsernameIfAny(DbConnectionStringBuilder builder);
    public abstract string? GetExplicitPasswordIfAny(DbConnectionStringBuilder builder);
    public abstract Version? GetVersion(DiscoveredServer server);

    private static readonly Regex RVagueVersion = RVagueVersionRe();

    /// <summary>
    /// Number of seconds to allow <see cref="CreateDatabase(DbConnectionStringBuilder, IHasRuntimeName)"/> to run for before timing out.
    /// Defaults to 30.
    /// </summary>
    public static int CreateDatabaseTimeoutInSeconds { get; set; } = 30;

    /// <summary>
    /// Returns a new <see cref="Version"/> by parsing the <paramref name="versionString"/>.  If the string
    /// is a valid version the full version string is represented otherwise a regex match is used to find
    /// numbers with dots separating them (e.g. 1.2.3  / 5.1 etc).
    /// </summary>
    /// <param name="versionString"></param>
    /// <returns></returns>
    protected static Version? CreateVersionFromString(string versionString)
    {
        if (Version.TryParse(versionString, out var result))
            return result;

        var m = RVagueVersion.Match(versionString);
        return m.Success ? Version.Parse(m.Value) :
            //whatever the string was it didn't even remotely resemble a Version
            null;
    }

    [GeneratedRegex(@"\d+\.\d+(\.\d+)?(\.\d+)?", RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex RVagueVersionRe();

    /// <summary>
    /// Validates that a connection is still alive and usable.
    /// Override in database-specific implementations to provide DBMS-specific validation logic.
    /// </summary>
    /// <param name="connection">The connection to validate</param>
    /// <returns>True if the connection is alive and usable</returns>
    public virtual bool IsConnectionAlive(DbConnection connection)
    {
        try
        {
            // Check for dangling transactions (fixes #30)
            if (HasDanglingTransaction(connection))
                return false;

            // Try a simple command to verify connection is usable
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT 1";
            cmd.CommandTimeout = 1;
            cmd.ExecuteScalar();
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Checks if the connection has a dangling transaction from previous use.
    /// Override in database-specific implementations to check the concrete connection type's Transaction property.
    /// </summary>
    /// <param name="connection">The connection to check</param>
    /// <returns>True if the connection has an uncommitted transaction</returns>
    public virtual bool HasDanglingTransaction(DbConnection connection) => false;

    /// <summary>
    /// Efficiently checks if a database exists using a direct SQL query instead of listing all databases.
    /// </summary>
    /// <param name="database">The database to check for existence</param>
    /// <returns>True if the database exists, false otherwise</returns>
    public abstract bool DatabaseExists(DiscoveredDatabase database);
}
