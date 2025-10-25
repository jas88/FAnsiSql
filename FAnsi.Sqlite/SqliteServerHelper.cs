using System;
using System.Collections.Generic;
using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;
using Microsoft.Data.Sqlite;

namespace FAnsi.Implementations.Sqlite;

/// <summary>
/// SQLite-specific implementation of server helper functionality. Handles SQLite connections,
/// database operations, and server-level queries.
/// </summary>
/// <remarks>
/// SQLite is file-based, so many traditional "server" concepts don't apply. Each database file
/// represents an independent database with its own connection.
/// </remarks>
public sealed class SqliteServerHelper : DiscoveredServerHelper
{
    /// <summary>
    /// Gets the singleton instance of <see cref="SqliteServerHelper"/>.
    /// </summary>
    public static readonly SqliteServerHelper Instance = new();

    private SqliteServerHelper() : base(DatabaseType.Sqlite)
    {
    }

    /// <summary>
    /// Gets the connection string property name for server (Data Source in SQLite).
    /// For SQLite, this is the file path to the database file.
    /// </summary>
    protected override string ServerKeyName => "Data Source";

    /// <summary>
    /// Gets the connection string property name for database (Data Source in SQLite).
    /// For SQLite, server and database are the same concept (file path).
    /// </summary>
    protected override string DatabaseKeyName => "Data Source";

    /// <summary>
    /// Gets the connection string property name for connection timeout.
    /// </summary>
    protected override string ConnectionTimeoutKeyName => "Default Timeout";

    #region Up Typing

    /// <inheritdoc />
    public override DbCommand GetCommand(string s, DbConnection con, DbTransaction? transaction = null) => new SqliteCommand(s, (SqliteConnection)con, transaction as SqliteTransaction);

    /// <summary>
    /// Gets a data adapter for the specified command.
    /// </summary>
    /// <param name="cmd">The command to create an adapter for</param>
    /// <exception cref="NotSupportedException">SQLite does not support DbDataAdapter directly through this API</exception>
    public override DbDataAdapter GetDataAdapter(DbCommand cmd) => throw new NotSupportedException("SQLite does not support DbDataAdapter directly");

    /// <summary>
    /// Gets a command builder for the specified command.
    /// </summary>
    /// <param name="cmd">The command to create a builder for</param>
    /// <exception cref="NotSupportedException">SQLite does not support DbCommandBuilder directly through this API</exception>
    public override DbCommandBuilder GetCommandBuilder(DbCommand cmd) => throw new NotSupportedException("SQLite does not support DbCommandBuilder directly");

    /// <inheritdoc />
    public override DbParameter GetParameter(string parameterName) => new SqliteParameter(parameterName, null);

    /// <inheritdoc />
    public override DbConnection GetConnection(DbConnectionStringBuilder builder) => new SqliteConnection(builder.ConnectionString);

    /// <inheritdoc />
    protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string? connectionString) =>
        connectionString != null ? new SqliteConnectionStringBuilder(connectionString) : new SqliteConnectionStringBuilder();

    /// <summary>
    /// Creates a connection string builder with the specified parameters.
    /// </summary>
    /// <param name="server">The server/file path (used if database is null)</param>
    /// <param name="database">The database file path (preferred over server)</param>
    /// <param name="username">Username (ignored for SQLite as it has no authentication)</param>
    /// <param name="password">Password (ignored for SQLite as it has no authentication)</param>
    /// <returns>A configured connection string builder</returns>
    /// <remarks>
    /// SQLite doesn't use username/password authentication. These parameters are accepted for API compatibility but ignored.
    /// </remarks>
    protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string server, string? database, string username, string password)
    {
        var builder = new SqliteConnectionStringBuilder();

        // For SQLite, server and database are the same (file path)
        var dataSource = database ?? server;
        builder.DataSource = dataSource;

        // SQLite doesn't use username/password authentication, but we accept them
        return builder;
    }

    #endregion

    /// <summary>
    /// Enables async operations on the connection string builder.
    /// </summary>
    /// <param name="builder">The connection string builder to configure</param>
    /// <returns>The same builder (SQLite connections are inherently async-ready)</returns>
    /// <remarks>
    /// SQLite connections support async operations by default, so no modifications are needed.
    /// </remarks>
    public override DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder)
    {
        // SQLite connections are inherently async-ready
        return builder;
    }

    /// <inheritdoc />
    public override IDiscoveredDatabaseHelper GetDatabaseHelper() => new SqliteDatabaseHelper();

    /// <inheritdoc />
    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => SqliteQuerySyntaxHelper.Instance;

    /// <summary>
    /// Lists all databases accessible through the connection string builder.
    /// </summary>
    /// <param name="builder">The connection string builder</param>
    /// <returns>A single database path (SQLite is file-based)</returns>
    /// <remarks>
    /// SQLite is file-based, so there's only one "database" per connection string.
    /// Returns the Data Source file path if configured.
    /// </remarks>
    public override IEnumerable<string> ListDatabases(DbConnectionStringBuilder builder)
    {
        // SQLite is file-based, so there's only one "database" per connection
        var filePath = builder.TryGetValue("Data Source", out var dataSource) ? dataSource?.ToString() : null;
        if (!string.IsNullOrEmpty(filePath))
            yield return filePath;
    }

    /// <summary>
    /// Lists all databases accessible through the connection.
    /// </summary>
    /// <param name="connection">The database connection</param>
    /// <returns>A single database path (SQLite is file-based)</returns>
    /// <remarks>
    /// SQLite is file-based, so there's only one "database" per connection.
    /// </remarks>
    public override IEnumerable<string> ListDatabases(DbConnection connection)
    {
        // SQLite is file-based, so there's only one "database" per connection
        var builder = GetConnectionStringBuilder(connection.ConnectionString);
        return ListDatabases(builder);
    }

    /// <summary>
    /// Checks if the specified database file exists.
    /// </summary>
    /// <param name="database">The database to check</param>
    /// <returns>True if the database file exists on disk, false otherwise</returns>
    public override bool DatabaseExists(DiscoveredDatabase database)
    {
        var filePath = database.GetRuntimeName();
        return !string.IsNullOrEmpty(filePath) && System.IO.File.Exists(filePath);
    }

    /// <summary>
    /// Creates a new SQLite database file.
    /// </summary>
    /// <param name="builder">The connection string builder (not used for SQLite creation)</param>
    /// <param name="newDatabaseName">The name/path of the database file to create</param>
    /// <remarks>
    /// <para>SQLite databases are created automatically when a connection is opened to a non-existent file.</para>
    /// <para>This method ensures the parent directory exists before creating the database.</para>
    /// </remarks>
    public override void CreateDatabase(DbConnectionStringBuilder builder, IHasRuntimeName newDatabaseName)
    {
        var filePath = newDatabaseName.GetRuntimeName();

        // Ensure directory exists
        var directory = System.IO.Path.GetDirectoryName(filePath);
        if (!string.IsNullOrEmpty(directory) && !System.IO.Directory.Exists(directory))
        {
            System.IO.Directory.CreateDirectory(directory);
        }

        // Create the database file by opening a connection
        var newBuilder = new SqliteConnectionStringBuilder { DataSource = filePath };
        using var connection = new SqliteConnection(newBuilder.ConnectionString);
        connection.Open();
        // SQLite database is created when the connection is opened
    }

    /// <summary>
    /// Retrieves descriptive information about the SQLite server/database file.
    /// </summary>
    /// <param name="builder">The connection string builder</param>
    /// <returns>A dictionary containing file information and SQLite version</returns>
    /// <remarks>
    /// Returns file metadata (path, size, timestamps) and SQLite version if accessible.
    /// </remarks>
    public override Dictionary<string, string> DescribeServer(DbConnectionStringBuilder builder)
    {
        var toReturn = new Dictionary<string, string>();

        var filePath = builder.TryGetValue("Data Source", out var dataSource) ? dataSource?.ToString() : null;
        if (!string.IsNullOrEmpty(filePath) && System.IO.File.Exists(filePath))
        {
            var fileInfo = new System.IO.FileInfo(filePath);
            toReturn.Add("Database File", filePath);
            toReturn.Add("File Size", $"{fileInfo.Length} bytes");
            toReturn.Add("Created", fileInfo.CreationTime.ToString());
            toReturn.Add("Modified", fileInfo.LastWriteTime.ToString());
        }

        try
        {
            using var connection = GetConnection(builder);
            connection.Open();

            using var cmd = GetCommand("SELECT sqlite_version()", connection);
            var version = cmd.ExecuteScalar()?.ToString();
            if (!string.IsNullOrEmpty(version))
            {
                toReturn.Add("SQLite Version", version);
            }
        }
        catch (Exception ex)
        {
            toReturn.Add("Connection Error", ex.Message);
        }

        return toReturn;
    }

    /// <summary>
    /// Gets the username from the connection string if explicitly set.
    /// </summary>
    /// <param name="builder">The connection string builder</param>
    /// <returns>Always null (SQLite doesn't use authentication)</returns>
    /// <remarks>
    /// SQLite is a file-based database and doesn't support username/password authentication.
    /// </remarks>
    public override string? GetExplicitUsernameIfAny(DbConnectionStringBuilder builder)
    {
        // SQLite doesn't use username authentication
        return null;
    }

    /// <summary>
    /// Gets the password from the connection string if explicitly set.
    /// </summary>
    /// <param name="builder">The connection string builder</param>
    /// <returns>Always null (SQLite doesn't use authentication)</returns>
    /// <remarks>
    /// SQLite is a file-based database and doesn't support username/password authentication.
    /// </remarks>
    public override string? GetExplicitPasswordIfAny(DbConnectionStringBuilder builder)
    {
        // SQLite doesn't use password authentication
        return null;
    }

    /// <summary>
    /// Gets the SQLite version of the server.
    /// </summary>
    /// <param name="server">The server to query</param>
    /// <returns>The SQLite version, or null if unable to determine</returns>
    /// <remarks>
    /// Executes 'SELECT sqlite_version()' to retrieve the SQLite engine version.
    /// </remarks>
    public override Version? GetVersion(DiscoveredServer server)
    {
        try
        {
            using var connection = server.GetConnection();
            connection.Open();

            using var cmd = GetCommand("SELECT sqlite_version()", connection);
            var versionString = cmd.ExecuteScalar()?.ToString();

            return CreateVersionFromString(versionString ?? "");
        }
        catch
        {
            return null;
        }
    }
}
