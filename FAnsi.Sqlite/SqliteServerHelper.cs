using System;
using System.Collections.Generic;
using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;
using Microsoft.Data.Sqlite;

namespace FAnsi.Implementations.Sqlite;

public sealed class SqliteServerHelper : DiscoveredServerHelper
{
    public static readonly SqliteServerHelper Instance = new();
    private SqliteServerHelper() : base(DatabaseType.Sqlite)
    {
    }

    //the name of the properties on DbConnectionStringBuilder that correspond to server and database
    protected override string ServerKeyName => "Data Source";
    protected override string DatabaseKeyName => "Data Source";

    protected override string ConnectionTimeoutKeyName => "Default Timeout";

    #region Up Typing
    public override DbCommand GetCommand(string s, DbConnection con, DbTransaction? transaction = null) => new SqliteCommand(s, (SqliteConnection)con, transaction as SqliteTransaction);

    public override DbDataAdapter GetDataAdapter(DbCommand cmd) => throw new NotSupportedException("SQLite does not support DbDataAdapter directly");

    public override DbCommandBuilder GetCommandBuilder(DbCommand cmd) => throw new NotSupportedException("SQLite does not support DbCommandBuilder directly");

    public override DbParameter GetParameter(string parameterName) => new SqliteParameter(parameterName, null);

    public override DbConnection GetConnection(DbConnectionStringBuilder builder) => new SqliteConnection(builder.ConnectionString);

    protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string? connectionString) =>
        connectionString != null ? new SqliteConnectionStringBuilder(connectionString) : new SqliteConnectionStringBuilder();

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

    public override DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder)
    {
        // SQLite connections are inherently async-ready
        return builder;
    }

    public override IDiscoveredDatabaseHelper GetDatabaseHelper() => new SqliteDatabaseHelper();

    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => SqliteQuerySyntaxHelper.Instance;

    public override IEnumerable<string> ListDatabases(DbConnectionStringBuilder builder)
    {
        // SQLite is file-based, so there's only one "database" per connection
        var filePath = builder.TryGetValue("Data Source", out var dataSource) ? dataSource?.ToString() : null;
        if (!string.IsNullOrEmpty(filePath))
            yield return filePath;
    }

    public override IEnumerable<string> ListDatabases(DbConnection connection)
    {
        // SQLite is file-based, so there's only one "database" per connection
        var builder = GetConnectionStringBuilder(connection.ConnectionString);
        return ListDatabases(builder);
    }

    public override bool DatabaseExists(DiscoveredDatabase database)
    {
        var filePath = database.GetRuntimeName();
        return !string.IsNullOrEmpty(filePath) && System.IO.File.Exists(filePath);
    }

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

    public override string? GetExplicitUsernameIfAny(DbConnectionStringBuilder builder)
    {
        // SQLite doesn't use username authentication
        return null;
    }

    public override string? GetExplicitPasswordIfAny(DbConnectionStringBuilder builder)
    {
        // SQLite doesn't use password authentication
        return null;
    }

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
