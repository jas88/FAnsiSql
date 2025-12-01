using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.ConnectionStringDefaults;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;
using MySqlConnector;

namespace FAnsi.Implementations.MySql;

public sealed class MySqlServerHelper : DiscoveredServerHelper
{
    public static readonly MySqlServerHelper Instance = new();
    static MySqlServerHelper()
    {
        AddConnectionStringKeyword(DatabaseType.MySql, "AllowUserVariables", "True", ConnectionStringKeywordPriority.ApiRule);
        AddConnectionStringKeyword(DatabaseType.MySql, "AllowLoadLocalInfile", "True", ConnectionStringKeywordPriority.ApiRule);
        AddConnectionStringKeyword(DatabaseType.MySql, "CharSet", "utf8mb4", ConnectionStringKeywordPriority.ApiRule);
    }

    private MySqlServerHelper() : base(DatabaseType.MySql)
    {
    }

    protected override string ServerKeyName => "Server";
    protected override string DatabaseKeyName => "Database";

    #region Up Typing
    public override DbCommand GetCommand(string s, DbConnection con, DbTransaction? transaction = null) =>
        new MySqlCommand(s, con as MySqlConnection, transaction as MySqlTransaction);

    public override DbDataAdapter GetDataAdapter(DbCommand cmd) => new MySqlDataAdapter(cmd as MySqlCommand ??
        throw new ArgumentException("Incorrect command type", nameof(cmd)));

    public override DbCommandBuilder GetCommandBuilder(DbCommand cmd) =>
        new MySqlCommandBuilder((MySqlDataAdapter)GetDataAdapter(cmd));

    public override DbParameter GetParameter(string parameterName) => new MySqlParameter(parameterName, null);

    public override DbConnection GetConnection(DbConnectionStringBuilder builder) =>
        new MySqlConnection(builder.ConnectionString);

    protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string? connectionString) =>
        connectionString != null
            ? new MySqlConnectionStringBuilder(connectionString)
            : new MySqlConnectionStringBuilder();

    protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string server, string? database, string username, string password)
    {
        var toReturn = new MySqlConnectionStringBuilder
        {
            Server = server
        };

        if (!string.IsNullOrWhiteSpace(database))
            toReturn.Database = database;

        if (!string.IsNullOrWhiteSpace(username))
        {
            toReturn.UserID = username;
            toReturn.Password = password;
        }

        return toReturn;
    }

    #endregion

    public override DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder) => builder; //no special stuff required?

    public override IDiscoveredDatabaseHelper GetDatabaseHelper() => new MySqlDatabaseHelper();

    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => MySqlQuerySyntaxHelper.Instance;

    public override void CreateDatabase(DbConnectionStringBuilder builder, IHasRuntimeName newDatabaseName)
    {
        var b = (MySqlConnectionStringBuilder)GetConnectionStringBuilder(builder.ConnectionString);
        b.Database = null;

        using var con = new MySqlConnection(b.ConnectionString);
        con.Open();
        using var cmd = GetCommand($"CREATE DATABASE `{newDatabaseName.GetRuntimeName()}`", con);
        cmd.CommandTimeout = CreateDatabaseTimeoutInSeconds;
        cmd.ExecuteNonQuery();
    }

    public override Dictionary<string, string> DescribeServer(DbConnectionStringBuilder builder) => throw new NotImplementedException();

    public override string GetExplicitUsernameIfAny(DbConnectionStringBuilder builder) => ((MySqlConnectionStringBuilder)builder).UserID;

    public override string GetExplicitPasswordIfAny(DbConnectionStringBuilder builder) => ((MySqlConnectionStringBuilder)builder).Password;

    public override Version? GetVersion(DiscoveredServer server)
    {
        using var con = server.GetConnection();
        con.Open();
        using var cmd = server.GetCommand("show variables like \"version\"", con);
        using var r = cmd.ExecuteReader();
        if (r.Read())
            return r["Value"] == DBNull.Value ? null : CreateVersionFromString((string)r["Value"]);

        return null;
    }

    public override IEnumerable<string> ListDatabases(DbConnectionStringBuilder builder)
    {
        var b = (MySqlConnectionStringBuilder)GetConnectionStringBuilder(builder.ConnectionString);
        b.Database = null;

        using var con = new MySqlConnection(b.ConnectionString);
        con.Open();
        foreach (var listDatabase in ListDatabases(con)) yield return listDatabase;
    }

    public override IEnumerable<string> ListDatabases(DbConnection con)
    {
        using var cmd = GetCommand("show databases;", con);
        using var r = cmd.ExecuteReader();
        while (r.Read())
            yield return (string)r["Database"];
    }

    public override bool HasDanglingTransaction(DbConnection connection)
    {
        // MySQL connections in a transaction have @@in_transaction set
        if (connection is MySqlConnection && connection.State == ConnectionState.Open)
        {
            try
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText = "SELECT @@in_transaction";
                cmd.CommandTimeout = 1;
                var result = cmd.ExecuteScalar();
                return result != null && Convert.ToInt32(result, CultureInfo.InvariantCulture) > 0;
            }
            catch
            {
                // If we can't check, assume no dangling transaction and let IsConnectionAlive's
                // "SELECT 1" test determine if the connection is actually usable
                return false;
            }
        }
        return false;
    }

    /// <summary>
    /// Checks if the database exists using the provided connection.
    /// </summary>
    /// <param name="database">The database to check</param>
    /// <param name="connection">The managed connection to use</param>
    /// <returns>True if the database exists, false otherwise</returns>
    public bool DatabaseExists(DiscoveredDatabase database, IManagedConnection connection)
    {
        using var cmd = new MySqlCommand("SELECT CASE WHEN EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = @name) THEN 1 ELSE 0 END", (MySqlConnection)connection.Connection);
        cmd.Transaction = (MySqlTransaction?)connection.Transaction;
        cmd.Parameters.AddWithValue("@name", database.GetRuntimeName());
        return Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture) == 1;
    }

    public override bool DatabaseExists(DiscoveredDatabase database)
    {
        // Remove database from connection string - INFORMATION_SCHEMA is accessible from any connection
        var builder = new MySqlConnectionStringBuilder(database.Server.Builder.ConnectionString)
        {
            Database = "" // Don't specify a database
        };
        var serverOnly = new DiscoveredServer(builder.ConnectionString, DatabaseType.MySql);
        using var con = serverOnly.GetManagedConnection();
        return DatabaseExists(database, con);
    }

    public override string GetServerLevelConnectionKey(string connectionString)
    {
        // Remove database name for server-level pooling
        var builder = new MySqlConnectionStringBuilder(connectionString)
        {
            Database = "" // Remove database
        };
        return builder.ConnectionString;
    }
}
