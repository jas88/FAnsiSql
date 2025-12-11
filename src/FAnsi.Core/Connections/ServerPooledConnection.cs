using System.Data;
using FAnsi.Discovery;

namespace FAnsi.Connections;

/// <summary>
///     Represents a server-level pooled connection that can switch between databases.
///     Used internally by ManagedConnectionPool for SQL Server and MySQL.
/// </summary>
internal sealed class ServerPooledConnection : IDisposable
{
    /// <summary>
    ///     The server helper for executing database-specific operations
    /// </summary>
    private readonly IDiscoveredServerHelper _serverHelper;

    public ServerPooledConnection(IManagedConnection managedConnection, DatabaseType databaseType,
        IDiscoveredServerHelper serverHelper, string? initialDatabase = null)
    {
        ManagedConnection = managedConnection ?? throw new ArgumentNullException(nameof(managedConnection));
        DatabaseType = databaseType;
        _serverHelper = serverHelper ?? throw new ArgumentNullException(nameof(serverHelper));
        CurrentDatabase = initialDatabase;
    }

    /// <summary>
    ///     The wrapped managed connection
    /// </summary>
    public IManagedConnection ManagedConnection { get; }

    /// <summary>
    ///     The current database context, or null if unknown/not applicable
    /// </summary>
    public string? CurrentDatabase { get; private set; }

    /// <summary>
    ///     The database type for this connection
    /// </summary>
    public DatabaseType DatabaseType { get; }

    public void Dispose()
    {
        ManagedConnection?.Connection?.Dispose();
    }

    /// <summary>
    ///     Switches the connection to the specified database if not already there.
    ///     Only supported for SQL Server and MySQL.
    /// </summary>
    /// <param name="targetDatabase">The database to switch to</param>
    public void SwitchDatabase(string targetDatabase)
    {
        if (string.IsNullOrWhiteSpace(targetDatabase))
            throw new ArgumentException("Database name cannot be null or whitespace", nameof(targetDatabase));

        // Already on the target database
        if (string.Equals(CurrentDatabase, targetDatabase, StringComparison.OrdinalIgnoreCase))
            return;

        switch (DatabaseType)
        {
            case DatabaseType.MicrosoftSQLServer:
                // SQL Server: Use USE command
                using (var cmd = ManagedConnection.Connection.CreateCommand())
                {
                    var syntax = _serverHelper.GetQuerySyntaxHelper();
                    cmd.CommandText = $"USE {syntax.EnsureWrapped(targetDatabase)}";
                    cmd.ExecuteNonQuery();
                }

                CurrentDatabase = targetDatabase;
                break;

            case DatabaseType.MySql:
                // MySQL: Use native ChangeDatabase method
                ManagedConnection.Connection.ChangeDatabase(targetDatabase);
                CurrentDatabase = targetDatabase;
                break;

            case DatabaseType.Oracle:
            case DatabaseType.PostgreSql:
                // Not supported - these should not be using server-level pooling
                throw new NotSupportedException($"Database switching is not supported for {DatabaseType}");

            default:
                throw new ArgumentOutOfRangeException(nameof(targetDatabase), $"Unknown database type: {DatabaseType}");
        }
    }

    /// <summary>
    ///     Validates that the connection is still alive and safe to reuse
    /// </summary>
    public bool IsValid() =>
        ManagedConnection.Connection.State == ConnectionState.Open &&
        ManagedConnection.Transaction == null &&
        _serverHelper.IsConnectionAlive(ManagedConnection.Connection) &&
        !_serverHelper.HasDanglingTransaction(ManagedConnection.Connection);
}
