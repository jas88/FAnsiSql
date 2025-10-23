using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Threading;
using FAnsi.Discovery;

namespace FAnsi.Connections;

/// <summary>
/// Provides thread-local connection pooling for DiscoveredServer instances to eliminate
/// ephemeral connection churn. For SQL Server and MySQL, maintains one connection per server
/// per thread and switches databases as needed. For PostgreSQL and Oracle, maintains one
/// connection per database per thread.
/// </summary>
internal static class ManagedConnectionPool
{
    /// <summary>
    /// Thread-local storage for server-level pooled connections (SQL Server, MySQL).
    /// Keyed by server-level connection string (without database name).
    /// </summary>
    private static readonly ThreadLocal<ConcurrentDictionary<string, ServerPooledConnection>> _threadLocalServerConnections =
        new(() => new ConcurrentDictionary<string, ServerPooledConnection>(), trackAllValues: true);

    /// <summary>
    /// Thread-local storage for database-level connections (PostgreSQL, Oracle fallback).
    /// Keyed by full connection string (includes database name).
    /// </summary>
    private static readonly ThreadLocal<ConcurrentDictionary<string, IManagedConnection>> _threadLocalDatabaseConnections =
        new(() => new ConcurrentDictionary<string, IManagedConnection>(), trackAllValues: true);

    /// <summary>
    /// Gets a pooled managed connection for the specified server. For SQL Server and MySQL,
    /// returns a server-level connection and switches databases as needed. For PostgreSQL and Oracle,
    /// returns a database-level connection.
    /// </summary>
    /// <param name="server">The discovered server to connect to</param>
    /// <param name="transaction">Optional transaction to use (if specified, bypasses pooling)</param>
    /// <returns>A managed connection that should not be disposed (CloseOnDispose = false)</returns>
    internal static IManagedConnection GetPooledConnection(DiscoveredServer server, IManagedTransaction? transaction = null)
    {
        // If we have a transaction, create a standard non-pooled connection directly (bypassing pool to avoid recursion)
        if (transaction != null)
            return new ManagedConnection(server, transaction);

        // Oracle: Skip thread-local pooling and rely on ADO.NET's native Oracle pooling
        // We can't reliably detect dangling transactions at the SQL level for Oracle
        // Return a normal connection (CloseOnDispose=true) so it's properly returned to ADO.NET's pool
        if (server.DatabaseType == DatabaseType.Oracle)
            return new ManagedConnection(server, null);

        // SQL Server and MySQL: Use server-level pooling with database switching
        if (server.DatabaseType == DatabaseType.MicrosoftSQLServer || server.DatabaseType == DatabaseType.MySql)
            return GetServerLevelPooledConnection(server);

        // PostgreSQL: Use database-level pooling (cannot switch databases on same connection)
        return GetDatabaseLevelPooledConnection(server);
    }

    /// <summary>
    /// Gets a server-level pooled connection for SQL Server or MySQL, switching databases as needed.
    /// </summary>
    private static IManagedConnection GetServerLevelPooledConnection(DiscoveredServer server)
    {
        var serverKey = server.Helper.GetServerLevelConnectionKey(server.Builder.ConnectionString);
        var targetDatabase = server.GetCurrentDatabase()?.GetRuntimeName();
        var threadServerConnections = _threadLocalServerConnections.Value;

        // Try to get existing server connection
        if (threadServerConnections != null && threadServerConnections.TryGetValue(serverKey, out var existingServerConn))
        {
            // Verify connection is still valid
            if (existingServerConn?.IsValid() == true)
            {
                try
                {
                    // Switch to target database if needed
                    if (!string.IsNullOrWhiteSpace(targetDatabase))
                        existingServerConn.SwitchDatabase(targetDatabase);

                    // Return a non-disposing wrapper
                    var wrapper = existingServerConn.ManagedConnection.Clone();
                    wrapper.CloseOnDispose = false;
                    return wrapper;
                }
                catch
                {
                    // Database switch failed, remove and recreate
                    threadServerConnections.TryRemove(serverKey, out _);
                    try
                    {
                        existingServerConn?.Dispose();
                    }
                    catch
                    {
                        // Swallow disposal errors
                    }
                }
            }
            else
            {
                // Connection is invalid, remove it
                threadServerConnections.TryRemove(serverKey, out _);
                try
                {
                    existingServerConn?.Dispose();
                }
                catch
                {
                    // Swallow disposal errors
                }
            }
        }

        // Create new server-level connection
        // Connect to a system database initially, then switch to target
        var systemDatabase = server.DatabaseType switch
        {
            DatabaseType.MicrosoftSQLServer => "master",
            DatabaseType.MySql => "mysql",
            _ => targetDatabase // Fallback to target database
        };

        var serverLevelBuilder = server.Helper.GetConnectionStringBuilder(serverKey);
        if (!string.IsNullOrWhiteSpace(systemDatabase))
            serverLevelBuilder = server.Helper.ChangeDatabase(serverLevelBuilder, systemDatabase);

        var serverLevelServer = new DiscoveredServer(serverLevelBuilder.ConnectionString, server.DatabaseType);
        var newConnection = new ManagedConnection(serverLevelServer, null);
        newConnection.CloseOnDispose = false;  // Don't close - we manage the lifetime

        var serverPooledConn = new ServerPooledConnection(
            newConnection,
            server.DatabaseType,
            server.Helper,
            systemDatabase);

        // Switch to target database if different from system database
        if (!string.IsNullOrWhiteSpace(targetDatabase) &&
            !string.Equals(targetDatabase, systemDatabase, StringComparison.OrdinalIgnoreCase))
        {
            serverPooledConn.SwitchDatabase(targetDatabase);
        }

        // Store it
        if (threadServerConnections != null)
            threadServerConnections[serverKey] = serverPooledConn;

        // Return a non-disposing wrapper
        var returnWrapper = newConnection.Clone();
        returnWrapper.CloseOnDispose = false;
        return returnWrapper;
    }

    /// <summary>
    /// Gets a database-level pooled connection (for PostgreSQL).
    /// </summary>
    private static IManagedConnection GetDatabaseLevelPooledConnection(DiscoveredServer server)
    {
        var connectionKey = server.Builder.ConnectionString;
        var threadDbConnections = _threadLocalDatabaseConnections.Value;

        // Try to get existing connection for this database
        if (threadDbConnections != null && threadDbConnections.TryGetValue(connectionKey, out var existingConnection))
        {
            // Verify connection is still valid and not in a transaction
            if (existingConnection?.Connection.State == ConnectionState.Open &&
                existingConnection.Transaction == null &&
                server.Helper.IsConnectionAlive(existingConnection.Connection))
            {
                // Return a non-disposing wrapper
                var wrapper = existingConnection.Clone();
                wrapper.CloseOnDispose = false;
                return wrapper;
            }

            // Connection is invalid, remove it
            threadDbConnections.TryRemove(connectionKey, out _);
            try
            {
                existingConnection?.Connection?.Dispose();
            }
            catch
            {
                // Swallow disposal errors
            }
        }

        // Create new database-level connection
        var newConnection = new ManagedConnection(server, null);
        newConnection.CloseOnDispose = false;

        // Store it
        if (threadDbConnections != null)
            threadDbConnections[connectionKey] = newConnection;

        // Return a non-disposing wrapper
        var returnWrapper = newConnection.Clone();
        returnWrapper.CloseOnDispose = false;
        return returnWrapper;
    }

    /// <summary>
    /// Clears all pooled connections for the current thread.
    /// Useful for cleanup or when you want to force new connections.
    /// </summary>
    internal static void ClearCurrentThreadConnections()
    {
        // Clear server-level connections (SQL Server, MySQL)
        var threadServerConnections = _threadLocalServerConnections.Value;
        if (threadServerConnections != null)
        {
            foreach (var kvp in threadServerConnections)
            {
                try
                {
                    kvp.Value?.Dispose();
                }
                catch
                {
                    // Swallow exceptions during cleanup
                }
            }
            threadServerConnections.Clear();
        }

        // Clear database-level connections (PostgreSQL)
        var threadDbConnections = _threadLocalDatabaseConnections.Value;
        if (threadDbConnections != null)
        {
            foreach (var kvp in threadDbConnections)
            {
                try
                {
                    if (kvp.Value?.Connection.State == ConnectionState.Open)
                    {
                        kvp.Value.Connection.Close();
                        kvp.Value.Connection.Dispose();
                    }
                }
                catch
                {
                    // Swallow exceptions during cleanup
                }
            }
            threadDbConnections.Clear();
        }
    }

    /// <summary>
    /// Clears all pooled connections across all threads.
    /// Should be called during application shutdown.
    /// </summary>
    internal static void ClearAllConnections()
    {
        // Clear server-level connections (SQL Server, MySQL)
        if (_threadLocalServerConnections?.Values != null)
        {
            foreach (var threadServerConnections in _threadLocalServerConnections.Values)
            {
                if (threadServerConnections == null) continue;

                foreach (var kvp in threadServerConnections)
                {
                    try
                    {
                        kvp.Value?.Dispose();
                    }
                    catch
                    {
                        // Swallow exceptions during cleanup
                    }
                }
                threadServerConnections.Clear();
            }
        }

        // Clear database-level connections (PostgreSQL)
        if (_threadLocalDatabaseConnections?.Values != null)
        {
            foreach (var threadDbConnections in _threadLocalDatabaseConnections.Values)
            {
                if (threadDbConnections == null) continue;

                foreach (var kvp in threadDbConnections)
                {
                    try
                    {
                        if (kvp.Value?.Connection.State == ConnectionState.Open)
                        {
                            kvp.Value.Connection.Close();
                            kvp.Value.Connection.Dispose();
                        }
                    }
                    catch
                    {
                        // Swallow exceptions during cleanup
                    }
                }
                threadDbConnections.Clear();
            }
        }
    }
}
