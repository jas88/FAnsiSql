using System.Data;
using System.Data.Common;
using System.Diagnostics;
using FAnsi.Discovery;

namespace FAnsi.Connections;

/// <inheritdoc/>
public sealed class ManagedConnection : IManagedConnection
{
    /// <inheritdoc/>
    public DbConnection Connection { get; }

    /// <inheritdoc/>
    public DbTransaction? Transaction { get; private set; }

    /// <inheritdoc/>
    public IManagedTransaction? ManagedTransaction { get; private set; }

    /// <inheritdoc/>
    public bool CloseOnDispose { get; set; }

    private readonly DiscoveredServer _discoveredServer;

    internal ManagedConnection(DiscoveredServer discoveredServer, IManagedTransaction? managedTransaction)
    {
        _discoveredServer = discoveredServer;

        //get a new connection or use the existing one within the transaction
        Connection = discoveredServer.GetConnection(managedTransaction);

        //if there is a transaction, also store the transaction
        ManagedTransaction = managedTransaction;
        Transaction = managedTransaction?.Transaction;

        //if there isn't a transaction then we opened a new connection, so we had better remember to close it again
        if (managedTransaction != null) return;

        CloseOnDispose = true;
        Debug.Assert(Connection.State == ConnectionState.Closed);
        Connection.Open();
    }

    public ManagedConnection Clone()
    {
        var clone = (ManagedConnection)MemberwiseClone();
        // Clear transaction references to avoid stale transaction associations
        // when reusing pooled connections
        clone.Transaction = null;
        clone.ManagedTransaction = null;
        return clone;
    }

    /// <summary>
    /// Closes and disposes the DbConnection if explicitly requested (CloseOnDispose=true).
    /// For pooled connections, warns if disposing with an uncommitted transaction and relies on
    /// pool validation to reject dirty connections on next retrieval (fixes #30).
    /// </summary>
    public void Dispose()
    {
        if (CloseOnDispose)
        {
            Connection.Dispose();
            return;
        }

        // For pooled connections: warn if disposing with dangling transaction
        // Don't dispose here to avoid breaking active operations - let pool validation handle it
        if (ManagedTransaction == null &&
            Connection.State == ConnectionState.Open &&
            _discoveredServer.Helper.HasDanglingTransaction(Connection))
        {
            Debug.WriteLine($"Warning: Disposing pooled connection with uncommitted transaction. " +
                          $"This may indicate a bug where a transaction was not properly committed or rolled back. " +
                          $"Connection: {Connection.GetType().Name}");
        }
    }
}
