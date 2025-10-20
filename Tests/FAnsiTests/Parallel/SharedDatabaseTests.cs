using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using FAnsi;
using FAnsi.Connections;
using FAnsi.Discovery;
using NUnit.Framework;

namespace FAnsiTests.Parallel;

/// <summary>
/// Base class for parallel database tests. Each database type gets its own fixture
/// that runs tests sequentially on a single connection with transaction rollback.
/// </summary>
public abstract class SharedDatabaseTests : DatabaseTests
{
    private IManagedConnection? _connection;
    private DbTransaction? _transaction;
    private readonly List<DiscoveredTable> _createdTables = [];

    protected abstract DatabaseType DatabaseType { get; }

    [OneTimeSetUp]
    public void OpenDatabaseConnection()
    {
        var server = GetTestServer(DatabaseType);
        var db = server.ExpectDatabase(GetTestScratchDatabaseName());

        if (!db.Exists())
        {
            AssertCanCreateDatabases();
            db.Create();
        }

        _connection = server.GetManagedConnection();
        _connection.Connection.Open();
        _connection.Connection.ChangeDatabase(db.GetRuntimeName());
    }

    [SetUp]
    public void BeginTestTransaction()
    {
        _transaction = _connection!.Connection.BeginTransaction();
    }

    [TearDown]
    public void CleanupTest()
    {
        if (DatabaseType == DatabaseType.Oracle)
        {
            // Oracle: DDL causes implicit commit, so explicitly drop created resources
            foreach (var table in _createdTables.AsEnumerable().Reverse())
            {
                try
                {
                    if (table.Exists())
                        table.Drop();
                }
                catch
                {
                    // Best effort cleanup
                }
            }
        }
        else
        {
            // MySQL 8+ InnoDB, SQL Server, PostgreSQL: transaction rollback handles everything
            _transaction?.Rollback();
        }

        _createdTables.Clear();
        _transaction?.Dispose();
        _transaction = null;
    }

    [OneTimeTearDown]
    public void CloseDatabaseConnection()
    {
        _connection?.Dispose();
    }

    /// <summary>
    /// Gets the current database for this test fixture.
    /// Connection is already open and in a transaction.
    /// </summary>
    protected DiscoveredDatabase GetDatabase()
    {
        var server = GetTestServer(DatabaseType);
        return server.ExpectDatabase(GetTestScratchDatabaseName());
    }

    /// <summary>
    /// Track a table for cleanup. Only needed for Oracle (explicit DROP),
    /// but safe to call for all database types.
    /// </summary>
    protected void TrackForCleanup(DiscoveredTable table)
    {
        _createdTables.Add(table);
    }

    /// <summary>
    /// Helper to get the scratch database name from the base class.
    /// </summary>
    private string GetTestScratchDatabaseName()
    {
        // Access via a temporary database instance
        var temp = GetTestDatabase(DatabaseType, cleanDatabase: false);
        return temp.GetRuntimeName();
    }
}
