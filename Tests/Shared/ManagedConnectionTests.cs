using System.Data;
using FAnsi;
using FAnsi.Connections;
using NUnit.Framework;

namespace FAnsiTests;

internal sealed class ManagedConnectionTests : DatabaseTests
{
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_GetConnection_NotOpenAtStart(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        var con = db.Server.GetConnection();

        //GetConnection should return an unopened connection
        Assert.That(con.State, Is.EqualTo(ConnectionState.Closed));
    }

    /// <summary>
    /// Tests that a managed connection is automatically opened when created.
    /// Thread-local pooling is currently disabled (all database types use ADO.NET pooling),
    /// so connections close on disposal and return to ADO.NET's pool.
    /// </summary>
    /// <param name="dbType"></param>
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_GetManagedConnection_AutoOpenClose(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        IManagedConnection con;
        using (con = db.Server.GetManagedConnection())
        {
            //GetManagedConnection should open itself
            Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Open));
        }

        // Thread-local pooling is disabled - all database types use ADO.NET pooling
        // Connection closes on dispose and returns to ADO.NET's pool
        Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Closed));
    }


    /// <summary>
    /// Tests that a managed connection is automatically opened and closed in dispose when starting
    /// a new transaction
    /// </summary>
    /// <param name="dbType"></param>
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_BeginNewTransactedConnection_AutoOpenClose(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        IManagedConnection con;
        using (con = db.Server.BeginNewTransactedConnection())
        {
            //GetManagedConnection should open itself
            Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Open));
        }

        //finally should close it
        Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Closed));
    }

    /// <summary>
    /// Tests that when passed an ongoing managed transaction the GetManagedConnection method
    /// reuses the exist <see cref="IDbConnection"/> and <see cref="IDbTransaction"/> and does
    /// not open or close them.
    ///
    /// <para>This is a design by the API to let us have using statements that either don't have a <see cref="IManagedTransaction"/> and handle
    /// opening and closing their own connections or do have a <see cref="IManagedTransaction"/> and ignore open/dispose step</para>
    /// </summary>
    /// <param name="dbType"></param>
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_GetManagedConnection_OngoingTransaction(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        IManagedConnection ongoingCon;
        //pretend that there is an ongoing transaction already
        using (ongoingCon = db.Server.BeginNewTransactedConnection())
        {
            var ongoingTrans = ongoingCon.ManagedTransaction;

            Assert.Multiple(() =>
            {
                //BeginNewTransactedConnection should open itself
                Assert.That(ongoingCon.Connection.State, Is.EqualTo(ConnectionState.Open));
                Assert.That(ongoingTrans, Is.Not.Null);
            });

            //a managed connection with an ongoing transaction
            IManagedConnection con;
            using (con = db.Server.GetManagedConnection(ongoingTrans))
            {
                Assert.Multiple(() =>
                {
                    //still open
                    Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Open));
                    Assert.That(con.Connection, Is.EqualTo(ongoingCon.Connection)); //same underlying connection
                });

            }
            //it should still be open after this finally block
            Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Open));
        }

        //this is the using on the transaction this one should now close itself
        Assert.That(ongoingCon.Connection.State, Is.EqualTo(ConnectionState.Closed));
    }


    /// <summary>
    /// Same as Test_GetManagedConnection_OngoingTransaction except we call <see cref="IManagedTransaction.CommitAndCloseConnection"/> or
    /// <see cref="IManagedTransaction.AbandonAndCloseConnection"/> instead of relying on the outermost using finally
    /// </summary>
    /// <param name="dbType"></param>
    /// <param name="commit">Whether to commit</param>
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypesWithBoolFlags))]
    public void Test_GetManagedConnection_OngoingTransaction_WithCommitRollback(DatabaseType dbType, bool commit)
    {
        var db = GetTestDatabase(dbType);

        IManagedConnection ongoingCon;
        //pretend that there is an ongoing transaction already
        using (ongoingCon = db.Server.BeginNewTransactedConnection())
        {
            var ongoingTrans = ongoingCon.ManagedTransaction;

            Assert.Multiple(() =>
            {
                //BeginNewTransactedConnection should open itself
                Assert.That(ongoingCon.Connection.State, Is.EqualTo(ConnectionState.Open));
                Assert.That(ongoingTrans, Is.Not.Null);
            });

            //a managed connection with an ongoing transaction
            IManagedConnection con;
            using (con = db.Server.GetManagedConnection(ongoingTrans))
            {
                Assert.Multiple(() =>
                {
                    //still open
                    Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Open));
                    Assert.That(con.Connection, Is.EqualTo(ongoingCon.Connection)); //same underlying connection
                });

            }
            //it should still be open after this finally block
            Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Open));

            if (commit)
                ongoingCon.ManagedTransaction?.CommitAndCloseConnection();
            else
                ongoingCon.ManagedTransaction?.AbandonAndCloseConnection();

            //that should really have closed it!
            Assert.That(ongoingCon.Connection.State, Is.EqualTo(ConnectionState.Closed));
        }

        //this is the using on the transaction this one should now close itself
        Assert.That(ongoingCon.Connection.State, Is.EqualTo(ConnectionState.Closed));
    }


    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_ManagedTransaction_MultipleCancel(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        IManagedConnection ongoingCon;
        //pretend that there is an ongoing transaction already
        using (ongoingCon = db.Server.BeginNewTransactedConnection())
        {
            ongoingCon.ManagedTransaction?.AbandonAndCloseConnection();
            ongoingCon.ManagedTransaction?.AbandonAndCloseConnection();
            ongoingCon.ManagedTransaction?.AbandonAndCloseConnection();
            ongoingCon.ManagedTransaction?.CommitAndCloseConnection();
        }
    }

    /// <summary>
    /// Tests that a managed connection is automatically opened and closed in dispose when starting
    /// a new transaction
    /// </summary>
    /// <param name="dbType"></param>
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_Clone_AutoOpenClose(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        IManagedConnection con;
        using (con = db.Server.BeginNewTransactedConnection())
        {
            //GetManagedConnection should open itself
            Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Open));

            using (var clone = con.Clone())
            {
                clone.CloseOnDispose = false;
                Assert.Multiple(() =>
                {
                    //GetManagedConnection should open itself
                    Assert.That(clone.Connection.State, Is.EqualTo(ConnectionState.Open));

                    Assert.That(clone.Connection, Is.EqualTo(con.Connection));
                });
            }

            //GetManagedConnection should not have closed because we told the clone not to
            Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Open));

        } //now disposing the non clone

        //finally should close it
        Assert.That(con.Connection.State, Is.EqualTo(ConnectionState.Closed));
    }

    /// <summary>
    /// Tests that connections with dangling transactions (@@TRANCOUNT > 0) are properly detected
    /// and rejected from the thread-local pool. Reproduces issue where HasDanglingTransaction
    /// catch block incorrectly returned false when the validation query itself failed due to
    /// pending transaction.
    /// </summary>
    #if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer)]
    #endif
    public void Test_DanglingTransaction_IsDetectedAndRejected(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        // Clear pool to start fresh
        FAnsi.Discovery.DiscoveredServer.ClearCurrentThreadConnectionPool();

        IManagedConnection firstCon;
        using (firstCon = db.Server.GetManagedConnection())
        {
            Assert.That(firstCon.Connection.State, Is.EqualTo(ConnectionState.Open));

            // Simulate RDMP scenario: Start a transaction using raw SQL (not IManagedTransaction)
            // This leaves @@TRANCOUNT > 0 but ManagedTransaction is null
            using var cmd = firstCon.Connection.CreateCommand();
            cmd.CommandText = "BEGIN TRANSACTION";
            cmd.ExecuteNonQuery();

            // Verify transaction was started
            using var checkCmd = firstCon.Connection.CreateCommand();
            checkCmd.CommandText = "SELECT @@TRANCOUNT";
            var tranCount = (int)checkCmd.ExecuteScalar()!;
            Assert.That(tranCount, Is.GreaterThan(0), "Transaction should have been started");
        }
        // Connection disposed but still in pool with dangling transaction

        // Try to get a connection again - should get a FRESH one, not the dirty pooled one
        IManagedConnection secondCon;
        using (secondCon = db.Server.GetManagedConnection())
        {
            Assert.That(secondCon.Connection.State, Is.EqualTo(ConnectionState.Open));

            // This should NOT fail - we should have gotten a clean connection
            using var cmd = secondCon.Connection.CreateCommand();
            cmd.CommandText = "SELECT 1";
            var result = cmd.ExecuteScalar();
            Assert.That(result, Is.EqualTo(1), "Should be able to execute commands on clean connection");

            // Verify no dangling transaction on new connection
            using var checkCmd = secondCon.Connection.CreateCommand();
            checkCmd.CommandText = "SELECT @@TRANCOUNT";
            var tranCount = (int)checkCmd.ExecuteScalar()!;
            Assert.That(tranCount, Is.EqualTo(0), "New connection should not have dangling transaction");
        }

        // Cleanup
        FAnsi.Discovery.DiscoveredServer.ClearCurrentThreadConnectionPool();
    }
}
