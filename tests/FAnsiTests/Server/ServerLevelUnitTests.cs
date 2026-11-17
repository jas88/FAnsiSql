using System.Data.Common;
using System.Globalization;
using FAnsi;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using FAnsi.Implementations.PostgreSql;
using FAnsi.Implementations.Sqlite;
using Microsoft.Data.SqlClient;
using NUnit.Framework;

namespace FAnsiTests.Server;

internal sealed class ServerLevelUnitTests
{
    [OneTimeSetUp]
    public void Setup()
    {
        // Explicit loading for tests (ModuleInitializer timing is unreliable in test runners)
#pragma warning disable CS0618 // Type or member is obsolete
        ImplementationManager.Load<MicrosoftSQLImplementation>();
        ImplementationManager.Load<MySqlImplementation>();
        ImplementationManager.Load<OracleImplementation>();
        ImplementationManager.Load<PostgreSqlImplementation>();
        ImplementationManager.Load<SqliteImplementation>();
#pragma warning restore CS0618 // Type or member is obsolete
    }

    [Test]
    public void ConstructionStringBuilderTest()
    {
        var b = new SqlConnectionStringBuilder(
            "Server=localhost;Database=RDMP_Catalogue;User ID=SA;Password=blah;Trust Server Certificate=true;Encrypt=True")
        {
            InitialCatalog = "master"
        };

        Assert.That(b.ConnectionString,
            Is.EqualTo(
                "Data Source=localhost;Initial Catalog=master;User ID=SA;Password=blah;Encrypt=True;Trust Server Certificate=True"));
    }

    [Test]
    public void TestConnection_TimeoutExpires_ThrowsTimeoutExceptionNamingServer()
    {
        // A SQLite file in an existing directory can be opened, but a timeout of 0 gives it no time to open
        var server = new DiscoveredServer($"Data Source={NewTempDatabasePath()}", DatabaseType.Sqlite);

        var ex = Assert.Throws<TimeoutException>(() => server.TestConnection(0));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain($"'{server.Name}'"));
            Assert.That(ex.Message, Does.Contain("timeout of 0 milliseconds"));
            Assert.That(ex.InnerException, Is.InstanceOf<OperationCanceledException>());
        });
    }

    [Test]
    public void TestConnection_ConnectionFails_ThrowsAggregateExceptionFromDriver()
    {
        var ex = Assert.Throws<AggregateException>(() => UnopenableSqliteServer().TestConnection());

        Assert.That(ex!.InnerException, Is.InstanceOf<DbException>());
    }

    [Test]
    public void Exists_ConnectionFails_ReturnsFalse() =>
        Assert.That(UnopenableSqliteServer().Exists(), Is.False);

    [Test]
    public void Exists_WithTransaction_ReturnsTrueWithoutConnecting()
    {
        // Existence cannot be checked in the middle of a transaction, so Exists trusts it without connecting:
        // the server cannot be opened, and the transaction throws if Exists touches it
        var server = UnopenableSqliteServer();

        Assert.That(server.Exists(new UnusableTransaction()), Is.True);
    }

    [Test]
    public void GetCommand_WithTransaction_EnlistsCommandInTransaction()
    {
        var path = NewTempDatabasePath();
        try
        {
            var server = new DiscoveredServer($"Data Source={path};Pooling=False", DatabaseType.Sqlite);
            using var con = server.BeginNewTransactedConnection();

            var cmd = server.GetCommand("SELECT 1", con.Connection, con.ManagedTransaction);

            Assert.Multiple(() =>
            {
                Assert.That(cmd.Connection, Is.SameAs(con.Connection));
                Assert.That(cmd.Transaction, Is.SameAs(con.Transaction));
                Assert.That(Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture), Is.EqualTo(1));
            });
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static string NewTempDatabasePath() =>
        Path.Combine(Path.GetTempPath(), $"FAnsiTests_{Guid.NewGuid():N}.db");

    /// <summary>
    ///     A SQLite database in a directory that does not exist, so every attempt to open it fails
    /// </summary>
    private static DiscoveredServer UnopenableSqliteServer() =>
        new($"Data Source={Path.Combine(Path.GetTempPath(), $"FAnsiTests_{Guid.NewGuid():N}", "missing.db")}",
            DatabaseType.Sqlite);

    /// <summary>
    ///     A transaction whose members all throw, for checking that they are not used
    /// </summary>
    private sealed class UnusableTransaction : IManagedTransaction
    {
        public DbConnection Connection => throw new NotSupportedException();
        public DbTransaction Transaction => throw new NotSupportedException();
        public void AbandonAndCloseConnection() => throw new NotSupportedException();
        public void CommitAndCloseConnection() => throw new NotSupportedException();
    }
}
