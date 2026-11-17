using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using FAnsi;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using FAnsi.Implementations.PostgreSql;
using FAnsi.Implementations.Sqlite;
using FAnsi.Naming;
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
    public void TestConnection_OpenNeverCompletes_ThrowsTimeoutException()
    {
        // The connection never opens and ignores the token, so only the timeout of the wait can end it. With a
        // timeout of 0, Task.Wait returns false straight away for a task that has not completed
        ImplementationManager.Register(new NeverOpeningImplementation());
        var builder = new NeverOpeningConnectionStringBuilder { ConnectionString = "Data Source=never-opens" };
        var server = new DiscoveredServer(builder);

        var ex = Assert.Throws<TimeoutException>(() => server.TestConnection(0));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("'never-opens'"));
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

    /// <summary>
    ///     Outside the range of <see cref="DatabaseType" />, so registering <see cref="NeverOpeningImplementation" />
    ///     cannot replace the implementation of a real DBMS
    /// </summary>
    private const DatabaseType NoSuchDatabaseType = (DatabaseType)(-1);

    /// <summary>
    ///     An implementation whose connections never open, found by its own connection string builder type
    /// </summary>
    private sealed class NeverOpeningImplementation()
        : Implementation<NeverOpeningConnectionStringBuilder>(NoSuchDatabaseType, typeof(NeverOpeningConnection))
    {
        public override IDiscoveredServerHelper GetServerHelper() => NeverOpeningServerHelper.Instance;
        public override bool IsFor(DbConnection connection) => connection is NeverOpeningConnection;
        public override IQuerySyntaxHelper GetQuerySyntaxHelper() => throw new NotSupportedException();
    }

    private sealed class NeverOpeningConnectionStringBuilder : DbConnectionStringBuilder;

    /// <summary>
    ///     A connection whose <see cref="OpenAsync" /> never completes and ignores the cancellation token
    /// </summary>
    private sealed class NeverOpeningConnection : DbConnection
    {
        [AllowNull] public override string ConnectionString { get; set; } = "";
        public override string Database => "";
        public override string DataSource => "never-opens";
        public override string ServerVersion => throw new NotSupportedException();
        public override ConnectionState State => ConnectionState.Closed;

        public override Task OpenAsync(CancellationToken cancellationToken) => new TaskCompletionSource().Task;
        public override void Open() => throw new NotSupportedException();

        public override void Close()
        {
            // Never opened, so there is nothing to close
        }

        public override void ChangeDatabase(string databaseName) => throw new NotSupportedException();

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) =>
            throw new NotSupportedException();

        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }

    /// <summary>
    ///     Builds connection strings for, and connections to, a server that never opens. TestConnection uses nothing else
    /// </summary>
    private sealed class NeverOpeningServerHelper() : DiscoveredServerHelper(NoSuchDatabaseType)
    {
        public static readonly NeverOpeningServerHelper Instance = new();

        protected override string ServerKeyName => "Data Source";
        protected override string DatabaseKeyName => "Initial Catalog";

        public override DbConnection GetConnection(DbConnectionStringBuilder builder) => new NeverOpeningConnection();

        protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string? connectionString) =>
            new NeverOpeningConnectionStringBuilder { ConnectionString = connectionString };

        protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string connectionString,
            string? database, string username, string password) => throw new NotSupportedException();

        public override DbCommand GetCommand(string s, DbConnection con, DbTransaction? transaction = null) =>
            throw new NotSupportedException();

        public override DbDataAdapter GetDataAdapter(DbCommand cmd) => throw new NotSupportedException();
        public override DbCommandBuilder GetCommandBuilder(DbCommand cmd) => throw new NotSupportedException();
        public override DbParameter GetParameter(string parameterName) => throw new NotSupportedException();

        public override IEnumerable<string> ListDatabases(DbConnectionStringBuilder builder) =>
            throw new NotSupportedException();

        public override IEnumerable<string> ListDatabases(DbConnection con) => throw new NotSupportedException();

        public override DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder) =>
            throw new NotSupportedException();

        public override IDiscoveredDatabaseHelper GetDatabaseHelper() => throw new NotSupportedException();
        public override IQuerySyntaxHelper GetQuerySyntaxHelper() => throw new NotSupportedException();

        public override void CreateDatabase(DbConnectionStringBuilder builder, IHasRuntimeName newDatabaseName) =>
            throw new NotSupportedException();

        public override Dictionary<string, string> DescribeServer(DbConnectionStringBuilder builder) =>
            throw new NotSupportedException();

        public override string GetExplicitUsernameIfAny(DbConnectionStringBuilder builder) =>
            throw new NotSupportedException();

        public override string GetExplicitPasswordIfAny(DbConnectionStringBuilder builder) =>
            throw new NotSupportedException();

        public override Version GetVersion(DiscoveredServer server) => throw new NotSupportedException();
        public override bool DatabaseExists(DiscoveredDatabase database) => throw new NotSupportedException();
    }
}
