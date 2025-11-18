using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Xml.Linq;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using FAnsi.Implementation;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.PostgreSql;
using FAnsi.Implementations.Sqlite;
using NUnit.Framework;

namespace FAnsiTests;

[SingleThreaded]
[NonParallelizable]
public abstract class DatabaseTests
{
    protected readonly Dictionary<DatabaseType, string> TestConnectionStrings = [];

    private bool _allowDatabaseCreation;
    private string _testScratchDatabase = null!;

    private const string TestFilename = "TestDatabases.xml";

    [OneTimeSetUp]
    public void CheckFiles()
    {
#pragma warning disable CS0618 // Type or member is obsolete
        ImplementationManager.Load<OracleImplementation>();
        ImplementationManager.Load<MicrosoftSQLImplementation>();
        ImplementationManager.Load<MySqlImplementation>();
        ImplementationManager.Load<PostgreSqlImplementation>();
        ImplementationManager.Load<SqliteImplementation>();
#pragma warning restore CS0618

        var file = Path.Combine(TestContext.CurrentContext.TestDirectory, TestFilename);

        if (!File.Exists(file))
            Assert.Ignore($"Could not find {TestFilename} - database not configured for testing");

        var doc = XDocument.Load(file);

        var root = doc.Element("TestDatabases") ?? throw new InvalidOperationException($"Missing element 'TestDatabases' in {TestFilename}");

        var settings = root.Element("Settings") ??
                       throw new InvalidOperationException($"Missing element 'Settings' in {TestFilename}");

        var e = settings.Element("AllowDatabaseCreation") ??
                throw new InvalidOperationException($"Missing element 'AllowDatabaseCreation' in {TestFilename}");

        _allowDatabaseCreation = Convert.ToBoolean(e.Value, CultureInfo.InvariantCulture);

        e = settings.Element("TestScratchDatabase") ??
            throw new InvalidOperationException($"Missing element 'TestScratchDatabase' in {TestFilename}");

        _testScratchDatabase = e.Value;

        foreach (var element in root.Elements("TestDatabase"))
        {
            var type = element.Element("DatabaseType")?.Value;

            if (!Enum.TryParse(type, out DatabaseType databaseType))
                throw new InvalidOperationException($"Could not parse DatabaseType {type}");

            var constr = element.Element("ConnectionString")?.Value ??
                         throw new InvalidOperationException($"Invalid connection string for {type}");

            // Test database connectivity - if it fails, skip this database type
            try
            {
                var server = new DiscoveredServer(constr, databaseType);
                using var testCon = server.GetConnection();
                testCon.Open();
                testCon.Close();

                TestConnectionStrings.Add(databaseType, constr);

                // Make sure our scratch db exists for databases that need pre-creation
                // PostgreSQL and Oracle require the database to exist before tests run
                // MySQL and SQL Server can create databases on demand
                // SQLite uses in-memory databases
                if (databaseType is DatabaseType.PostgreSql or DatabaseType.Oracle)
                {
                    if (server.DiscoverDatabases().All(db => db.GetWrappedName()?.Contains(_testScratchDatabase) != true))
                        server.CreateDatabase(_testScratchDatabase);
                }
            }
            catch (Exception)
            {
                // Database not available - don't add to TestConnectionStrings
                // Tests will handle this via conditional compilation or AssertRequirement
            }
        }
    }

    [SetUp]
    public void LogTestStart()
    {
        // Only in CI
        if (Environment.GetEnvironmentVariable("GITHUB_ACTIONS") != "true")
            return;

        var testName = TestContext.CurrentContext.Test.Name;
        TestContext.Out.WriteLine($"▶▶▶ STARTING TEST: {testName} at {DateTime.UtcNow:HH:mm:ss.fff}");
    }

    [TearDown]
    public void VerifyDatabaseHealthAfterTest()
    {
        // Only check in CI where we control the database state
        if (Environment.GetEnvironmentVariable("GITHUB_ACTIONS") != "true")
            return;

        foreach (var (type, connString) in TestConnectionStrings)
        {
            try
            {
                var server = new DiscoveredServer(connString, type);
                using var con = server.GetConnection();
                con.Open();

                // Try a simple query to detect deadlocks/hangs
                // SQL Server: Check for dangling transactions with @@TRANCOUNT (connection pooling can return connections with uncommitted transactions)
                // Oracle requires FROM DUAL for SELECT without a table
                var healthCheckSql = type switch
                {
                    DatabaseType.MicrosoftSQLServer => "SELECT @@TRANCOUNT",
                    DatabaseType.Oracle => "SELECT 1 FROM DUAL",
                    _ => "SELECT 1"
                };
                using var cmd = server.GetCommand(healthCheckSql, con);
                cmd.CommandTimeout = 5; // Set timeout on command, not connection string (Oracle doesn't support it in connection string)
                var result = cmd.ExecuteScalar();

                // Convert result to long for type-agnostic comparison (handles int, long, decimal, etc.)
                var expectedValue = type == DatabaseType.MicrosoftSQLServer ? 0L : 1L; // @@TRANCOUNT should be 0, SELECT 1 should be 1
                if (result == null || Convert.ToInt64(result, CultureInfo.InvariantCulture) != expectedValue)
                {
                    var detail = type == DatabaseType.MicrosoftSQLServer && Convert.ToInt64(result, CultureInfo.InvariantCulture) > 0
                        ? $"Dangling transaction detected: @@TRANCOUNT = {result}"
                        : "health check returned unexpected result";

                    // CRITICAL: Rollback dangling transaction BEFORE closing connection
                    // Otherwise connection returns to pool with active transaction
                    if (type == DatabaseType.MicrosoftSQLServer && Convert.ToInt64(result, CultureInfo.InvariantCulture) > 0)
                    {
                        using var rollbackCmd = server.GetCommand("ROLLBACK TRANSACTION", con);
                        rollbackCmd.ExecuteNonQuery();
                    }

                    con.Close();
                    Assert.Fail($"CURRENT TEST corrupted {type} database state - {detail}.");
                }

                con.Close();
            }
            catch (Exception ex)
            {
                Assert.Fail($"CURRENT TEST left {type} database in broken state. Likely leaked transaction or held locks. Error: {ex.Message}");
            }
        }
    }

    protected DiscoveredServer GetTestServer(DatabaseType type)
    {
        // In RDBMS-specific test projects, ignore tests for other database types
#if ORACLE_TESTS
        if (type != DatabaseType.Oracle)
            Assert.Ignore($"Skipping {type} test in Oracle test project");
#elif POSTGRESQL_TESTS
        if (type != DatabaseType.PostgreSql)
            Assert.Ignore($"Skipping {type} test in PostgreSQL test project");
#elif MYSQL_TESTS
        if (type != DatabaseType.MySql)
            Assert.Ignore($"Skipping {type} test in MySQL test project");
#elif MSSQL_TESTS
        if (type != DatabaseType.MicrosoftSQLServer)
            Assert.Ignore($"Skipping {type} test in SQL Server test project");
#elif SQLITE_TESTS
        if (type != DatabaseType.Sqlite)
            Assert.Ignore($"Skipping {type} test in SQLite test project");
#endif

        if (!TestConnectionStrings.TryGetValue(type, out var connString))
            AssertRequirement($"No connection string configured for {type}");

        // Increase command timeout for CI environments where queries can be slower
        // SQL Server default is 30 seconds, increase to 120 seconds
        if (type == DatabaseType.MicrosoftSQLServer && connString != null && !connString.Contains("Command Timeout", StringComparison.OrdinalIgnoreCase))
        {
            connString += ";Command Timeout=120";
        }

        return new DiscoveredServer(connString, type);
    }

    protected DiscoveredDatabase GetTestDatabase(DatabaseType type, bool cleanDatabase = true)
    {
        var server = GetTestServer(type);
        var db = server.ExpectDatabase(_testScratchDatabase);

        if (!db.Exists())
            if (_allowDatabaseCreation)
                db.Create();
            else
                AssertRequirement($"Database {_testScratchDatabase} does not exist and AllowDatabaseCreation is false");
        else
        {
            if (!cleanDatabase) return db;

            IEnumerable<DiscoveredTable> deleteTableOrder;

            try
            {
                //delete in reverse dependency order to avoid foreign key constraint issues preventing deleting
                var tree = new RelationshipTopologicalSort(db.DiscoverTables(true));
                deleteTableOrder = tree.Order.Reverse();
            }
            catch (FAnsi.Exceptions.CircularDependencyException)
            {
                deleteTableOrder = db.DiscoverTables(true);
            }

            foreach (var t in deleteTableOrder)
                t.Drop();

            foreach (var func in db.DiscoverTableValuedFunctions())
                func.Drop();
        }

        return db;
    }

    /// <summary>
    /// Asserts a test requirement is not met. In CI, this fails the test. On dev workstations, marks test as ignored.
    /// </summary>
    /// <param name="message">The assertion message</param>
    protected static void AssertRequirement(string message)
    {
        if (Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true")
            Assert.Fail(message);
        else
            Assert.Ignore(message);
    }

    protected void AssertCanCreateDatabases()
    {
        if (!_allowDatabaseCreation)
            Assert.Ignore("AllowDatabaseCreation is false - skipping database creation tests");
    }

    private static bool AreBasicallyEquals(object? o, object? o2, bool handleSlashRSlashN = true)
    {
        //if they are legit equals
        if (Equals(o, o2))
            return true;

        //if they are null but basically the same
        var oIsNull = o == null || o == DBNull.Value || o.ToString()?.Equals("0", StringComparison.Ordinal) == true;
        var o2IsNull = o2 == null || o2 == DBNull.Value || o2.ToString()?.Equals("0", StringComparison.Ordinal) == true;

        if (oIsNull || o2IsNull)
            return oIsNull == o2IsNull;

        //they are not null so tostring them deals with int vs long etc that DbDataAdapters can be a bit flaky on
        if (handleSlashRSlashN)
            return string.Equals(o?.ToString()?.Replace("\r", "", StringComparison.Ordinal).Replace("\n", "", StringComparison.Ordinal), o2?.ToString()?.Replace("\r", "", StringComparison.Ordinal).Replace("\n", "", StringComparison.Ordinal), StringComparison.Ordinal);

        return string.Equals(o?.ToString(), o2?.ToString(), StringComparison.Ordinal);
    }

    protected static void AssertAreEqual(DataTable dt1, DataTable dt2)
    {
        Assert.Multiple(() =>
        {
            Assert.That(dt2.Columns, Has.Count.EqualTo(dt1.Columns.Count), "DataTables had a column count mismatch");
            Assert.That(dt2.Rows, Has.Count.EqualTo(dt1.Rows.Count), "DataTables had a row count mismatch");
        });

        foreach (DataRow row1 in dt1.Rows)
        {
            var match = dt2.Rows.Cast<DataRow>().Any(row2 => dt1.Columns.Cast<DataColumn>().All(c => AreBasicallyEquals(row1[c.ColumnName], row2[c.ColumnName])));
            Assert.That(match, $"Couldn't find match for row:{string.Join(",", row1.ItemArray)}");
        }

    }
}
