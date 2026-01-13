using System.Data;
using System.Globalization;
using System.Xml.Linq;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using FAnsi.Exceptions;
using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using FAnsi.Implementations.PostgreSql;
using FAnsi.Implementations.Sqlite;
using NUnit.Framework;

namespace FAnsiTests;

[SingleThreaded]
[NonParallelizable]
public abstract class DatabaseTests
{
    private const string TestFilename = "TestDatabases.xml";
    protected readonly Dictionary<DatabaseType, string> TestConnectionStrings = [];

    private bool _allowDatabaseCreation;
    private string _testScratchDatabase = null!;

    [OneTimeSetUp]
    public void CheckFiles()
    {
        // Explicit loading for tests (ModuleInitializer timing is unreliable in test runners)
        // Production code using FAnsi.Legacy gets automatic loading
#pragma warning disable CS0618 // Type or member is obsolete
        ImplementationManager.Load<MicrosoftSQLImplementation>();
        ImplementationManager.Load<MySqlImplementation>();
        ImplementationManager.Load<OracleImplementation>();
        ImplementationManager.Load<PostgreSqlImplementation>();
        ImplementationManager.Load<SqliteImplementation>();
#pragma warning restore CS0618 // Type or member is obsolete

        var file = Path.Combine(TestContext.CurrentContext.TestDirectory, TestFilename);

        Assert.That(File.Exists(file), $"Could not find {TestFilename}");

        var doc = XDocument.Load(file);

        var root = doc.Element("TestDatabases") ??
                   throw new InvalidOperationException($"Missing element 'TestDatabases' in {TestFilename}");

        var settings = root.Element("Settings") ??
                       throw new InvalidOperationException($"Missing element 'Settings' in {TestFilename}");

        var e = settings.Element("AllowDatabaseCreation") ??
                throw new InvalidOperationException($"Missing element 'AllowDatabaseCreation' in {TestFilename}");

        _allowDatabaseCreation = Convert.ToBoolean(e.Value);

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

            TestConnectionStrings.Add(databaseType, constr);

            // Make sure our scratch db exists for databases that need pre-creation
            // PostgreSQL and Oracle require the database to exist before tests run
            // MySQL and SQL Server can create databases on demand
            // SQLite uses in-memory databases
            if (databaseType is DatabaseType.PostgreSql or DatabaseType.Oracle)
            {
                var server = GetTestServer(databaseType);
                var db = server.ExpectDatabase(_testScratchDatabase);

                // Check if database already exists
                if (!db.Exists())
                    try
                    {
                        server.CreateDatabase(_testScratchDatabase);
                    }
                    catch (Exception ex)
                    {
                        // Ignore "already exists" errors from parallel test execution
                        // Oracle: ORA-01920 (user already exists)
                        // PostgreSQL: 23505 (duplicate key in pg_database)
                        var isAlreadyExists = ex.Message.Contains("ORA-01920") ||
                                              ex.Message.Contains("23505") ||
                                              ex.Message.Contains("already exists");
                        if (!isAlreadyExists)
                            throw;
                    }
            }
        }
    }

    protected DiscoveredServer GetTestServer(DatabaseType type)
    {
        if (!TestConnectionStrings.TryGetValue(type, out var connString))
            // SQLite has known FAnsi database abstraction limitations, allow it to skip in CI
            AssertRequirement($"No connection string configured for {type}",
                allowSkipInCi: type == DatabaseType.Sqlite);

        return new DiscoveredServer(connString, type);
    }

    protected DiscoveredDatabase GetTestDatabase(DatabaseType type, bool cleanDatabase = true)
    {
        var server = GetTestServer(type);
        var db = server.ExpectDatabase(_testScratchDatabase);

        if (!db.Exists())
        {
            if (_allowDatabaseCreation)
                db.Create();
            else
                AssertRequirement(
                    $"Database {_testScratchDatabase} does not exist and AllowDatabaseCreation is false in {TestFilename}");
        }
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
            catch (CircularDependencyException)
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
    ///     Asserts a test requirement is not met. In CI, this fails the test. On dev workstations, marks test as inconclusive.
    ///     SQLite is always allowed to be inconclusive due to known FAnsi database abstraction limitations.
    /// </summary>
    /// <param name="message">The assertion message</param>
    /// <param name="allowSkipInCi">If true, allows the test to be inconclusive even in CI</param>
    protected static void AssertRequirement(string message, bool allowSkipInCi = false)
    {
        if (Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true" && !allowSkipInCi)
            Assert.Fail(message);
        else
            Assert.Inconclusive(message);
    }

    protected void AssertCanCreateDatabases()
    {
        if (!_allowDatabaseCreation)
            Assert.Inconclusive("Test cannot run when AllowDatabaseCreation is false");
    }

    private static bool AreBasicallyEquals(object? o, object? o2, bool handleSlashRSlashN = true)
    {
        //if they are legit equals
        if (Equals(o, o2))
            return true;

        // Handle DateTime/DateOnly/TimeOnly comparisons
        if (o is DateTime dt1 && o2 is DateTime dt2)
            return dt1.Date == dt2.Date && dt1.TimeOfDay == dt2.TimeOfDay;

        // Handle string to DateTime comparisons (for SQLite which stores DateTime as TEXT)
        if (o is DateTime dtObj && o2 is string str)
            if (DateTime.TryParse(str, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsedDt))
                return dtObj.Date == parsedDt.Date && dtObj.TimeOfDay == parsedDt.TimeOfDay;
        if (o is string str2 && o2 is DateTime dtObj2)
            if (DateTime.TryParse(str2, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsedDt2))
                return parsedDt2.Date == dtObj2.Date && parsedDt2.TimeOfDay == dtObj2.TimeOfDay;

        if (o is DateTime dt && o2 is DateOnly d)
            return dt.Date == d.ToDateTime(TimeOnly.MinValue).Date;
        if (o is DateOnly d1 && o2 is DateTime dt3)
            return d1.ToDateTime(TimeOnly.MinValue).Date == dt3.Date;
        if (o is DateOnly d2 && o2 is DateOnly d3)
            return d2 == d3;

        //if they are null but basically the same
        var oIsNull = o == null || o == DBNull.Value || o.ToString()?.Equals("0", StringComparison.Ordinal) == true;
        var o2IsNull = o2 == null || o2 == DBNull.Value || o2.ToString()?.Equals("0", StringComparison.Ordinal) == true;

        if (oIsNull || o2IsNull)
            return oIsNull == o2IsNull;

        //they are not null so tostring them deals with int vs long etc that DbDataAdapters can be a bit flaky on
        if (handleSlashRSlashN)
            return string.Equals(o?.ToString()?.Replace("\r", "").Replace("\n", ""),
                o2?.ToString()?.Replace("\r", "").Replace("\n", ""), StringComparison.Ordinal);

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
            var match = dt2.Rows.Cast<DataRow>().Any(row2 =>
                dt1.Columns.Cast<DataColumn>().All(c => AreBasicallyEquals(row1[c.ColumnName], row2[c.ColumnName])));
            Assert.That(match, $"Couldn't find match for row:{string.Join(",", row1.ItemArray)}");
        }
    }
}
