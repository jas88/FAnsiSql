using System;
using System.Data;
using System.IO;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementations.Sqlite;
using Microsoft.Data.Sqlite;

namespace FAnsiTests;

/// <summary>
/// Simple test to verify SQLite implementation works
/// </summary>
[TestFixture]
public class SimpleSQLiteTests
{
    private string? _testDbPath;
    private DiscoveredServer? _server;
    private DiscoveredDatabase? _database;

    [SetUp]
    public void Setup()
    {
        // Ensure SQLite implementation is loaded
        SqliteImplementation.EnsureLoaded();

        // Create test database path
        _testDbPath = Path.Combine(Path.GetTempPath(), $"SimpleSQLiteTest_{Guid.NewGuid()}.db");

        // Clean up any existing file
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);

        // Create server connection
        var connectionString = $"Data Source={_testDbPath};Pooling=True;Cache=Private";
        _server = new DiscoveredServer(connectionString, DatabaseType.Sqlite);

        // Create database
        _database = _server.ExpectDatabase(_testDbPath);
        _database.Create();
    }

    [TearDown]
    public void TearDown()
    {
        _database = null;
        _server = null;

        if (!string.IsNullOrEmpty(_testDbPath) && File.Exists(_testDbPath))
        {
            try
            {
                File.Delete(_testDbPath);
            }
            catch
            {
                // Ignore cleanup failures
            }
        }
    }

    [Test]
    public void Test_BasicConnection()
    {
        Assert.That(_server, Is.Not.Null);
        Assert.That(_database, Is.Not.Null);
        Assert.That(_database.Exists(), Is.True);
    }

    [Test]
    public void Test_CreateTable()
    {
        var table = _database!.ExpectTable("TestTable");

        // Create a simple table
        using var con = _database.GetManagedConnection();
        using var cmd = _database.Server.GetCommand(
            "CREATE TABLE TestTable (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Age INTEGER)",
            con.Connection);
        cmd.ExecuteNonQuery();

        // Verify table exists
        Assert.That(table.Exists(), Is.True);

        // Verify columns
        var columns = table.DiscoverColumns();
        var columnNames = new HashSet<string>(Array.ConvertAll(columns.ToArray(), c => c.GetRuntimeName()));

        Assert.That(columnNames, Contains("Id"));
        Assert.That(columnNames, Contains("Name"));
        Assert.That(columnNames, Contains("Age"));
    }

    [Test]
    public void Test_InsertAndSelect()
    {
        // Create table
        var table = _database!.ExpectTable("People");
        using var con = _database.GetManagedConnection();
        using var cmd = _database.Server.GetCommand(
            "CREATE TABLE People (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, BirthDate DATETIME)",
            con.Connection);
        cmd.ExecuteNonQuery();

        // Insert data
        using var insertCmd = _database.Server.GetCommand(
            "INSERT INTO People (Name, BirthDate) VALUES (@name, @birthdate)", con.Connection);
        insertCmd.Parameters.Add(new SqliteParameter("@name", "John Doe"));
        insertCmd.Parameters.Add(new SqliteParameter("@birthdate", DateTime.Parse("1990-01-01")));
        insertCmd.ExecuteNonQuery();

        // Query data
        using var selectCmd = _database.Server.GetCommand("SELECT * FROM People", con.Connection);
        using var reader = selectCmd.ExecuteReader();

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetString(1), Is.EqualTo("John Doe"));
        Assert.That(reader.GetDateTime(2), Is.EqualTo(DateTime.Parse("1990-01-01")));
    }

    [Test]
    public void Test_TopXSyntax()
    {
        // Create table with sample data
        var table = _database!.ExpectTable("Numbers");
        using var con = _database.GetManagedConnection();
        using var createCmd = _database.Server.GetCommand("CREATE TABLE Numbers (Value INTEGER)", con.Connection);
        createCmd.ExecuteNonQuery();

        // Insert 10 values
        for (int i = 1; i <= 10; i++)
        {
            using var insertCmd = _database.Server.GetCommand("INSERT INTO Numbers (Value) VALUES (@value)", con.Connection);
            insertCmd.Parameters.Add(new SqliteParameter("@value", i));
            insertCmd.ExecuteNonQuery();
        }

        // Test LIMIT syntax (SQLite's version of TOP)
        var syntax = _database.Server.GetQuerySyntaxHelper();
        var topX = syntax.HowDoWeAchieveTopX(5);

        Assert.That(topX.Type, Is.EqualTo(QueryComponent.Postfix));
        Assert.That(topX.SQL, Is.EqualTo("LIMIT 5"));

        // Execute the query
        using var selectCmd = _database.Server.GetCommand($"SELECT * FROM Numbers {topX.SQL}", con.Connection);
        using var reader = selectCmd.ExecuteReader();

        var count = 0;
        while (reader.Read())
            count++;

        Assert.That(count, Is.EqualTo(5));
    }

    [Test]
    public void Test_QuerySyntaxHelper()
    {
        var syntax = _database!.Server.GetQuerySyntaxHelper();

        // Test identifier quoting
        Assert.That(syntax.OpenQualifier, Is.EqualTo("\""));
        Assert.That(syntax.CloseQualifier, Is.EqualTo("\""));

        // Test scalar functions
        var todayFunc = syntax.GetScalarFunctionSql(MandatoryScalarFunctions.GetTodaysDate);
        Assert.That(todayFunc, Is.EqualTo("datetime('now')"));

        // Test escaping
        var escaped = syntax.Escape("O'Reilly");
        Assert.That(escaped, Is.EqualTo("O''Reilly"));
    }

    [Test]
    public void Test_BulkInsert()
    {
        // Create table
        var table = _database!.ExpectTable("BulkTest");
        using var con = _database.GetManagedConnection();
        using var createCmd = _database.Server.GetCommand(
            "CREATE TABLE BulkTest (Id INTEGER PRIMARY KEY, Name TEXT, Value REAL)", con.Connection);
        createCmd.ExecuteNonQuery();

        // Create DataTable with test data
        var dt = new DataTable();
        dt.Columns.Add("Name", typeof(string));
        dt.Columns.Add("Value", typeof(double));

        // Add test rows
        dt.Rows.Add("Row1", 1.23);
        dt.Rows.Add("Row2", 4.56);
        dt.Rows.Add("Row3", 7.89);

        // Perform bulk insert
        using var bulkInsert = table.BeginBulkInsert(con, System.Globalization.CultureInfo.InvariantCulture);
        var inserted = bulkInsert.Upload(dt);

        Assert.That(inserted, Is.EqualTo(3));

        // Verify data was inserted
        using var selectCmd = _database.Server.GetCommand("SELECT COUNT(*) FROM BulkTest", con.Connection);
        var count = Convert.ToInt32(selectCmd.ExecuteScalar());
        Assert.That(count, Is.EqualTo(3));
    }
}
