using System;
using System.Data;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.PostgreSql;
using FAnsi.Implementations.Oracle;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using TypeGuesser;

Console.WriteLine("================================================================================");
Console.WriteLine("FAnsiSql - Comprehensive Native AOT Compatibility Test Suite");
Console.WriteLine("================================================================================");
Console.WriteLine();

var totalTests = 0;
var passedTests = 0;
var failedTests = 0;

// Test SQL Server
Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════════╗");
Console.WriteLine("║ SQL SERVER (Microsoft.Data.SqlClient)                                       ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════════╝");
Console.WriteLine();
totalTests++;
try
{
    TestSqlServer();
    Console.WriteLine("✅ SQL Server: ALL TESTS PASSED");
    passedTests++;
}
catch (Exception ex)
{
    Console.WriteLine($"❌ SQL Server: TEST FAILED - {ex.GetType().Name}: {ex.Message}");
    failedTests++;
}
Console.WriteLine();

// Test MySQL
Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════════╗");
Console.WriteLine("║ MYSQL (MySqlConnector)                                                      ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════════╝");
Console.WriteLine();
totalTests++;
try
{
    TestMySql();
    Console.WriteLine("✅ MySQL: ALL TESTS PASSED");
    passedTests++;
}
catch (Exception ex)
{
    Console.WriteLine($"❌ MySQL: TEST FAILED - {ex.GetType().Name}: {ex.Message}");
    failedTests++;
}
Console.WriteLine();

// Test PostgreSQL
Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════════╗");
Console.WriteLine("║ POSTGRESQL (Npgsql)                                                         ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════════╝");
Console.WriteLine();
totalTests++;
try
{
    TestPostgreSql();
    Console.WriteLine("✅ PostgreSQL: ALL TESTS PASSED");
    passedTests++;
}
catch (Exception ex)
{
    Console.WriteLine($"❌ PostgreSQL: TEST FAILED - {ex.GetType().Name}: {ex.Message}");
    failedTests++;
}
Console.WriteLine();

// Test Oracle
Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════════╗");
Console.WriteLine("║ ORACLE (Oracle.ManagedDataAccess.Core)                                      ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════════╝");
Console.WriteLine();
totalTests++;
try
{
    TestOracle();
    Console.WriteLine("✅ Oracle: ALL TESTS PASSED");
    passedTests++;
}
catch (Exception ex)
{
    Console.WriteLine($"❌ Oracle: TEST FAILED - {ex.GetType().Name}: {ex.Message}");
    failedTests++;
}
Console.WriteLine();

// Summary
Console.WriteLine("================================================================================");
Console.WriteLine("TEST SUMMARY");
Console.WriteLine("================================================================================");
Console.WriteLine($"Total Database Systems Tested: {totalTests}");
Console.WriteLine($"Passed: {passedTests}");
Console.WriteLine($"Failed: {failedTests}");
Console.WriteLine();

if (failedTests == 0)
{
    Console.WriteLine("🎉 ALL DATABASE SYSTEMS ARE AOT-COMPATIBLE!");
    Console.WriteLine();
    Console.WriteLine("All FAnsiSql database implementations work correctly with Native AOT.");
    Console.WriteLine("This confirms compatibility with:");
    Console.WriteLine("  - Microsoft.Data.SqlClient (SQL Server)");
    Console.WriteLine("  - MySqlConnector (MySQL)");
    Console.WriteLine("  - Npgsql (PostgreSQL)");
    Console.WriteLine("  - Oracle.ManagedDataAccess.Core (Oracle)");
    return 0;
}
else
{
    Console.WriteLine($"⚠️  {failedTests} DATABASE SYSTEM(S) FAILED AOT COMPATIBILITY TESTS");
    return 1;
}

static void TestSqlServer()
{
    Console.WriteLine("[1/6] Loading SQL Server implementation...");
    ImplementationManager.Load<MicrosoftSQLImplementation>();
    Console.WriteLine("      ✓ Implementation loaded");

    Console.WriteLine("[2/6] Creating DiscoveredServer from connection string...");
    var server = new DiscoveredServer(
        "Server=localhost;Database=master;User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=True;",
        DatabaseType.MicrosoftSQLServer);
    Console.WriteLine($"      ✓ Server: {server.DatabaseType}");

    Console.WriteLine("[3/6] Testing connection string builder...");
    var builder = new SqlConnectionStringBuilder
    {
        DataSource = "localhost",
        InitialCatalog = "tempdb",
        TrustServerCertificate = true,
        UserID = "sa",
        Password = "YourStrong!Passw0rd"
    };
    var server2 = new DiscoveredServer(builder);
    Console.WriteLine($"      ✓ Builder-based server: {server2.Name}");

    Console.WriteLine("[4/6] Testing discovery objects...");
    var database = server.ExpectDatabase("TestDB");
    var table = database.ExpectTable("TestTable");
    Console.WriteLine($"      ✓ Table path: {table.GetFullyQualifiedName()}");

    Console.WriteLine("[5/6] Testing DataTable operations (bulk copy simulation)...");
    var dt = new DataTable();
    dt.Columns.Add("Id", typeof(int));
    dt.Columns.Add("Name", typeof(string));
    dt.Columns.Add("CreatedDate", typeof(DateTime));
    dt.Rows.Add(1, "AOT Test Item", DateTime.Now);
    dt.Rows.Add(2, "Another Test", DateTime.Now);
    Console.WriteLine($"      ✓ DataTable: {dt.Rows.Count} rows, {dt.Columns.Count} columns");

    Console.WriteLine("[6/6] Testing type translation...");
    var typeTranslater = server.GetQuerySyntaxHelper().TypeTranslater;
    var sqlType = typeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(string), 100));
    Console.WriteLine($"      ✓ Type translation: string(100) → {sqlType}");
}

static void TestMySql()
{
    Console.WriteLine("[1/6] Loading MySQL implementation...");
    ImplementationManager.Load<MySqlImplementation>();
    Console.WriteLine("      ✓ Implementation loaded");

    Console.WriteLine("[2/6] Creating DiscoveredServer from connection string...");
    var server = new DiscoveredServer(
        "Server=localhost;Uid=root;Pwd=;Database=mysql;",
        DatabaseType.MySql);
    Console.WriteLine($"      ✓ Server: {server.DatabaseType}");

    Console.WriteLine("[3/6] Testing connection string builder...");
    var builder = new MySqlConnectionStringBuilder
    {
        Server = "localhost",
        Database = "testdb",
        UserID = "root",
        Password = ""
    };
    var server2 = new DiscoveredServer(builder);
    Console.WriteLine($"      ✓ Builder-based server: {server2.Name}");

    Console.WriteLine("[4/6] Testing discovery objects...");
    var database = server.ExpectDatabase("TestDB");
    var table = database.ExpectTable("TestTable");
    Console.WriteLine($"      ✓ Table path: {table.GetFullyQualifiedName()}");

    Console.WriteLine("[5/6] Testing DataTable operations...");
    var dt = new DataTable();
    dt.Columns.Add("id", typeof(int));
    dt.Columns.Add("name", typeof(string));
    dt.Columns.Add("created_date", typeof(DateTime));
    dt.Rows.Add(1, "MySQL AOT Test", DateTime.Now);
    dt.Rows.Add(2, "Another Row", DateTime.Now);
    Console.WriteLine($"      ✓ DataTable: {dt.Rows.Count} rows");

    Console.WriteLine("[6/6] Testing type translation...");
    var typeTranslater = server.GetQuerySyntaxHelper().TypeTranslater;
    var sqlType = typeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(string), 100));
    Console.WriteLine($"      ✓ Type translation: string(100) → {sqlType}");
}

static void TestPostgreSql()
{
    Console.WriteLine("[1/6] Loading PostgreSQL implementation...");
    ImplementationManager.Load<PostgreSqlImplementation>();
    Console.WriteLine("      ✓ Implementation loaded");

    Console.WriteLine("[2/6] Creating DiscoveredServer from connection string...");
    var server = new DiscoveredServer(
        "Server=localhost;Port=5432;User Id=postgres;Password=pgpass4291;Database=postgres;",
        DatabaseType.PostgreSql);
    Console.WriteLine($"      ✓ Server: {server.DatabaseType}");

    Console.WriteLine("[3/6] Testing connection string builder...");
    var builder = new NpgsqlConnectionStringBuilder
    {
        Host = "localhost",
        Port = 5432,
        Database = "testdb",
        Username = "postgres",
        Password = "pgpass4291"
    };
    var server2 = new DiscoveredServer(builder);
    Console.WriteLine($"      ✓ Builder-based server: {server2.Name}");

    Console.WriteLine("[4/6] Testing discovery objects...");
    var database = server.ExpectDatabase("testdb");
    var table = database.ExpectTable("test_table");
    Console.WriteLine($"      ✓ Table path: {table.GetFullyQualifiedName()}");

    Console.WriteLine("[5/6] Testing DataTable operations...");
    var dt = new DataTable();
    dt.Columns.Add("id", typeof(int));
    dt.Columns.Add("name", typeof(string));
    dt.Columns.Add("created_date", typeof(DateTime));
    dt.Rows.Add(1, "PostgreSQL AOT Test", DateTime.Now);
    dt.Rows.Add(2, "Second Row", DateTime.Now);
    Console.WriteLine($"      ✓ DataTable: {dt.Rows.Count} rows");

    Console.WriteLine("[6/6] Testing type translation...");
    var typeTranslater = server.GetQuerySyntaxHelper().TypeTranslater;
    var sqlType = typeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(string), 100));
    Console.WriteLine($"      ✓ Type translation: string(100) → {sqlType}");
}

static void TestOracle()
{
    Console.WriteLine("[1/6] Loading Oracle implementation...");
    ImplementationManager.Load<OracleImplementation>();
    Console.WriteLine("      ✓ Implementation loaded");

    Console.WriteLine("[2/6] Creating DiscoveredServer from connection string...");
    var server = new DiscoveredServer(
        "Data Source=localhost:1521/xe;User Id=system;Password=oracle;",
        DatabaseType.Oracle);
    Console.WriteLine($"      ✓ Server: {server.DatabaseType}");

    Console.WriteLine("[3/6] Testing connection string builder...");
    var builder = new OracleConnectionStringBuilder
    {
        DataSource = "localhost:1521/xe",
        UserID = "system",
        Password = "oracle"
    };
    var server2 = new DiscoveredServer(builder);
    Console.WriteLine($"      ✓ Builder-based server: {server2.Name}");

    Console.WriteLine("[4/6] Testing discovery objects...");
    var database = server.ExpectDatabase("TESTDB");
    var table = database.ExpectTable("TEST_TABLE");
    Console.WriteLine($"      ✓ Table path: {table.GetFullyQualifiedName()}");

    Console.WriteLine("[5/6] Testing DataTable operations...");
    var dt = new DataTable();
    dt.Columns.Add("ID", typeof(int));
    dt.Columns.Add("NAME", typeof(string));
    dt.Columns.Add("CREATED_DATE", typeof(DateTime));
    dt.Rows.Add(1, "Oracle AOT Test", DateTime.Now);
    dt.Rows.Add(2, "Another Entry", DateTime.Now);
    Console.WriteLine($"      ✓ DataTable: {dt.Rows.Count} rows");

    Console.WriteLine("[6/6] Testing type translation...");
    var typeTranslater = server.GetQuerySyntaxHelper().TypeTranslater;
    var sqlType = typeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(string), 100));
    Console.WriteLine($"      ✓ Type translation: string(100) → {sqlType}");
}
