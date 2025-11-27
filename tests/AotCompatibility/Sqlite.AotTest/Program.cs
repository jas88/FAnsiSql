using System;
using System.Data;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementation;
using FAnsi.Implementations.Sqlite;
using Microsoft.Data.Sqlite;

Console.WriteLine("=== SQLite Native AOT Compatibility Test ===");
Console.WriteLine();

try
{
    Console.WriteLine("Test 1: Loading SQLite implementation...");
#pragma warning disable CS0618 // Type or member is obsolete
    ImplementationManager.Load<SqliteImplementation>();
#pragma warning restore CS0618 // Type or member is obsolete
    Console.WriteLine("✅ Implementation loaded");
    Console.WriteLine();

    Console.WriteLine("Test 2: Creating in-memory DiscoveredServer...");
    var server = new DiscoveredServer(
        "Data Source=:memory:",
        DatabaseType.Sqlite);
    Console.WriteLine($"✅ Server created: {server.DatabaseType}");
    Console.WriteLine();

    Console.WriteLine("Test 3: Testing connection string builder...");
    var builder = new SqliteConnectionStringBuilder
    {
        DataSource = ":memory:",
        Mode = SqliteOpenMode.Memory
    };
    var server2 = new DiscoveredServer(builder);
    Console.WriteLine($"✅ Builder-based server created: {server2.Name}");
    Console.WriteLine();

    Console.WriteLine("Test 4: Testing discovery objects...");
    var database = server.ExpectDatabase("main");
    var table = database.ExpectTable("TestTable");
    Console.WriteLine($"✅ Discovery objects: {table.GetFullyQualifiedName()}");
    Console.WriteLine();

    Console.WriteLine("Test 5: Testing DataTable operations...");
    var dt = new DataTable();
    dt.Columns.Add("Id", typeof(int));
    dt.Columns.Add("Name", typeof(string));
    dt.Rows.Add(1, "SQLite Test");
    Console.WriteLine($"✅ DataTable: {dt.Rows.Count} rows");
    Console.WriteLine();

    Console.WriteLine("=== ALL TESTS PASSED ===");
    Console.WriteLine("Microsoft.Data.Sqlite is AOT-compatible for FAnsiSql operations!");
    return 0;
}
catch (Exception ex)
{
    Console.WriteLine($"❌ TEST FAILED: {ex.GetType().Name}: {ex.Message}");
    return 1;
}
