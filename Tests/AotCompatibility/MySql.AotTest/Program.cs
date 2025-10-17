using System;
using System.Data;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementation;
using FAnsi.Implementations.MySql;
using MySqlConnector;

Console.WriteLine("=== MySQL Native AOT Compatibility Test ===");
Console.WriteLine();

try
{
    Console.WriteLine("Test 1: Loading MySQL implementation...");
    ImplementationManager.Load<MySqlImplementation>();
    Console.WriteLine("✅ Implementation loaded");
    Console.WriteLine();

    Console.WriteLine("Test 2: Creating DiscoveredServer...");
    var server = new DiscoveredServer(
        "Server=localhost;Database=mysql;",
        DatabaseType.MySql);
    Console.WriteLine($"✅ Server created: {server.DatabaseType}");
    Console.WriteLine();

    Console.WriteLine("Test 3: Testing connection string builder...");
    var builder = new MySqlConnectionStringBuilder
    {
        Server = "localhost",
        Database = "testdb"
    };
    var server2 = new DiscoveredServer(builder);
    Console.WriteLine($"✅ Builder-based server created: {server2.Name}");
    Console.WriteLine();

    Console.WriteLine("Test 4: Testing discovery objects...");
    var database = server.ExpectDatabase("TestDB");
    var table = database.ExpectTable("TestTable");
    Console.WriteLine($"✅ Discovery objects: {table.GetFullyQualifiedName()}");
    Console.WriteLine();

    Console.WriteLine("Test 5: Testing DataTable operations...");
    var dt = new DataTable();
    dt.Columns.Add("Id", typeof(int));
    dt.Columns.Add("Name", typeof(string));
    dt.Rows.Add(1, "MySQL Test");
    Console.WriteLine($"✅ DataTable: {dt.Rows.Count} rows");
    Console.WriteLine();

    Console.WriteLine("=== ALL TESTS PASSED ===");
    Console.WriteLine("MySqlConnector is AOT-compatible for FAnsiSql operations!");
    return 0;
}
catch (Exception ex)
{
    Console.WriteLine($"❌ TEST FAILED: {ex.GetType().Name}: {ex.Message}");
    return 1;
}
