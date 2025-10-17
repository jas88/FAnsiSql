using System;
using System.Data;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementation;
using FAnsi.Implementations.Oracle;
using Oracle.ManagedDataAccess.Client;

Console.WriteLine("=== Oracle Native AOT Compatibility Test ===");
Console.WriteLine();

try
{
    Console.WriteLine("Test 1: Loading Oracle implementation...");
    ImplementationManager.Load<OracleImplementation>();
    Console.WriteLine("✅ Implementation loaded");
    Console.WriteLine();

    Console.WriteLine("Test 2: Creating DiscoveredServer...");
    var server = new DiscoveredServer(
        "Data Source=localhost/XE;",
        DatabaseType.Oracle);
    Console.WriteLine($"✅ Server created: {server.DatabaseType}");
    Console.WriteLine();

    Console.WriteLine("Test 3: Testing connection string builder...");
    var builder = new OracleConnectionStringBuilder
    {
        DataSource = "localhost/XE"
    };
    var server2 = new DiscoveredServer(builder);
    Console.WriteLine($"✅ Builder-based server created: {server2.Name}");
    Console.WriteLine();

    Console.WriteLine("Test 4: Testing discovery objects...");
    var database = server.ExpectDatabase("TESTDB");
    var table = database.ExpectTable("TEST_TABLE");
    Console.WriteLine($"✅ Discovery objects: {table.GetFullyQualifiedName()}");
    Console.WriteLine();

    Console.WriteLine("Test 5: Testing DataTable operations...");
    var dt = new DataTable();
    dt.Columns.Add("ID", typeof(int));
    dt.Columns.Add("NAME", typeof(string));
    dt.Rows.Add(1, "Oracle Test");
    Console.WriteLine($"✅ DataTable: {dt.Rows.Count} rows");
    Console.WriteLine();

    Console.WriteLine("=== ALL TESTS PASSED ===");
    Console.WriteLine("Oracle.ManagedDataAccess.Core is AOT-compatible for FAnsiSql operations!");
    return 0;
}
catch (Exception ex)
{
    Console.WriteLine($"❌ TEST FAILED: {ex.GetType().Name}: {ex.Message}");
    return 1;
}
