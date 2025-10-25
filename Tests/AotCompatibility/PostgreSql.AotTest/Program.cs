using System;
using System.Data;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementation;
using FAnsi.Implementations.PostgreSql;
using Npgsql;

Console.WriteLine("=== PostgreSQL Native AOT Compatibility Test ===");
Console.WriteLine();

try
{
    Console.WriteLine("Test 1: Loading PostgreSQL implementation...");
#pragma warning disable CS0618 // Type or member is obsolete
    ImplementationManager.Load<PostgreSqlImplementation>();
#pragma warning restore CS0618 // Type or member is obsolete
    Console.WriteLine("✅ Implementation loaded");
    Console.WriteLine();

    Console.WriteLine("Test 2: Creating DiscoveredServer...");
    var server = new DiscoveredServer(
        "Host=localhost;Database=postgres;",
        DatabaseType.PostgreSql);
    Console.WriteLine($"✅ Server created: {server.DatabaseType}");
    Console.WriteLine();

    Console.WriteLine("Test 3: Testing connection string builder...");
    var builder = new NpgsqlConnectionStringBuilder
    {
        Host = "localhost",
        Database = "testdb"
    };
    var server2 = new DiscoveredServer(builder);
    Console.WriteLine($"✅ Builder-based server created: {server2.Name}");
    Console.WriteLine();

    Console.WriteLine("Test 4: Testing discovery objects...");
    var database = server.ExpectDatabase("testdb");
    var table = database.ExpectTable("test_table");
    Console.WriteLine($"✅ Discovery objects: {table.GetFullyQualifiedName()}");
    Console.WriteLine();

    Console.WriteLine("Test 5: Testing DataTable operations...");
    var dt = new DataTable();
    dt.Columns.Add("id", typeof(int));
    dt.Columns.Add("name", typeof(string));
    dt.Rows.Add(1, "PostgreSQL Test");
    Console.WriteLine($"✅ DataTable: {dt.Rows.Count} rows");
    Console.WriteLine();

    Console.WriteLine("=== ALL TESTS PASSED ===");
    Console.WriteLine("Npgsql is AOT-compatible for FAnsiSql operations!");
    return 0;
}
catch (Exception ex)
{
    Console.WriteLine($"❌ TEST FAILED: {ex.GetType().Name}: {ex.Message}");
    return 1;
}
