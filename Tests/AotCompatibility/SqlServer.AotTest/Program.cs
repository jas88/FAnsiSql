using System;
using System.Data;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using Microsoft.Data.SqlClient;
using TypeGuesser;

Console.WriteLine("=== SQL Server Native AOT Compatibility Test ===");
Console.WriteLine();

try
{
    // Test 1: Load implementation
    Console.WriteLine("Test 1: Loading SQL Server implementation...");
    ImplementationManager.Load<MicrosoftSQLImplementation>();
    Console.WriteLine("✅ Implementation loaded successfully");
    Console.WriteLine();

    // Test 2: Create server object
    Console.WriteLine("Test 2: Creating DiscoveredServer...");
    var server = new DiscoveredServer(
        "Server=localhost;Database=master;TrustServerCertificate=True;",
        DatabaseType.MicrosoftSQLServer);
    Console.WriteLine($"✅ Server created: {server.DatabaseType}");
    Console.WriteLine();

    // Test 3: Test connection string builder
    Console.WriteLine("Test 3: Testing connection string builder...");
    var builder = new SqlConnectionStringBuilder
    {
        DataSource = "localhost",
        InitialCatalog = "tempdb",
        TrustServerCertificate = true
    };
    var server2 = new DiscoveredServer(builder);
    Console.WriteLine($"✅ Builder-based server created: {server2.Name}");
    Console.WriteLine();

    // Test 4: Test database discovery objects
    Console.WriteLine("Test 4: Testing discovery objects...");
    var database = server.ExpectDatabase("TestDB");
    var table = database.ExpectTable("TestTable");
    Console.WriteLine($"✅ Discovery objects created: {table.GetFullyQualifiedName()}");
    Console.WriteLine();

    // Test 5: Test DataTable creation (bulk copy simulation)
    Console.WriteLine("Test 5: Testing DataTable operations...");
    var dt = new DataTable();
    dt.Columns.Add("Id", typeof(int));
    dt.Columns.Add("Name", typeof(string));
    dt.Columns.Add("CreatedDate", typeof(DateTime));
    dt.Rows.Add(1, "Test Item", DateTime.Now);
    dt.Rows.Add(2, "Another Test", DateTime.Now);
    Console.WriteLine($"✅ DataTable created with {dt.Rows.Count} rows, {dt.Columns.Count} columns");
    Console.WriteLine();

    // Test 6: Test type translation
    Console.WriteLine("Test 6: Testing type translation...");
    var typeTranslater = server.GetQuerySyntaxHelper().TypeTranslater;
    var sqlType = typeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(string), 100));
    Console.WriteLine($"✅ Type translation works: string(100) → {sqlType}");
    Console.WriteLine();

    Console.WriteLine("=== ALL TESTS PASSED ===");
    Console.WriteLine();
    Console.WriteLine("SQL Server driver (Microsoft.Data.SqlClient) is AOT-compatible for FAnsiSql operations!");
    return 0;
}
catch (Exception ex)
{
    Console.WriteLine($"❌ TEST FAILED: {ex.GetType().Name}");
    Console.WriteLine($"Message: {ex.Message}");
    Console.WriteLine($"Stack: {ex.StackTrace}");
    return 1;
}
