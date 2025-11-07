using System;
using System.Linq;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementation;

Console.WriteLine("Testing MySQL connection...");

try
{
    // Load MySQL implementation
    ImplementationManager.Load<FAnsi.Implementations.MySql.MySqlImplementation>();

    var connString = "server=127.0.0.1;Uid=root;Pwd=root;AllowPublicKeyRetrieval=True;ConvertZeroDateTime=true";
    Console.WriteLine($"Testing connection with: {connString}");

    var server = new DiscoveredServer(connString, DatabaseType.MySql);
    Console.WriteLine("Server object created successfully");

    var databases = server.DiscoverDatabases();
    Console.WriteLine($"Found {databases.Count()} databases");

    foreach (var db in databases)
    {
        Console.WriteLine($"Database: {db.GetWrappedName()}");
    }

    // Test creating a database
    Console.WriteLine("Testing database creation...");
    var testDb = server.ExpectDatabase("FAnsiTests");

    if (!testDb.Exists())
    {
        Console.WriteLine("Creating test database...");
        testDb.Create();
        Console.WriteLine("Database created successfully");
    }
    else
    {
        Console.WriteLine("Test database already exists");
    }

    Console.WriteLine("MySQL connection test completed successfully!");
}
catch (Exception ex)
{
    Console.WriteLine($"ERROR: {ex.GetType().Name}: {ex.Message}");
    Console.WriteLine($"Stack trace: {ex.StackTrace}");
}
