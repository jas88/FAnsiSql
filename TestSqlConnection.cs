using System;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using FAnsi.Discovery;
using FAnsi.Implementations.Sqlite;

public class TestSqlConnection
{
    public static void Main(string[] args)
    {
        try
        {
            // Ensure SQLite implementation is loaded
            SqliteImplementation.EnsureLoaded();

            var testConnectionString = "Data Source=FAnsiTests.db;Pooling=True;Cache=Private";
            Console.WriteLine($"Original connection string: {testConnectionString}");

            // Create DiscoveredServer
            var server = new DiscoveredServer(testConnectionString, DatabaseType.Sqlite);
            Console.WriteLine($"Server Builder ConnectionString: {server.Builder.ConnectionString}");
            Console.WriteLine($"Server Name: {server.Name}");

            // Test GetServerName
            var helper = SqliteServerHelper.Instance;
            var serverName = helper.GetServerName(server.Builder);
            Console.WriteLine($"GetServerName result: {serverName}");

            // Test GetCurrentDatabase
            var currentDb = helper.GetCurrentDatabase(server.Builder);
            Console.WriteLine($"GetCurrentDatabase result: {currentDb}");

            // Test ExpectDatabase (this is where the error might occur)
            var database = server.ExpectDatabase("FAnsiTests.db");
            Console.WriteLine($"Database Builder ConnectionString: {database.Server.Builder.ConnectionString}");

            // Test creating connection
            using var connection = database.Server.GetConnection();
            Console.WriteLine($"Connection ConnectionString: {connection.ConnectionString}");

            // Try to open
            connection.Open();
            Console.WriteLine("✓ Connection opened successfully");

            connection.Close();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Error: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
            if (ex.InnerException != null)
            {
                Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
                Console.WriteLine($"Inner stack trace: {ex.InnerException.StackTrace}");
            }
        }
    }
}
