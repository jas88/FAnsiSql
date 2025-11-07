using System;
using FAnsi;
using FAnsi.Discovery;

class Program
{
    static void Main()
    {
        try
        {
            var connString = "server=127.0.0.1;Uid=root;Pwd=root;AllowPublicKeyRetrieval=True;ConvertZeroDateTime=true";
            Console.WriteLine($"Testing connection with: {connString}");

            var server = new DiscoveredServer(connString, DatabaseType.MySql);
            Console.WriteLine("Server object created successfully");

            var databases = server.DiscoverDatabases();
            Console.WriteLine($"Found {databases.Length} databases");

            foreach (var db in databases)
            {
                Console.WriteLine($"Database: {db.GetWrappedName()}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"ERROR: {ex.GetType().Name}: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
        }
    }
}
