#!/usr/bin/env dotnet-script

#r "FAnsi.Core/bin/Debug/net9.0/FAnsi.Core.dll"
#r "FAnsi.Sqlite/bin/Debug/net9.0/FAnsi.Sqlite.dll"
#r "nuget: Microsoft.Data.Sqlite, 8.0.0"

using FAnsi;
using FAnsi.Discovery;
using Microsoft.Data.Sqlite;
using System;

// Test FAnsi SQLite connection string parsing
var testConnectionString = "Data Source=FAnsiTests.db;Pooling=True;Cache=Private";
Console.WriteLine($"Original connection string: {testConnectionString}");

try
{
    // Test what FAnsi DiscoveredServer does
    var server = new DiscoveredServer(testConnectionString, DatabaseType.Sqlite);
    Console.WriteLine($"FAnsi server builder connection string: {server.Builder.ConnectionString}");
    Console.WriteLine($"Database type: {server.DatabaseType}");

    // Test getting the database
    var database = server.ExpectDatabase("FAnsiTests.db");
    Console.WriteLine($"Database builder connection string: {database.Server.Builder.ConnectionString}");

    // Try to get a connection
    using var connection = database.Server.GetConnection();
    Console.WriteLine($"Connection string from GetConnection: {connection.ConnectionString}");

    // Try to open it
    connection.Open();
    Console.WriteLine("✓ FAnsi connection opened successfully");
}
catch (Exception ex)
{
    Console.WriteLine($"❌ FAnsi Error: {ex.Message}");
    Console.WriteLine($"Stack trace: {ex.StackTrace}");

    if (ex.InnerException != null)
    {
        Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
    }
}
