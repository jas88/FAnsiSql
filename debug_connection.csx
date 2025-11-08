#!/usr/bin/env dotnet-script

#r "nuget: Microsoft.Data.Sqlite, 8.0.0"

using Microsoft.Data.Sqlite;
using System;
using System.Data.Common;

// Test SQLite connection string parsing
var testConnectionString = "Data Source=FAnsiTests.db;Pooling=True;Cache=Private";
Console.WriteLine($"Original connection string: {testConnectionString}");

// Test creating a connection string builder
try
{
    var builder = new SqliteConnectionStringBuilder(testConnectionString);
    Console.WriteLine($"Parsed builder connection string: {builder.ConnectionString}");
    Console.WriteLine($"DataSource: {builder.DataSource}");

    // Test creating a connection
    using var connection = new SqliteConnection(builder.ConnectionString);
    Console.WriteLine($"Connection string for new connection: {connection.ConnectionString}");

    // Try to open the connection
    connection.Open();
    Console.WriteLine("✓ Connection opened successfully");

    // Try a simple query
    using var cmd = connection.CreateCommand();
    cmd.CommandText = "SELECT sqlite_version()";
    var version = cmd.ExecuteScalar();
    Console.WriteLine($"✓ SQLite version: {version}");

    connection.Close();
    Console.WriteLine("✓ Connection closed successfully");
}
catch (Exception ex)
{
    Console.WriteLine($"❌ Error: {ex.Message}");
    Console.WriteLine($"Stack trace: {ex.StackTrace}");
}
