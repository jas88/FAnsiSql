using System;
using System.Data;
using System.Data.Common;
using FAnsi.Connections;
using FAnsi.Discovery;

namespace TestCloneFix;

/// <summary>
/// Simple test to verify Clone() method behavior after fix
/// </summary>
public class CloneTest
{
    public static void Main()
    {
        Console.WriteLine("Testing Clone() method fix...");

        // Create a mock discovered server for testing
        var mockServer = new MockDiscoveredServer();

        // Create a managed connection
        using var connection = new ManagedConnection(mockServer, null);
        Console.WriteLine($"Original connection created: {connection.Connection.GetHashCode()}");

        // Clone the connection
        using var clone = connection.Clone();
        Console.WriteLine($"Cloned connection created: {clone.Connection.GetHashCode()}");

        // Test that they are the same object (reference equality)
        bool areSame = ReferenceEquals(connection.Connection, clone.Connection);
        Console.WriteLine($"Are connections the same object? {areSame}");

        if (areSame)
        {
            Console.WriteLine("✅ SUCCESS: Clone() method correctly returns same connection object");
        }
        else
        {
            Console.WriteLine("❌ FAILURE: Clone() method returns different connection objects");
            Environment.Exit(1);
        }

        // Verify transaction references are cleared
        if (clone.Transaction == null && clone.ManagedTransaction == null)
        {
            Console.WriteLine("✅ SUCCESS: Transaction references are properly cleared in clone");
        }
        else
        {
            Console.WriteLine("❌ FAILURE: Transaction references not cleared in clone");
            Environment.Exit(1);
        }
    }
}

/// <summary>
/// Mock DiscoveredServer for testing purposes
/// </summary>
public class MockDiscoveredServer : DiscoveredServer
{
    public MockDiscoveredServer() : base(new MockDbConnection(), DatabaseType.MicrosoftSQLServer)
    {
    }

    public override DbConnection GetConnection(IManagedTransaction? managedTransaction)
    {
        // For testing, always return the same connection instance
        return new MockDbConnection();
    }
}

/// <summary>
/// Mock DbConnection for testing purposes
/// </summary>
public class MockDbConnection : DbConnection
{
    private string _connectionString = string.Empty;
    private ConnectionState _state = ConnectionState.Closed;

    public override string ConnectionString
    {
        get => _connectionString;
        set => _connectionString = value;
    }

    public override string Database => "MockDatabase";

    public override string DataSource => "MockDataSource";

    public override string ServerVersion => "1.0";

    public override ConnectionState State => _state;

    public override void ChangeDatabase(string databaseName)
    {
        Database = databaseName;
    }

    public override void Close()
    {
        _state = ConnectionState.Closed;
    }

    public override void Open()
    {
        _state = ConnectionState.Open;
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        return new MockDbTransaction();
    }

    protected override DbCommand CreateDbCommand()
    {
        return new MockDbCommand();
    }
}

/// <summary>
/// Mock DbTransaction for testing purposes
/// </summary>
public class MockDbTransaction : DbTransaction
{
    public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;

    public override DbConnection? Connection { get; }

    public override void Commit() { }

    public override void Rollback() { }

    public override void Dispose() { }
}

/// <summary>
/// Mock DbCommand for testing purposes
/// </summary>
public class MockDbCommand : DbCommand
{
    public override string CommandText { get; set; } = string.Empty;
    public override int CommandTimeout { get; set; }
    public override CommandType CommandType { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }
    public override bool DesignTimeVisible { get; set; }

    protected override DbConnection? DbConnection { get; set; }
    protected override DbTransaction? DbTransaction { get; set; }

    public override void Cancel() { }

    public override int ExecuteNonQuery() => 0;

    public override object? ExecuteScalar() => null;

    protected override DbParameter CreateDbParameter()
    {
        return new MockDbParameter();
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        throw new NotImplementedException();
    }

    public override void Prepare() { }
}

/// <summary>
/// Mock DbParameter for testing purposes
/// </summary>
public class MockDbParameter : DbParameter
{
    public override DbType DbType { get; set; }
    public override ParameterDirection Direction { get; set; }
    public override bool IsNullable { get; set; }
    public override string ParameterName { get; set; } = string.Empty;
    public override string SourceColumn { get; set; } = string.Empty;
    public override object? Value { get; set; }
    public override bool SourceColumnNullMapping { get; set; }

    public override void ResetDbType() { }
}
