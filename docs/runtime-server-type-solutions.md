# Runtime Server Type Solutions for Generic FAnsiSql

## Problem Statement

The current generic interface design assumes compile-time knowledge of database types, but FAnsiSql heavily relies on **runtime type discovery** patterns. Many common scenarios involve working with databases where the type is only known at runtime.

## Current Runtime Discovery Patterns

### 1. Connection String-Based Discovery
```csharp
// Database type determined from connection string
var server = new DiscoveredServer(connectionStringBuilder);
var helper = server.Helper; // Type discovered at runtime
```

### 2. Connection-Based Discovery
```csharp
// Database type determined from existing connection
var server = new DiscoveredServer(existingConnection);
var implementation = ImplementationManager.GetImplementation(existingConnection);
```

### 3. Configuration-Based Discovery
```csharp
// Database type from configuration file
var dbType = config.GetDatabaseType(); // Determined at runtime
var server = server.GetServer(dbType);
```

## Solution: Hybrid Generic + Runtime Architecture

### Core Design Principle

**Generic for Known Types + Runtime Abstraction for Unknown Types**

The solution maintains both generic interfaces (for compile-time known types) and runtime abstractions (for dynamic discovery), with seamless interoperability between them.

## Proposed Architecture

### 1. Enhanced Generic Interfaces with Runtime Support

```csharp
// Enhanced generic interface that supports runtime discovery
public interface IDiscoveredServerHelper<TConnection, TTransaction, TCommand, TDataAdapter, TParameter, TCommandBuilder> : IDiscoveredServerHelper
    where TConnection : DbConnection
    where TTransaction : DbTransaction
    where TCommand : DbCommand
    where TDataAdapter : DbDataAdapter
    where TParameter : DbParameter
    where TCommandBuilder : DbCommandBuilder
{
    // Generic methods (compile-time type safe)
    new TCommand GetCommand(string sql, TConnection connection, TTransaction? transaction = null);
    new TDataAdapter GetDataAdapter(TCommand command);
    new TParameter GetParameter();
    new TCommandBuilder GetCommandBuilder(TDataAdapter adapter);

    // Runtime compatibility methods
    DbCommand IDiscoveredServerHelper.GetCommand(string sql, DbConnection con, DbTransaction? transaction)
        => GetCommand(sql, (TConnection)con, (TTransaction?)transaction);

    DbDataAdapter IDiscoveredServerHelper.GetDataAdapter(DbCommand cmd)
        => GetDataAdapter((TCommand)cmd);

    DbParameter IDiscoveredServerHelper.GetParameter(string parameterName)
        => GetParameter(parameterName);

    DbCommandBuilder IDiscoveredServerHelper.GetCommandBuilder(DbCommand cmd)
        => GetCommandBuilder(GetDataAdapter((TCommand)cmd));
}
```

### 2. Runtime Wrapper for Generic Implementations

```csharp
// Runtime wrapper that adapts generic implementations to legacy interface
public sealed class RuntimeServerHelperWrapper<TConnection, TTransaction, TCommand, TDataAdapter, TParameter, TCommandBuilder> : IDiscoveredServerHelper
    where TConnection : DbConnection
    where TTransaction : DbTransaction
    where TCommand : DbCommand
    where TDataAdapter : DbDataAdapter
    where TParameter : DbParameter
    where TCommandBuilder : DbCommandBuilder
{
    private readonly IDiscoveredServerHelper<TConnection, TTransaction, TCommand, TDataAdapter, TParameter, TCommandBuilder> _genericHelper;

    public RuntimeServerHelperWrapper(
        IDiscoveredServerHelper<TConnection, TTransaction, TCommand, TDataAdapter, TParameter, TCommandBuilder> genericHelper)
    {
        _genericHelper = genericHelper;
    }

    public DatabaseType DatabaseType => _genericHelper.DatabaseType;

    // Legacy interface implementation that delegates to generic implementation
    public DbCommand GetCommand(string sql, DbConnection con, DbTransaction? transaction = null)
    {
        // Safe casting with runtime validation
        if (con is not TConnection typedConnection)
            throw new InvalidOperationException($"Connection must be of type {typeof(TConnection).Name}");

        if (transaction != null && transaction is not TTransaction typedTransaction)
            throw new InvalidOperationException($"Transaction must be of type {typeof(TTransaction).Name}");

        return _genericHelper.GetCommand(sql, typedConnection, typedTransaction);
    }

    public DbDataAdapter GetDataAdapter(DbCommand cmd)
    {
        if (cmd is not TCommand typedCommand)
            throw new InvalidOperationException($"Command must be of type {typeof(TCommand).Name}");

        return _genericHelper.GetDataAdapter(typedCommand);
    }

    public DbParameter GetParameter(string parameterName)
    {
        return _genericHelper.GetParameter(parameterName);
    }

    public DbCommandBuilder GetCommandBuilder(DbCommand cmd)
    {
        if (cmd is not TCommand typedCommand)
            throw new InvalidOperationException($"Command must be of type {typeof(TCommand).Name}");

        return _genericHelper.GetCommandBuilder(_genericHelper.GetDataAdapter(typedCommand));
    }

    public DbConnection GetConnection(DbConnectionStringBuilder builder)
    {
        return _genericHelper.GetConnection(builder);
    }

    public DbConnectionStringBuilder GetConnectionStringBuilder(string? connectionString)
    {
        return _genericHelper.GetConnectionStringBuilder(connectionString);
    }
}
```

### 3. Enhanced DiscoveredServer with Dual Interface Support

```csharp
public sealed class DiscoveredServer : IMightNotExist
{
    private readonly IDiscoveredServerHelper _helper;
    private readonly IDiscoveredServerHelper? _genericHelper;

    // Legacy property for backward compatibility
    public IDiscoveredServerHelper Helper => _helper;

    // New generic property when type is known at compile time
    public IDiscoveredServerHelper<TConnection, TTransaction, TCommand, TDataAdapter, TParameter, TCommandBuilder>? GenericHelper<TConnection, TTransaction, TCommand, TDataAdapter, TParameter, TCommandBuilder>()
        where TConnection : DbConnection
        where TTransaction : DbTransaction
        where TCommand : DbCommand
        where TDataAdapter : DbDataAdapter
        where TParameter : DbParameter
        where TCommandBuilder : DbCommandBuilder
    {
        get => _genericHelper as IDiscoveredServerHelper<TConnection, TTransaction, TCommand, TDataAdapter, TParameter, TCommandBuilder>;
    }

    // Enhanced constructor with generic support
    public DiscoveredServer(DbConnectionStringBuilder builder)
    {
        // Try to get generic implementation first
        _genericHelper = TryGetGenericHelper(builder);

        // Fall back to legacy implementation
        _helper = _genericHelper as IDiscoveredServerHelper ??
                  ImplementationManager.GetImplementation(builder).GetServerHelper();
    }

    private static IDiscoveredServerHelper? TryGetGenericHelper(DbConnectionStringBuilder builder)
    {
        var implementation = ImplementationManager.GetImplementation(builder);

        return implementation.DatabaseType switch
        {
            DatabaseType.MicrosoftSQLServer => CreateGenericWrapper<SqlConnection, SqlTransaction, SqlCommand, SqlDataAdapter, SqlParameter, SqlCommandBuilder>(implementation),
            DatabaseType.MySql => CreateGenericWrapper<MySqlConnection, MySqlTransaction, MySqlCommand, MySqlDataAdapter, MySqlParameter, MySqlCommandBuilder>(implementation),
            DatabaseType.PostgreSql => CreateGenericWrapper<NpgsqlConnection, NpgsqlTransaction, NpgsqlCommand, NpgsqlDataAdapter, NpgsqlParameter, NpgsqlCommandBuilder>(implementation),
            DatabaseType.Oracle => CreateGenericWrapper<OracleConnection, OracleTransaction, OracleCommand, OracleDataAdapter, OracleParameter, OracleCommandBuilder>(implementation),
            DatabaseType.Sqlite => CreateGenericWrapper<SqliteConnection, SqliteTransaction, SqliteCommand, SqliteDataAdapter, SqliteParameter, SqliteCommandBuilder>(implementation),
            _ => null
        };
    }

    private static IDiscoveredServerHelper CreateGenericWrapper<TConnection, TTransaction, TCommand, TDataAdapter, TParameter, TCommandBuilder>(
        IImplementation implementation)
        where TConnection : DbConnection
        where TTransaction : DbTransaction
        where TCommand : DbCommand
        where TDataAdapter : DbDataAdapter
        where TParameter : DbParameter
        where TCommandBuilder : DbCommandBuilder
    {
        // This would create the appropriate generic helper implementation
        // For now, return the legacy helper as fallback
        return implementation.GetServerHelper();
    }
}
```

### 4. Pattern Matching Helper Methods

```csharp
public static class DiscoveredServerExtensions
{
    // Pattern matching for runtime type usage
    public static T UseHelper<T>(this DiscoveredServer server, Func<IDiscoveredServerHelper, T> operation)
    {
        return operation(server.Helper);
    }

    // Generic pattern matching when type might be known
    public static T UseGenericHelper<TConnection, TTransaction, TCommand, TDataAdapter, TParameter, TCommandBuilder, T>(
        this DiscoveredServer server,
        Func<IDiscoveredServerHelper<TConnection, TTransaction, TCommand, TDataAdapter, TParameter, TCommandBuilder>, T> operation)
        where TConnection : DbConnection
        where TTransaction : DbTransaction
        where TCommand : DbCommand
        where TDataAdapter : DbDataAdapter
        where TParameter : DbParameter
        where TCommandBuilder : DbCommandBuilder
    {
        var genericHelper = server.GenericHelper<TConnection, TTransaction, TCommand, TDataAdapter, TParameter, TCommandBuilder>();
        if (genericHelper != null)
        {
            return operation(genericHelper);
        }

        // Fall back to runtime operation with legacy helper
        throw new NotSupportedException($"Generic helper not available for {typeof(TConnection).Name}");
    }

    // Database type-specific pattern matching
    public static T UseSqlServer<T>(this DiscoveredServer server, Func<IDiscoveredServerHelper<SqlConnection, SqlTransaction, SqlCommand, SqlDataAdapter, SqlParameter, SqlCommandBuilder>, T> operation)
    {
        if (server.DatabaseType == DatabaseType.MicrosoftSQLServer)
        {
            var genericHelper = server.GenericHelper<SqlConnection, SqlTransaction, SqlCommand, SqlDataAdapter, SqlParameter, SqlCommandBuilder>();
            if (genericHelper != null)
                return operation(genericHelper);
        }

        throw new NotSupportedException("This operation requires SQL Server");
    }

    public static T UseMySql<T>(this DiscoveredServer server, Func<IDiscoveredServerHelper<MySqlConnection, MySqlTransaction, MySqlCommand, MySqlDataAdapter, MySqlParameter, MySqlCommandBuilder>, T> operation)
    {
        if (server.DatabaseType == DatabaseType.MySql)
        {
            var genericHelper = server.GenericHelper<MySqlConnection, MySqlTransaction, MySqlCommand, MySqlDataAdapter, MySqlParameter, MySqlCommandBuilder>();
            if (genericHelper != null)
                return operation(genericHelper);
        }

        throw new NotSupportedException("This operation requires MySQL");
    }

    // Similar methods for other database types...
}
```

### 5. Enhanced Source Generator for Runtime Support

```csharp
// Updated source generator that handles runtime scenarios
public class EnhancedGenericWrapperGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        // Generate runtime-compatible extension methods
        context.RegisterSourceOutput(context.CompilationProvider, GenerateRuntimeExtensions);
    }

    private void GenerateRuntimeExtensions(SourceProductionContext context, Compilation compilation)
    {
        var sourceBuilder = new StringBuilder();

        sourceBuilder.AppendLine("// <auto-generated/>");
        sourceBuilder.AppendLine("namespace FAnsiSql.Generated");
        sourceBuilder.AppendLine("{");
        sourceBuilder.AppendLine("    using System;");
        sourceBuilder.AppendLine("    using FAnsi.Discovery;");
        sourceBuilder.AppendLine();

        // Generate pattern matching extensions for each database type
        GenerateSqlServerExtensions(sourceBuilder);
        GenerateMySqlExtensions(sourceBuilder);
        GeneratePostgreSqlExtensions(sourceBuilder);
        GenerateOracleExtensions(sourceBuilder);
        GenerateSqliteExtensions(sourceBuilder);

        // Generate generic fallback methods
        GenerateGenericFallbackMethods(sourceBuilder);

        sourceBuilder.AppendLine("}");

        context.AddSource("RuntimeGenericExtensions.g.cs", SourceText.From(sourceBuilder.ToString()));
    }

    private void GenerateSqlServerExtensions(StringBuilder sourceBuilder)
    {
        sourceBuilder.AppendLine("    public static class DiscoveredServerSqlServerExtensions");
        sourceBuilder.AppendLine("    {");
        sourceBuilder.AppendLine("        /// <summary>");
        sourceBuilder.AppendLine("        /// Executes an operation using SQL Server-specific generic helper when available, falls back to runtime helper otherwise.");
        sourceBuilder.AppendLine("        /// </summary>");
        sourceBuilder.AppendLine("        public static T WithSqlServer<T>(");
        sourceBuilder.AppendLine("            this DiscoveredServer server,");
        sourceBuilder.AppendLine("            Func<IDiscoveredServerHelper<SqlConnection, SqlTransaction, SqlCommand, SqlDataAdapter, SqlParameter, SqlCommandBuilder>, T> genericOperation,");
        sourceBuilder.AppendLine("            Func<IDiscoveredServerHelper, T>? fallbackOperation = null)");
        sourceBuilder.AppendLine("        {");
        sourceBuilder.AppendLine("            if (server.DatabaseType == FAnsi.Discovery.DatabaseType.MicrosoftSQLServer)");
        sourceBuilder.AppendLine("            {");
        sourceBuilder.AppendLine("                try");
        sourceBuilder.AppendLine("                {");
        sourceBuilder.AppendLine("                    var genericHelper = server.GenericHelper<SqlConnection, SqlTransaction, SqlCommand, SqlDataAdapter, SqlParameter, SqlCommandBuilder>();");
        sourceBuilder.AppendLine("                    if (genericHelper != null)");
        sourceBuilder.AppendLine("                        return genericOperation(genericHelper);");
        sourceBuilder.AppendLine("                }");
        sourceBuilder.AppendLine("                catch");
        sourceBuilder.AppendLine("                {");
        sourceBuilder.AppendLine("                    // Fall back to runtime operation if generic helper not available");
        sourceBuilder.AppendLine("                }");
        sourceBuilder.AppendLine("            }");
        sourceBuilder.AppendLine();
        sourceBuilder.AppendLine("            // Use fallback operation or throw");
        sourceBuilder.AppendLine("            if (fallbackOperation != null)");
        sourceBuilder.AppendLine("                return fallbackOperation(server.Helper);");
        sourceBuilder.AppendLine("            ");
        sourceBuilder.AppendLine("            throw new NotSupportedException(\"SQL Server is required for this operation\");");
        sourceBuilder.AppendLine("        }");
        sourceBuilder.AppendLine("    }");
        sourceBuilder.AppendLine();
    }

    // Similar methods for other database types...

    private void GenerateGenericFallbackMethods(StringBuilder sourceBuilder)
    {
        sourceBuilder.AppendLine("    public static class GenericFallbackExtensions");
        sourceBuilder.AppendLine("    {");
        sourceBuilder.AppendLine("        /// <summary>");
        sourceBuilder.AppendLine("        /// Attempts to use generic helper when database type is known, falls back to runtime casting otherwise.");
        sourceBuilder.AppendLine("        /// </summary>");
        sourceBuilder.AppendLine("        public static T WithTypedConnection<T>(");
        sourceBuilder.AppendLine("            this DiscoveredServer server,");
        sourceBuilder.AppendLine("            Func<object, T> operation)");
        sourceBuilder.AppendLine("        {");
        sourceBuilder.AppendLine("            return server.DatabaseType switch");
        sourceBuilder.AppendLine("            {");
        sourceBuilder.AppendLine("                FAnsi.Discovery.DatabaseType.MicrosoftSQLServer =>");
        sourceBuilder.AppendLine("                    server.WithSqlServer(");
        sourceBuilder.AppendLine("                        helper => operation(helper),");
        sourceBuilder.AppendLine("                        helper => operation(helper)),");
        sourceBuilder.AppendLine("                FAnsi.Discovery.DatabaseType.MySql =>");
        sourceBuilder.AppendLine("                    server.WithMySql(");
        sourceBuilder.AppendLine("                        helper => operation(helper),");
        sourceBuilder.AppendLine("                        helper => operation(helper)),");
        sourceBuilder.AppendLine("                // Other database types...");
        sourceBuilder.AppendLine("                _ => operation(server.Helper) // Fall back to runtime");
        sourceBuilder.AppendLine("            };");
        sourceBuilder.AppendLine("        }");
        sourceBuilder.AppendLine("    }");
    }
}
```

## Usage Examples

### 1. Runtime Database Discovery (Current Pattern Maintained)

```csharp
// Database type unknown at compile time - works exactly as before
var server = new DiscoveredServer(connectionStringBuilder);

// Use legacy runtime helper (always available)
var cmd = server.Helper.GetCommand("SELECT * FROM Users", connection, transaction);
var adapter = server.Helper.GetDataAdapter(cmd);
```

### 2. Generic When Available, Runtime Fallback

```csharp
var server = new DiscoveredServer(connectionStringBuilder);

// Try generic first, fall back to runtime if not available
var result = server.WithSqlServer(
    genericHelper =>
    {
        // Type-safe generic operation
        var cmd = genericHelper.GetCommand("SELECT * FROM Users", connection, transaction);
        return ExecuteQuery(cmd);
    },
    fallbackHelper =>
    {
        // Runtime fallback with casting
        var cmd = fallbackHelper.GetCommand("SELECT * FROM Users", connection, transaction);
        return ExecuteQuery(cmd);
    });
```

### 3. Database-Specific Operations with Runtime Detection

```csharp
public void PerformDatabaseSpecificOperation(DiscoveredServer server)
{
    switch (server.DatabaseType)
    {
        case DatabaseType.MicrosoftSQLServer:
            server.WithSqlServer(helper =>
            {
                // Use SQL Server-specific features
                var cmd = helper.GetCommand("EXEC sp_who2", connection, transaction);
                return cmd.ExecuteNonQuery();
            });
            break;

        case DatabaseType.MySql:
            server.WithMySql(helper =>
            {
                // Use MySQL-specific features
                var cmd = helper.GetCommand("SHOW PROCESSLIST", connection, transaction);
                return cmd.ExecuteNonQuery();
            });
            break;

        default:
            // Fall back to generic runtime operation
            var cmd = server.Helper.GetCommand("SELECT 1", connection, transaction);
            cmd.ExecuteNonQuery();
            break;
    }
}
```

### 4. Configuration-Driven Database Operations

```csharp
public class DatabaseService
{
    private readonly DiscoveredServer _server;

    public DatabaseService(string connectionString)
    {
        _server = new DiscoveredServer(DbConnectionStringBuilderFactory.Create(connectionString));
    }

    public List<User> GetUsers()
    {
        // Use best available approach - generic if possible, runtime if not
        return _server.WithTypedConnection(helper =>
        {
            var cmd = helper.GetCommand("SELECT * FROM Users", GetConnection(), GetTransaction());
            return ExecuteUserQuery(cmd);
        });
    }

    // Database-specific optimizations when available
    public void OptimizeForDatabase()
    {
        switch (_server.DatabaseType)
        {
            case DatabaseType.MicrosoftSQLServer:
                _server.WithSqlServer(helper =>
                {
                    // SQL Server-specific optimization
                    OptimizeSqlServer(helper);
                });
                break;

            case DatabaseType.MySql:
                _server.WithMySql(helper =>
                {
                    // MySQL-specific optimization
                    OptimizeMySql(helper);
                });
                break;
        }
    }
}
```

## Benefits of This Approach

### 1. **Backward Compatibility Maintained**
- All existing runtime discovery patterns continue to work
- No breaking changes for current consumers
- Gradual migration path available

### 2. **Performance Optimizations When Possible**
- Generic operations used when database type is known
- Runtime fallback only when necessary
- Compile-time type safety for known scenarios

### 3. **Flexibility for Unknown Types**
- Handles all current runtime discovery use cases
- Database switching and dynamic configuration supported
- Configuration-driven database selection maintained

### 4. **Developer Experience**
- IntelliSense available for generic operations
- Pattern matching provides clear intent
- Graceful fallbacks prevent runtime errors

### 5. **Migration Path**
- Existing code works unchanged
- New code can use generic APIs when appropriate
- Gradual adoption possible at method/class level

## Implementation Strategy

### Phase 1: Foundation (Weeks 1-2)
- Implement hybrid generic interfaces
- Create runtime wrapper classes
- Update source generators for pattern matching

### Phase 2: Core Integration (Weeks 3-4)
- Update DiscoveredServer with dual interface support
- Implement pattern matching extensions
- Update ImplementationManager for generic support

### Phase 3: Testing and Validation (Weeks 5-6)
- Comprehensive testing of runtime scenarios
- Performance benchmarking of hybrid approach
- Validation of all existing use cases

### Phase 4: Documentation and Migration (Weeks 7-8)
- Update migration documentation
- Provide examples for runtime scenarios
- Community guidance for mixed usage

This hybrid approach ensures that **all current runtime discovery patterns continue to work** while **providing generic type safety when the database type is known**, giving the best of both worlds.
