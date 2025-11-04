# FAnsiSql Migration Analyzer

A Roslyn analyzer that helps migrate FAnsiSql code from casting-based APIs to type-safe generic interfaces.

## Features

### Diagnostics Provided

- **FANSI0001**: Connection Casting - Detects `(ConcreteConnection)connection.Connection` patterns
- **FANSI0002**: Command Casting - Detects `(ConcreteCommand)dbCommand` patterns

### What It Detects

**Connection Casting:**
```csharp
// This will trigger FANSI0001
var sqlConn = (SqlConnection)connection.Connection;
var sqlTrans = (SqlTransaction)connection.Transaction;
```

**Command Casting:**
```csharp
// This will trigger FANSI0002
var sqlCmd = (SqlCommand)helper.GetCommand(sql, dbConnection, dbTransaction);
var adapter = new SqlDataAdapter((SqlCommand)cmd);
```

## Installation

```xml
<PackageReference Include="FAnsiSql.Migration.Analyzer" Version="1.0.0-preview" />
```

## Usage

Once installed, the analyzer will automatically:

1. **Detect** casting patterns in your FAnsiSql code
2. **Report** diagnostics with suggested improvements
3. **Provide** guidance for migrating to generic APIs

## Migration Guidance

### Before (Casting-based)
```csharp
using var connection = server.GetConnection();
var sqlConn = (SqlConnection)connection.Connection;
var sqlTrans = (SqlTransaction)connection.Transaction;

var cmd = new SqlCommand("SELECT * FROM Users", sqlConn, sqlTrans);
```

### After (Generic APIs)
```csharp
using var connection = server.GetConnection().AsGeneric<SqlConnection, SqlTransaction>();

var helper = GenericMicrosoftSQLServerHelper.Instance;
var cmd = helper.GetCommand("SELECT * FROM Users", connection.Connection, connection.Transaction);
```

## Benefits of Migration

- **Type Safety**: Compile-time checking instead of runtime casting exceptions
- **Performance**: Eliminated casting overhead in database operations
- **Maintainability**: Cleaner code with explicit type contracts
- **IntelliSense**: Better IDE support with strongly-typed APIs

## Integration with Source Generators

This analyzer works best with the `FAnsiSql.Migration.Generator` package, which provides:

- Auto-initialization of FAnsiSql providers
- Generated extension methods for convenient generic API access
- Type-safe wrapper methods for common operations

## Configuration

The analyzer can be configured through editorconfig or .editorconfig:

```ini
# FAnsiSql Migration Analyzer Configuration
fansisql_migration_analyzer.enabled = true
fansisql_migration_analyzer.connection_casting_severity = suggestion
fansisql_migration_analyzer.command_casting_severity = suggestion
```

## Suppressing Diagnostics

If you need to suppress specific diagnostics:

```csharp
#pragma warning disable FANSI0001 // Connection casting
var sqlConn = (SqlConnection)connection.Connection;
#pragma warning restore FANSI0001 // Connection casting
```

## Requirements

- **Visual Studio 2022** or **JetBrains Rider** with Roslyn support
- **.NET SDK 6.0** or later
- **FAnsiSql** packages (any database provider)

## Troubleshooting

### Analyzer Not Working

1. Ensure the package is installed as a development dependency
2. Restart Visual Studio or reload the project
3. Check that FAnsiSql packages are referenced in your project

### False Positives

If you encounter false positives:

1. Verify that you're actually using FAnsiSql types
2. Check that your project references FAnsiSql provider packages
3. Report issues to the FAnsiSql GitHub repository

### Performance Impact

The analyzer is designed to have minimal performance impact:

- Incremental analysis for fast response times
- Simple pattern matching for low overhead
- No external dependencies or network calls
