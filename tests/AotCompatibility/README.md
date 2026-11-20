# FAnsiSql Native AOT Compatibility Tests

This directory contains Native AOT (Ahead-of-Time) compatibility tests for FAnsiSql and all database implementations.

## Overview

These tests verify that FAnsiSql and its database-specific implementations can be compiled and run as native AOT binaries, which:
- Eliminates JIT compilation overhead
- Reduces application startup time
- Enables deployment without the .NET runtime
- Reduces memory footprint
- Improves cold-start performance

## Test Projects

### Individual Database Tests
- `SqlServer.AotTest/` - Microsoft SQL Server (Microsoft.Data.SqlClient)
- `MySql.AotTest/` - MySQL (MySqlConnector)
- `PostgreSql.AotTest/` - PostgreSQL (Npgsql)
- `Oracle.AotTest/` - Oracle (Oracle.ManagedDataAccess.Core)
- `Sqlite.AotTest/` - SQLite (Microsoft.Data.Sqlite)

### Comprehensive Test
- `FAnsi.AotTest/` - Tests the SQL Server, MySQL, PostgreSQL and Oracle implementations in a single AOT binary

## Running Tests Locally

### Build and Run Individual Database Test
```bash
cd tests/AotCompatibility/SqlServer.AotTest
dotnet publish -c Release -r <RID>
./bin/Release/net11.0/<RID>/publish/SqlServer.AotTest
```

### Build and Run Comprehensive Test
```bash
cd tests/AotCompatibility/FAnsi.AotTest
dotnet publish -c Release -r <RID>
./bin/Release/net11.0/<RID>/publish/FAnsi.AotTest
```

Replace `<RID>` with your platform runtime identifier:
- Linux: `linux-x64` or `linux-arm64`
- macOS: `osx-x64` or `osx-arm64`
- Windows: `win-x64` or `win-arm64`

Native AOT can only compile for the operating system it runs on, and needs that platform's native toolchain
(clang on Linux, the Xcode command line tools on macOS, the Visual Studio C++ tools on Windows).
The programs do not connect to any database, so no database servers are needed.

## Test Coverage

Each test verifies:
1. **Implementation Loading** - `{TypeName}.EnsureLoaded()`, after which the implementation's module initializer
   has registered it (see below)
2. **Server Object Creation** - `DiscoveredServer` instantiation, which fails if the implementation is not registered
3. **Connection String Builder** - Platform-specific builders
4. **Discovery Objects** - Database/Table/Column object creation
5. **DataTable Operations** - Bulk copy simulation
6. **Type Translation** - C# to SQL type mapping (SQL Server and comprehensive tests only)

## Registering Implementations

Each implementation registers itself from a module initializer when its assembly loads, under Native AOT as
under JIT. Nothing registers an implementation that your code never refers to, so call
`{TypeName}.EnsureLoaded()` (for example `MySqlImplementation.EnsureLoaded()`) before creating a
`DiscoveredServer` for that DBMS.

## Warnings

Every test project sets `TreatWarningsAsErrors`, so a publish fails on any trim or AOT warning that the native
compiler (ILC) reports in FAnsiSql or in the test program. ILC reports each warning in a project reference (the
FAnsiSql libraries) separately, but sums up the warnings in each package assembly as a single IL2104 (trimming)
or IL3053 (AOT) warning.

The SQL Server and Oracle drivers raise warnings that FAnsiSql cannot fix, so `SqlServer.AotTest`,
`Oracle.AotTest` and `FAnsi.AotTest` suppress these codes:

| Code | Raised for |
|------|------------|
| IL2104 | Microsoft.Data.SqlClient, Microsoft.Data.SqlClient.Internal.Logging, System.Configuration.ConfigurationManager, Oracle.ManagedDataAccess |
| IL3053 | Microsoft.Data.SqlClient, Oracle.ManagedDataAccess |
| IL3000 | Oracle.ManagedDataAccess, which reads `Assembly.Location` (always empty in a native binary); not used by `SqlServer.AotTest` |

Publish with `-p:TrimmerSingleWarn=false` to list the individual warnings. They are in driver features such as
diagnostic events, enclave attestation and JSON/vector types in SqlClient, and UDTs, JSON conversion, configuration
files and dynamic assembly loading in Oracle; test any of these you use under Native AOT.

`MySql.AotTest`, `PostgreSql.AotTest` and `Sqlite.AotTest` suppress nothing: MySqlConnector, Npgsql and
Microsoft.Data.Sqlite raise no warnings.

## CI Integration

The `Build, test, security, and AOT` workflow (`.github/workflows/dotnet-core.yml`):
1. Publishes each test project for linux-x64 and runs the native binary
2. Builds `FAnsi.AotTest` with `PublishAot=true` and checks the log for IL warnings

The build fails if a publish fails (including on any warning above that is not suppressed), if a program exits
with a non-zero code, or if the build reports an IL warning.

## AOT Compatibility Status

| Component | AOT Compatible | Notes |
|-----------|---------------|-------|
| FAnsi.Core | ✅ Fully Compatible | |
| FAnsi.MicrosoftSql | ⚠️ Partial | No warnings in FAnsiSql code; Microsoft.Data.SqlClient raises some (see Warnings) |
| FAnsi.MySql | ✅ Fully Compatible | |
| FAnsi.PostgreSql | ✅ Fully Compatible | |
| FAnsi.Oracle | ⚠️ Partial | No warnings in FAnsiSql code; Oracle.ManagedDataAccess raises some (see Warnings) |
| FAnsi.Sqlite | ✅ Fully Compatible | |

**Legend**:
- ✅ Fully Compatible - Compiles and runs with no issues
- ⚠️ Partial - Compiles but has runtime issues or warnings
- ❌ Not Compatible - Cannot compile or has blocking issues

## Future Work

1. **Add Connection Tests**: Test actual database connections (requires running DB services)
2. **Test Bulk Operations**: Verify bulk copy works in AOT
3. **Measure Performance**: Compare AOT vs JIT startup time and memory usage
4. **Test on All Platforms**: Verify Linux, macOS, and Windows AOT builds

## Contributing

When adding new features to FAnsiSql:
1. Ensure they're AOT-compatible (avoid dynamic code generation)
2. Add corresponding tests in this directory
3. Run `dotnet publish` with `-p:PublishAot=true` to verify
4. Check for IL warnings and document any that can't be resolved

## References

- [.NET Native AOT deployment](https://learn.microsoft.com/en-us/dotnet/core/deploying/native-aot/)
- [Prepare .NET libraries for trimming](https://learn.microsoft.com/en-us/dotnet/core/deploying/trimming/prepare-libraries-for-trimming)
- [Introduction to AOT warnings](https://learn.microsoft.com/en-us/dotnet/core/deploying/native-aot/fixing-warnings)
