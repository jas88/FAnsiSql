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

### Comprehensive Test
- `FAnsi.AotTest/` - Tests all 4 database implementations in a single AOT binary

## Running Tests Locally

### Build and Run Individual Database Test
```bash
cd Tests/AotCompatibility/SqlServer.AotTest
dotnet publish -c Release -r <RID>
./bin/Release/net9.0/<RID>/publish/SqlServer.AotTest
```

### Build and Run Comprehensive Test
```bash
cd Tests/AotCompatibility/FAnsi.AotTest
dotnet publish -c Release -r <RID>
./bin/Release/net9.0/<RID>/publish/FAnsi.AotTest
```

Replace `<RID>` with your platform runtime identifier:
- Linux: `linux-x64` or `linux-arm64`
- macOS: `osx-x64` or `osx-arm64`
- Windows: `win-x64` or `win-arm64`

## Test Coverage

Each test verifies:
1. **Implementation Loading** - `ImplementationManager.Load<T>()`
2. **Server Object Creation** - `DiscoveredServer` instantiation
3. **Connection String Builder** - Platform-specific builders
4. **Discovery Objects** - Database/Table/Column object creation
5. **DataTable Operations** - Bulk copy simulation
6. **Type Translation** - C# to SQL type mapping

## Known Limitations

### Resource File Issue (Current Blocker)

**Status**: KNOWN ISSUE - AOT compilation succeeds, but runtime fails due to resource embedding

**Symptom**:
```
MissingManifestResourceException: Could not find the resource "FAnsi.FAnsiStrings.resources"
among the resources "FAnsi.Core.FAnsiStrings.resources"
```

**Root Cause**:
- The FAnsi.Core project embeds resources as "FAnsi.Core.FAnsiStrings.resources"
- The generated code tries to load "FAnsi.FAnsiStrings.resources"
- This mismatch is exposed by AOT's stricter resource handling

**Affected Code Paths**:
- Error messages in `ImplementationManager.GetImplementation()`
- Any code path that accesses `FAnsiStrings` resource strings

**Solution Required**:
This needs to be fixed in `FAnsi.Core/FAnsi.Core.csproj`:
1. Option 1: Update resource namespace to match (recommended)
2. Option 2: Add explicit rd.xml resource descriptor
3. Option 3: Replace resource strings with const strings

**Workaround**:
Tests can be made to pass by avoiding error paths that load resources, but this doesn't fully test the library.

### Driver-Specific Warnings

**Oracle.ManagedDataAccess.Core**:
- IL2026, IL2057, IL2075: Reflection warnings (expected, Oracle uses reflection internally)
- IL3050: Dynamic code warnings (JSON serialization, non-critical for most uses)

**Microsoft.Data.SqlClient**:
- IL2026: DiagnosticSource trimming warnings (telemetry features may be limited)
- IL2057: UDT type loading warnings (custom SQL types may not work)

**All Drivers**:
These warnings indicate features that may not work in AOT but don't affect core database operations:
- Custom user-defined types (UDTs)
- Some reflection-based features
- Dynamic JSON serialization
- Assembly plugin loading

## CI Integration

The GitHub Actions workflow (`/.github/workflows/aot-test.yml`) automatically:
1. Builds all AOT test projects for linux-x64
2. Runs each native binary against database services
3. Checks for AOT/trim warnings
4. Uploads binaries as artifacts

## AOT Compatibility Status

| Component | AOT Compatible | Notes |
|-----------|---------------|-------|
| FAnsi.Core | ⚠️ Partial | Resource file issue blocks runtime |
| FAnsi.MicrosoftSql | ⚠️ Partial | Blocked by Core issue; compiles successfully |
| FAnsi.MySql | ⚠️ Partial | Blocked by Core issue; compiles successfully |
| FAnsi.PostgreSql | ⚠️ Partial | Blocked by Core issue; compiles successfully |
| FAnsi.Oracle | ⚠️ Partial | Blocked by Core issue; compiles successfully |

**Legend**:
- ✅ Fully Compatible - Compiles and runs with no issues
- ⚠️ Partial - Compiles but has runtime issues or warnings
- ❌ Not Compatible - Cannot compile or has blocking issues

## Future Work

1. **Fix Resource File Issue**: Update FAnsi.Core resource embedding
2. **Add Connection Tests**: Test actual database connections (requires running DB services)
3. **Test Bulk Operations**: Verify bulk copy works in AOT
4. **Measure Performance**: Compare AOT vs JIT startup time and memory usage
5. **Test on All Platforms**: Verify Linux, macOS, and Windows AOT builds

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
