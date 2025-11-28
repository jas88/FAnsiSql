
# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [3.6.1] - 2025-11-27

### Changed
- **Npgsql 10.0 compatibility**
  - Updated Npgsql from 9.0.4 to 10.0.0
  - Added DateOnly/TimeOnly support for PostgreSQL date/time type mappings
  - Refactored PostgreSqlTypeTranslater to use FrozenDictionary for O(1) type lookups

- **Project modernization and .NET 10 support**
  - Updated target frameworks: Libraries now multi-target net8.0, net9.0, net10.0
  - Tests and tools now target net10.0
  - Reorganized project structure: libraries moved to `src/`, tests to `tests/`
  - Implemented central package management via Directory.Packages.props
  - Added global.json to specify .NET SDK 10.0.100
  - Updated CI workflows to use global.json for SDK versioning
  - Improved NuGet caching using Directory.Packages.props as cache key

### Added
- **DateOnly/TimeOnly support across all database implementations**
  - Added DateOnly/TimeOnly type support in base TypeTranslater class
  - All database backends now recognize and handle .NET 6+ date/time types

- **Modern .NET project structure**
  - Created src/Directory.Build.props for library multi-targeting (net8.0;net9.0;net10.0)
  - Created tests/Directory.Build.props for test projects (net10.0)
  - Target frameworks now managed centrally via Directory.Build.props files
  - Automated target framework updates via generate-build-props.sh script

### Fixed
- **Type mapping bugs in base TypeTranslater**
  - Fixed duplicate type checks: `typeof(short)` and `typeof(int)` appeared twice in conditionals
  - Fixed Test_Calendar_Day failure caused by Npgsql 10.0 returning DateOnly instead of DateTime
  - Updated test assertions to handle DateOnly/DateTime interoperability
  - Fixed string-to-DateTime comparison for SQLite cross-database tests
  - Fixed SQLite server name handling in connection string builder

### Removed
- **Deprecated dependencies and files**
  - Removed Microsoft.SourceLink.GitHub package (now integrated in .NET SDK)
  - Removed SharedAssemblyInfo.cs (replaced by centralized Directory.Build.props properties)
  - Removed hardcoded TargetFramework(s) from .csproj files (now inherited from Directory.Build.props)

## [3.6.0] - 2025-11-17

### Changed
- **Migrated to TypeGuesser v2.0.1** with improved performance and accuracy

### Added
- **Added 72 new test methods** (4,441 lines) significantly improving code coverage
- **Comprehensive test suites** for TableHelper, BulkCopy, ColumnHelper, Aggregation, and Update operations
- **Added SQLite DataAdapter support** for better data operations
- Added `coverage.runsettings` for XPlat code coverage
- Enhanced CI/CD workflows for better coverage reporting
- Added detailed coverage analysis documentation

### Fixed
- Fixed table name bug in AggregateHelper tests
- Fixed incorrect test assertions in aggregation tests
- Improved SQLite compatibility with proper skip conditions for unsupported features
- Fixed PRAGMA command handling in SQLite TableHelper
- Added culture-invariant operations throughout test suite
- Eliminated all CA1304/CA1305 culture-sensitive warnings

## [3.5.0] - 2025-11-04

### Performance
- **ImplementationManager optimization with O(1) FrozenDictionary lookups**
  - Replaced O(n) linear searches with O(1) FrozenDictionary for type-based lookups
  - Thread-safe registration with lock-protected volatile field pattern
  - Enhanced IImplementation interface with type properties for efficient lookups
  - Dramatically improved performance for implementation resolution operations

- **Zero-allocation type translation with ReadOnlySpan<char> optimizations**
  - Replaced inefficient string operations with ReadOnlySpan<char> in TypeTranslater
  - Eliminated string allocations in GetLengthIfString, IsUnicode, and type detection methods
  - Added zero-allocation ParseSizeFromType and ParseDecimalSize methods for numeric type parsing
  - Optimized MySQL type translator to use span-based ordinal comparisons
  - Maintained full backward compatibility while providing significant performance improvements

### Fixed
- **Platform-specific type translation overrides**
  - SQL Standard as default: uses "character varying(n)" for new platforms
  - Platform-specific overrides: SQL Server, MySQL, PostgreSQL return "varchar(n)"
  - All Test_CSharpToDbType_String10 tests now pass for all platforms
  - PostgreSQL GetStringDataTypeImpl override returns "varchar(n)" instead of "character varying(n)"
  - Architecture supports easy addition of new platform-specific overrides

- **CI configuration and compilation issues**
  - Fixed GitHub Actions workflow to use explicit solution file (FAnsi.sln)
  - Added missing GetServerHelper() implementations to Oracle, PostgreSQL, and MySQL
  - Added missing connectionType parameter to all provider constructors
  - Fixed null reference warning in DiscoveredServer assembly loading
  - Added proper AOT/trimming attributes (RequiresDynamicCode, RequiresUnreferencedCode)

- **Automatic provider registration via recursive assembly loading**
  - Static constructor in DiscoveredServer automatically loads all referenced FAnsiSql assemblies
  - Uses Assembly.GetReferencedAssemblies() and Assembly.Load() with recursive assembly discovery
  - Eliminates chicken-and-egg problem where implementations must be loaded before they can register
  - Maintains full backward compatibility with existing ModuleInitializers

### Test Results
- **100% test success rate**: 1,307 tests pass, 0 failures (previously 1,305 tests with 2 failures)
- **All platforms working**: SQL Server, MySQL, PostgreSQL, Oracle, SQLite tests all pass
- **Type translation tests**: All Test_CSharpToDbType_String10 variants pass for respective platforms
- **Performance validation**: All optimizations maintain thread-safety and compatibility

### Infrastructure
- **Removed obsolete source generation complexity**
  - Eliminated AutoInitializationGenerator.cs and GenericWrapperGenerator.cs
  - Simplified architecture with static constructor approach instead of source generators
  - Removed TestGenerator project files
  - Cleaner build process with fewer moving parts

## [3.4.0] - 2025-10-27

### Added
- **Standardized ModuleInitializer auto-registration across all RDBMS implementations**
  - All five implementations (SQL Server, MySQL, Oracle, PostgreSQL, SQLite) now use consistent ModuleInitializer pattern
  - Added `EnsureLoaded()` static method to each implementation for explicit assembly loading
  - No manual registration required - implementations auto-register when assembly loads
  - Removed static constructors in favor of ModuleInitializer for more reliable registration
  - AOT-compatible with proper CA2255 warning suppression

- **Optional thread-local connection pooling via feature flag**
  - New `FAnsiConfiguration.EnableThreadLocalConnectionPooling` flag (default: `false`)
  - When enabled, reduces connection count by up to 90% for SQL Server/MySQL in multi-database scenarios
  - SQL Server and MySQL use server-level pooling with automatic database switching
  - PostgreSQL uses database-level pooling (cannot switch databases)
  - Oracle and SQLite continue using ADO.NET native pooling
  - New `FAnsiConfiguration.ClearConnectionPools()` and `ClearAllConnectionPools()` methods
  - Comprehensive XML documentation with usage examples and warnings

### Changed
- **Updated ImplementationManager.Load<T>() obsolete message**
  - Now provides clear guidance: "call {TypeName}.EnsureLoaded() instead"
  - Example: `MicrosoftSQLImplementation.EnsureLoaded()`
  - Explains ModuleInitializer auto-registration behavior

### Improved
- **Refactored connection management for better reusability**
  - Added connection-parameter overloads to key helper methods
  - `TruncateTable()` now has overload accepting existing `DbConnection`
  - `CreateTable()` uses `GetManagedConnection()` for automatic pooling support
  - Pattern: `Method() { using var conn = GetManagedConnection(); return Method(conn); }`

## [3.3.4] - 2025-10-26

### Fixed
- **FAnsiSql.Legacy package dependency correction**
  - Fixed FAnsiSql.Legacy to correctly reference "FAnsiSql.Sqlite" instead of "HIC.FAnsi.Sqlite"
  - The v3.3.3 package was built before the PackageId was updated in FAnsi.Sqlite.csproj
  - No code changes required - ProjectReference automatically converts to correct PackageId on rebuild

## [3.3.3] - 2025-10-26

### Added
- **SQLite support** - Fifth database platform alongside SQL Server, MySQL, Oracle, and PostgreSQL
  - Full implementation of all core operations (table creation, bulk insert, queries, type translation)
  - Type affinity system for SQLite's flexible typing
  - In-memory database support for fast testing (Data Source=:memory:)
  - File-based persistent databases
  - Enhanced BulkCopy error reporting (matches SQL Server quality)
  - Auto-registration via ModuleInitializer
  - Comprehensive XML documentation (100% coverage)
  - 111+ cross-platform tests enabled
  - Platform limitations clearly documented (no ALTER COLUMN, no schemas, no PIVOT, etc)

### Fixed
- **MySQL PIVOT+TOP query syntax error** (Issue #38)
  - Fixed incorrect LIMIT placement inside ROW_NUMBER() OVER() window function clause
  - MySQL does not allow LIMIT inside window function OVER() clauses (Error 1064)
  - Moved LIMIT to correct position at CTE level, after GROUP BY and ORDER BY
  - Fixes 3 failing RDMP tests: GroupBy_PivotWithSum_HAVING_Top1_WHERE, GroupBy_PivotWithSum_Top2AlphabeticalAsc_WHEREStatement, GroupBy_PivotWithSum_Top2BasedonCountColumnDesc
  - Added 6 comprehensive test cases for PIVOT+TOP combinations
  - MySQL-only change, no impact on SQL Server/PostgreSQL/Oracle

### Changed
- Removed hardcoded version from FAnsi.Core.csproj (MinVer handles versioning automatically)

## [3.3.2] - 2025-10-24

### Fixed
- Fixed BulkCopy colid mapping to use table column order instead of alphabetical sorting
  - SQL Server assigns colid based on physical table order, not alphabetical
  - Added colid value to error messages for debugging clarity
  - Fixes TestBulkInsert_MultipleColumns_SortingWorks
- Temporarily disabled thread-local connection pooling
  - All database types now use ADO.NET native pooling
  - Prevents connection pool issues with dropped databases
- Updated ManagedConnection tests for disabled pooling
- Fixed 37 nullable reference warnings across all projects

### Removed
- Removed unused LINQ query provider implementation (~3000 lines)
  - Eliminates all IL2067, IL2072, IL3051 AOT warnings
  - Users should use Entity Framework Core for LINQ support
  - Build now has zero warnings

### Changed
- Build enforces zero warnings (TreatWarningsAsErrors=true)
- Fixed CRLF line endings in git hooks

### Fixed
- **SQL Server dangling transaction detection**
  - Fixed bug where `HasDanglingTransaction()` incorrectly returned `false` when the validation query itself failed due to a pending transaction
  - When `@@TRANCOUNT > 0`, SQL Server requires all commands to have the `.Transaction` property set, causing the validation query to throw an exception
  - Now correctly interprets this exception as proof of a dangling transaction and rejects the connection from the pool
  - Prevents "ExecuteReader requires the command to have a transaction when the connection assigned to the command is in a pending local transaction" errors from propagating to user code
  - Added test `Test_DanglingTransaction_IsDetectedAndRejected` to verify proper detection and rejection

### Features
- **Read-only IQueryable support for LINQ-to-SQL translation**
  - Added `DiscoveredTable.GetQueryable<T>()` for efficient server-side query execution
  - Translates LINQ expressions (Where, OrderBy, Take) to SQL automatically
  - No change tracking or update functionality - purely for read-only queries
  - Supports all database types: SQL Server, MySQL, PostgreSQL, Oracle
  - Uses existing connection pooling infrastructure
  - Example: `table.GetQueryable<Patient>().Where(p => p.Age > 18).OrderBy(p => p.Name).Take(100).ToList()`
  - Dramatically reduces data transfer for filtered queries

### Performance
- **Server-level connection pooling for SQL Server and MySQL** (up to 90% connection count reduction)
  - One connection per server per thread instead of per database
  - Automatic database switching using `USE` (SQL Server) or `ChangeDatabase()` (MySQL)
  - Dramatically reduces connection count in multi-database scenarios (e.g., 20 databases → 1 connection)
  - PostgreSQL continues using database-level pooling (cannot switch databases)
  - Oracle continues using ADO.NET native pooling (no thread-local pooling)
  - New `ServerPooledConnection` class tracks current database context
  - Maintains all existing safety features: dangling transaction detection, connection validation
- **Optimized table and view existence checks** (80-99% faster)
  - Added `DiscoveredTableHelper.Exists()` override in all database implementations
  - Changed from listing all tables and filtering in memory to direct SQL EXISTS queries
  - SQL Server: Uses `sys.objects` with schema awareness
  - MySQL: Uses `INFORMATION_SCHEMA.TABLES` with table_type filtering
  - PostgreSQL: Uses `pg_catalog.pg_class` with relkind filtering
  - Oracle: Uses `ALL_TABLES` and `ALL_VIEWS` with case-insensitive comparison
  - For databases with 1000+ tables: reduces check time from ~500ms to ~5ms
- **Optimized primary key existence checks** (90-99% faster)
  - Added `DiscoveredTableHelper.HasPrimaryKey()` method
  - Avoids discovering all columns just to check for primary key
  - Used in `MakeDistinct()` to skip processing tables that already have primary keys
  - SQL Server: Queries `sys.indexes` for primary key constraints
  - MySQL: Queries `INFORMATION_SCHEMA.TABLE_CONSTRAINTS`
  - PostgreSQL: Queries `pg_catalog.pg_constraint`
  - Oracle: Queries `ALL_CONSTRAINTS` for constraint_type = 'P'

## [3.3.1] - 2025-10-22

### Fixed
- Oracle: Added NOCACHE to IDENTITY columns to fix sequence allocation issues when mixing array-bound bulk inserts with regular inserts
- Connection pooling: Detect and reject pooled connections with dangling transactions (#30)
  - Fixes "ExecuteReader requires the command to have a transaction" errors when connections with uncommitted transactions are reused
  - Added database-specific transaction detection (SQL Server, MySQL, PostgreSQL)
  - Oracle connections now use ADO.NET's native pooling instead of thread-local pooling (no SQL-level transaction detection available)
  - Added developer warning when disposing pooled connections with dangling transactions

## [3.3.0] - 2025-10-21

### Breaking Changes
- **Package Rename**: Packages no longer use the `HIC.` prefix
  - Old: `HIC.FAnsiSql` → New: `FAnsiSql.Legacy` (transitional meta-package, see below)
  - Package IDs changed from `HIC.FAnsi.*` to `FAnsiSql.*` for modular packages
  - Assembly and product names updated to remove `HIC.` prefix
- **Repository Move**: Project moved from `HicServices/FAnsiSql` to `jas88/FAnsiSql`

### Added
- Thread-local connection pooling for `DiscoveredServer.GetManagedConnection()`
  - Eliminates ephemeral connection churn by maintaining one long-lived connection per thread per server
  - `DiscoveredServer.ClearCurrentThreadConnectionPool()` - Clear connections for current thread
  - `DiscoveredServer.ClearAllConnectionPools()` - Clear all pooled connections across all threads
- Pre-commit hooks and git configuration for improved development workflow
- Modernized aggregate helpers with improved pivot support

### Fixed
- MySQL aggregation issues with SET SESSION conflicts and pivot ordering (#23)

### Infrastructure
- Migrated to modular package structure with separate packages per DBMS (FAnsiSql.Core, FAnsiSql.MySql, FAnsiSql.MicrosoftSql, FAnsiSql.Oracle, FAnsiSql.PostgreSql)
- Added transitional meta-package `FAnsiSql.Legacy` that references all 4 DBMS implementations for easy migration from HIC.FAnsiSql
- Updated to .NET 9.0
- **AOT Compatibility**: Added AOT compatibility markers (`IsAotCompatible=true`)
  - Note: Oracle.ManagedDataAccess.Core is closed-source and has AOT limitations
  - Microsoft.Data.SqlClient has some reflection-based operations that may require runtime code generation
- Bump Microsoft.Data.SqlClient from 5.2.2 to 6.1.2
- Bump Npgsql from 8.0.5 to 9.0.4
- Bump Oracle.ManagedDataAccess.Core from 23.6.0 to 23.26.0
- Bump MySqlConnector from 2.4.0 to 2.4.0
- Bump System.Linq.Async from 6.0.1 to 6.0.3
- Bump actions/checkout from 4 to 5 (GitHub Actions)
- Bump actions/setup-dotnet from 4 to 5 (GitHub Actions)

## [3.2.7] - 2024-10-17

- Add Boolean syntax helpers
- Bump HIC.TypeGuesser from 1.2.6 to 1.2.7
- Bump Microsoft.Data.SqlClient from 5.2.1 to 5.2.2
- Bump Npsql from 8.0.3 to 8.0.5
- Bump Oracle.ManagedDataAccess.Core from 23.5.0 to 23.6.0

## [3.2.6] - 2024-07-16

- Listing databases on Postgres now respects connection string database and timeout
- Bump Oracle.ManagedDataAccess.Core from 23.4.0 to 23.5.0
- Bump HIC.TypeGuesser from 1.2.4 to 1.2.6

## [3.2.5] - 2024-06-07

- Bugfix for resource lifetime in ListDatabasesAsync, add unit tests

## [3.2.4] - 2024-06-05

- Add the ability to create and drop indexes from DiscoveredTables

## [3.2.3] - 2024-05-22

- Fix bug in PostgreSQL boolean handling (use booleans, not BIT)
- Fix bug in PostgreSQL where database listing failed if a database named 'postgres' was not present
- Make database enumeration an Enumerable not an array
- Bump MySqlConnector from 2.3.5 to 2.3.7
- Bump Npgsql from 8.0.2 to 8.0.3
- Bump Oracle.ManagedDataAccess.Core from 3.21.130 to 23.4.0
- Add System.Linq.Async 6.0.1

## [3.2.2] - 2024-03-13

- Enable custom timeout for bulk copy operations

## [3.2.1] - 2024-03-11

- Add Setter for DiscoveredDatabaseHelper Create Database Timeout

## [3.2.0] - 2024-03-04

- Target .Net 8.0
- Enable AOT compatibility, though upstream dependencies still have issues
- Nullable annotations enabled (some warnings remain)
- Bump HIC.TypeGuesser from 1.1.0 to 1.2.3
- Bump Microsoft.Data.SqlClient from 5.1.1 to 5.2.0
- Bump MySqlConnector from 2.2.6 to 2.3.5
- Bump Npgsql from 7.0.4 to 8.0.2
- Bump Oracle.ManagedDataAccess.Core from 3.21.100 to 3.21.130

## [3.1.1] - 2023-09-01

- Bugfix: MySQL text was erroneously capped at 64k (TEXT) instead of LONGTEXT (4GiB)
- Adjust timeout handling, use 10 seconds not 3 for server live tests since our Azure VM is slow
- Bump MySqlConnector from 2.2.6 to 2.2.7
- Bump Oracle.ManagedDataAccess.Core from 3.21.100 to 3.21.110

## [3.1.0] - 2023-05-15

- Now targeting .Net 6
- Single assembly build, more single-file and AOT friendly
- Oracle CI tests implemented, multiple bug fixes (stop mistaking Oracle LONG blob type for an integer value, wrap column names correctly, fix naming limits)
- Bump HIC.TypeGuesser from 1.0.3 to 1.1.0
- Bump Microsoft.Data.SqlClient from 5.0.1 to 5.1.1
- Bump MySqlConnector from 2.1.13 to 2.2.6
- Bump Npgsql from 6.0.7 to 7.0.4
- Bump Oracle.ManagedDataAccess.Core from 2.19.101 to 3.21.100
- Eliminate System.ComponentModel.Composition dependency

## [3.0.1] - 2022-10-28

### Fixed

- Fixed bug where passing empty string to `EnsureWrapped` would return wrapped empty string e.g. `[]`

## [3.0.0] - 2022-08-29

### Fixed

- Fixed bug with Aggregate graph in some specific versions of MySql

### Changed

- Switched to targeting net standard 2.1 (previously of 2.0)

## [2.0.5] - 2022-08-23

### Fixed

- Fixed returning length estimate of -1 for `ntext` datatype
- Fixed not sorting by date in Sql Server Calendar table with Pivot aggregate

## [2.0.4] - 2022-04-21

### Fixed

- Fixed parameter detection to pick up names immediately after commas

## [2.0.3] - 2022-02-22

### Changed

- `IntegratedSecurity` is now disabled when creating `SqlConnectionStringBuilder` instances where `Authentication` keyword is specified (Azure compatibility)
- Made SqlServer `SET SINGLE_USER [...]` SQL optional when issuing Drop Database.  Configure with `MicrosoftSQLDatabaseHelper.SetSingleUserWhenDroppingDatabases`.
- When sending `SET SINGLE_USER [...]` SQL during `DROP DATABASE` on an SqlServer fails, try the drop again without the SINGLE_USER statement.

### Dependencies

- Bump MySqlConnector from 2.1.5 to 2.1.6

## [2.0.2] - 2022-02-03

### Changed

- Bump Microsoft.Data.SqlClient from 3.0.1 to 4.1.0
- Bump Npgsql from 5.0.7 to 6.0.3
- Bump MySqlConnector from 1.3.11 to 2.1.5

## [2.0.1] - 2021-07-27

### Changed

- Upgraded Sql Server library from `System.Data.SqlClient` to `Microsoft.Data.SqlClient`
- Bump MySqlConnector from 1.3.9 to 1.3.11
- Bump Npgsql from 5.0.5 to 5.0.7

### Added

- Added `CreateDatabaseTimeoutInSeconds` static property to `DiscoveredServerHelper`


## [1.0.7] - 2021-05-18

### Added

- String parse errors in bulk insert now include the column name

### Changed

- Bump Oracle.ManagedDataAccess.Core from 2.19.100 to 2.19.101
- Bump Npgsql from 5.0.1.1 to 5.0.5
- Bump MySqlConnector from 1.2.1 to 1.3.9

## [1.0.6] - 2020-09-16

### Added

- Support for ExplicitDateFormats in CreateTable and BulkInsert

## [1.0.5] - 2020-08-13

### Added

- Updated IQuerySyntaxHelper to expose properties (OpenQualifier, CloseQualifier, DatabaseTableSeparator, IllegalNameChars)

### Fixed

- Fixed bug in repeated calls to GetRuntimeName and EnsureWrapped when a column/table name had escaped qualifiers (e.g. `[Hey]]There]`)

## [1.0.4] - 2020-08-10

### Fixed

- Fixed bug in CreateSchema (Sql Server) where a schema with the same name already exists.  This bug was introduced in 1.0.3 (only affects repeated calls).

## [1.0.3] - 2020-08-06

### Changed

- Updated MySqlConnector to 1.0.0

### Added

- Added `GetWrappedName` method for columns/tables for when a full expression is not allowed but wrapping is still needed.

## [1.0.2] - 2020-07-07

### Fixed

- Fixed Nuget package dependencies

## [1.0.1] - 2020-07-07

### Fixed

- Updated dependencies, fixing issue in uploading string typed big integers e.g. `"9223372036854775807"`
- Fixed table creation and column discovery when column/table names containing backticks and/or single quotes

## [0.11.0] - 2020-02-27

### Changed

- Changed client library from MySql.Data to [MySqlConnector](https://github.com/mysql-net/MySqlConnector)
  - If you have any connection strings with `Ssl-Mode` change it to `SSLMode` (i.e. remove the hyphen)
  - Update your package references (if any)

## [0.10.13] - 2019-11-25

### Fixed

- Fixed `MakeDistinct` for Sql Server not wrapping column names (Only causes problems when using columns with spaces / reserved words)

## [0.10.12] - 2019-11-19

### Fixed

- Fixed bug where `GetFullyQualifiedName` in MySql would not wrap the column name in quotes

## [0.10.11] - 2019-11-18

### Fixed

- Fixed bug reading `text` data types out of Postgres databases (would be read as invalid type `varchar(max)`)

## [0.10.10] - 2019-11-07

### Added

- Added `IQuerySyntaxHelper.SupportsEmbeddedParameters()` which returns whether or not the DBMS supports embedded SQL only parameters (e.g. `DECLARE @bob varchar(10)`).  In order to be qualify the DBMS must:
  - have a pure SQL only declaration format (i.e. not injected from outside)
  - support variable values canging during the query
  - not require mutilating the entire SQL query (e.g. with BEGIN / END ) blocks and indentation
  - not affect normal behaviour's such as SELECT returning result set from query

### Fixed

- AddColumn now works properly with dodgy column names (e.g. `"My Fun New Column[Lol]"`)
- MySql `GetConnectionStringBuilder` method no longer swallows platform exceptions around trusted security

## [0.10.9] - 2019-11-04

### Fixed

- Fixed Postgres escaped names (e.g `"MyCol"`) now properly strip `"` when calling `GetRuntimeName`


## [0.10.8] - 2019-11-04


### Added

- Support for Postgres DBMS

### Fixed

- Fixed Oracle `long` mapping (previously mapped to "bigint" now maps to "long")


## [0.10.7] - 2019-09-20

### Added

- Task cancellation support for various long running operations (e.g. CreatePrimaryKey)
- Added Schema creation method to `DiscoveredDatabase`

### Changed

- Sql Server `GetRowCount` no longer uses `sys.partitions` which is unreliable (now just runs `select count(*)` like other implementations)

### Fixed

- Fixed connection leaking when using `BeginNewTransactedConnection` in a `using` block without calling either `CommitAndCloseConnection` or `AbandonAndCloseConnection`

## [0.10.6] - 2019-09-16

### Fixed

- Fixed bug when calling SetDoNotReType multiple times on the same DataColumn

## [0.10.5] - 2019-09-16

### Added

- Added ability to control T/F Y/N interpretation (as either bit or varchar column)

### Changed

- Updated TypeGuesser to 0.0.4

## [0.10.4] - 2019-09-11

### Added
- Added extension method `DataColumn.DoNotReType()` which suppresses Type changes on a column (e.g. during CreateTable calls)

### Fixed
- Fixed bug where culture was set after evaluating DataColumn contents during CreateTable
- Trying to create / upload DataTables which have columns of type System.Object now results in NotSupportedException (previously caused unstable behaviour depending on what object Types were put in table)

## [0.10.3] - 2019-09-10

### Changed

- Updated TypeGuesser to 0.0.3 (improves performance and trims trailing zeros from decimals).

## [0.10.2] - 2019-09-05

### Added

- Foreign Key constraints can now be added to tables using new method `DiscoveredTable.AddForeignKey`

### Fixed

- Fixed bug in MySql where `DiscoveredTable.DiscoverRelationships(...)` could throw an ArgumentException ("same key has already been added [...]") in some circumstances

## [0.10.1] - 2019-09-05

### Fixed

- Fixed bug in bulk insert where the uploaded DataTable column Order (DataColumn.Ordinal) would change when creating Hard Typed columns out of untyped string columns.  This bug only manifested if you did operations based on column order on the DataTable after it had been inserted into the database succesfully.
- Fixed bug in DiscoveredTableValuedFunction that prevented dropping if they were not in the default schema "dbo"

## [0.10.0] - 2019-08-30

### Changed

- Type Guessing rules adjusted (and moved to [new repository TypeGuesser](https://github.com/HicServices/TypeGuesser))
  - Bit strings now include "Y", "N" "1" and "0".
  - Zeros after decimal point no longer prohibit guessing int (e.g. 1.00 is the now `int` instead of `decimal(3,2)`)
- DecimalSize class now uses `int` instead of nullable int (`int?`) for number of digits before/after decimal point.
- Table/column name suggester now allows unicode characters (now called `GetSensibleEntityNameFromString`)
- Attempting to resize a column to the same size it is currently is now ignored (previously `InvalidResizeException` was thrown)
- Added new Exception types (instead of generic .net Exceptions)
  - ColumnMappingException when insert / bulk insert fails to match input columns to destination table
  - TypeNotMappedException when there is a problem translating a C# Type to a proprietary SQL datatype (or vice versa)
- MakeDistinct on DiscoveredTable no longer throws an Exception if the table has a Primary Key (instead the method exits without doing anything)
- Reduced code duplication in AggregateHelper implementations by centralising code in new class AggregateCustomLineCollection

### Fixed

- Fixed support for Unicode in table/column names in Sql Server

## [0.9.8] - 2019-08-26

## Added
- Support for unicode text
- `DecimalTypeDecider` now recognises floating poing notation e.g. "-4.10235746055587E-05"

## [0.9.7] - 2019-08-20

## Added

- Added method `IsValidDatabaseName` (and table/column variants) to `QuerySyntaxHelper`.  This allows testing strings without try/catch

### Fixed

- Tables with invalid names e.g. `[mytbl.lol]][.lol.lol]` are no longer returned by `DiscoveredDatabase.DiscoverTables` (previously a `RuntimeNameException` was thrown)

## [0.9.6] - 2019-08-09

### Fixed

- Improved error messages in Sql Server for failed bulk insert
- Reduced MaximumDatabaseLength in Sql Server to 100 (previously 124) to allow for longer default log file suffixes


## [0.9.5] - 2019-08-08

### Added

- Added (DBMS specific) awareness of maximum table/database/column lengths into `IQuerySyntaxHelper`
- Create / Discover methods now validate the provided names before sending Sql to the DBMS (prevents attempts to create table names that are too long for the DBMS or entities containing periods or brackets)

### Fixed
- Oracle no longer truncates strings in GetRuntimeName to 30

## [0.9.4] - 2019-07-29

### Fixed
- Fixed bug creating Oracle tables from free text data containing extended ASCII / Unicode characters.

## [0.9.3] - 2019-07-19

### Added

- Oracle support for Basic and Calendar table aggregates

### Fixed

- DiscoveredTable.Rename now throws NotSupportedException for Views and TableValuedFunctions

## [0.9.2] - 2019-07-04

### Added

- Oracle DiscoverTables now supports return view option
- MySql DiscoverTables now supports return view option

### Removed

- FAnsi.csproj no longer depends on System.Data.SqlClient (dependency moved to FAnsi.Implementations.MicrosoftSQL)

### Fixed

- Fixed Oracle rename implementation
- Fixed DiscoverTables not correctly setting TableType for Views
- Fixed Drop table to work correctly with Views
- Exists now works correctly for Views (previously it would return true if there was no view but a table with the same name)

[Unreleased]: https://github.com/jas88/FAnsiSql/compare/v3.6.1...main
[3.6.1]: https://github.com/jas88/FAnsiSql/compare/v3.6.0...HEAD
[3.3.4]: https://github.com/jas88/FAnsiSql/compare/v3.3.3...v3.3.4
[3.3.3]: https://github.com/jas88/FAnsiSql/compare/v3.3.2...v3.3.3
[3.3.2]: https://github.com/jas88/FAnsiSql/compare/v3.3.1...v3.3.2
[3.3.1]: https://github.com/jas88/FAnsiSql/compare/v3.3.0...v3.3.1
[3.3.0]: https://github.com/jas88/FAnsiSql/compare/v3.2.7...v3.3.0
[3.2.7]: https://github.com/jas88/FAnsiSql/compare/v3.2.6...v3.2.7
[3.2.6]: https://github.com/jas88/FAnsiSql/compare/v2.3.5...v3.2.6
[3.2.5]: https://github.com/jas88/FAnsiSql/compare/v2.3.4...v3.2.5
[3.2.4]: https://github.com/jas88/FAnsiSql/compare/v3.2.3...v3.2.4
[3.2.3]: https://github.com/jas88/FAnsiSql/compare/v3.2.2...v3.2.3
[3.2.2]: https://github.com/jas88/FAnsiSql/compare/v3.2.1...v3.2.2
[3.2.1]: https://github.com/jas88/FAnsiSql/compare/v3.2.0...v3.2.1
[3.2.0]: https://github.com/jas88/FAnsiSql/compare/v3.1.1...v3.2.0
[3.1.1]: https://github.com/jas88/FAnsiSql/compare/v3.1.0...v3.1.1
[3.1.0]: https://github.com/jas88/FAnsiSql/compare/3.0.1...v3.1.0
[3.0.1]: https://github.com/jas88/FAnsiSql/compare/3.0.0...3.0.1
[3.0.0]: https://github.com/jas88/FAnsiSql/compare/2.0.5...3.0.0
[2.0.5]: https://github.com/jas88/FAnsiSql/compare/2.0.4...2.0.5
[2.0.4]: https://github.com/jas88/FAnsiSql/compare/2.0.3...2.0.4
[2.0.3]: https://github.com/jas88/FAnsiSql/compare/2.0.2...2.0.3
[2.0.2]: https://github.com/jas88/FAnsiSql/compare/2.0.1...2.0.2
[2.0.1]: https://github.com/jas88/FAnsiSql/compare/1.0.7...2.0.1
[1.0.7]: https://github.com/jas88/FAnsiSql/compare/1.0.6...1.0.7
[1.0.6]: https://github.com/jas88/FAnsiSql/compare/1.0.5...1.0.6
[1.0.5]: https://github.com/jas88/FAnsiSql/compare/1.0.4...1.0.5
[1.0.4]: https://github.com/jas88/FAnsiSql/compare/1.0.3...1.0.4
[1.0.3]: https://github.com/jas88/FAnsiSql/compare/1.0.2...1.0.3
[1.0.2]: https://github.com/jas88/FAnsiSql/compare/1.0.1...1.0.2
[1.0.1]: https://github.com/jas88/FAnsiSql/compare/0.11.0...1.0.1
[0.11.0]: https://github.com/jas88/FAnsiSql/compare/0.10.13...0.11.0
[0.10.13]: https://github.com/jas88/FAnsiSql/compare/0.10.12...0.10.13
[0.10.12]: https://github.com/jas88/FAnsiSql/compare/0.10.11...0.10.12
[0.10.11]: https://github.com/jas88/FAnsiSql/compare/0.10.10...0.10.11
[0.10.10]: https://github.com/jas88/FAnsiSql/compare/0.10.9...0.10.10
[0.10.9]: https://github.com/jas88/FAnsiSql/compare/0.10.8...0.10.9
[0.10.8]: https://github.com/jas88/FAnsiSql/compare/0.10.7...0.10.8
[0.10.7]: https://github.com/jas88/FAnsiSql/compare/0.10.6...0.10.7
[0.10.6]: https://github.com/jas88/FAnsiSql/compare/0.10.5...0.10.6
[0.10.5]: https://github.com/jas88/FAnsiSql/compare/0.10.4...0.10.5
[0.10.4]: https://github.com/jas88/FAnsiSql/compare/0.10.3...0.10.4
[0.10.3]: https://github.com/jas88/FAnsiSql/compare/0.10.2...0.10.3
[0.10.2]: https://github.com/jas88/FAnsiSql/compare/0.10.1...0.10.2
[0.10.1]: https://github.com/jas88/FAnsiSql/compare/0.10.0...0.10.1
[0.10.0]: https://github.com/jas88/FAnsiSql/compare/0.9.8...0.10.0
[0.9.8]: https://github.com/jas88/FAnsiSql/compare/0.9.7...0.9.8
[0.9.7]: https://github.com/jas88/FAnsiSql/compare/0.9.6...0.9.7
[0.9.6]: https://github.com/jas88/FAnsiSql/compare/0.9.5...0.9.6
[0.9.5]: https://github.com/jas88/FAnsiSql/compare/0.9.4...0.9.5
[0.9.4]: https://github.com/jas88/FAnsiSql/compare/0.9.3...0.9.4
[0.9.3]: https://github.com/jas88/FAnsiSql/compare/0.9.2...0.9.3
[0.9.2]: https://github.com/jas88/FAnsiSql/compare/v0.9.1.10...0.9.2
[3.6.0]: https://github.com/jas88/FAnsiSql/compare/v3.5.0...v3.6.0
[3.5.0]: https://github.com/jas88/FAnsiSql/compare/v3.4.0...v3.5.0
[3.4.0]: https://github.com/jas88/FAnsiSql/compare/v3.3.4...v3.4.0
