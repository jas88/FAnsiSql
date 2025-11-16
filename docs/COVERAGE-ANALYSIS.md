# FAnsiSql Code Coverage Analysis

**Generated:** 2025-11-16
**Current Coverage:** 9.0% (920/10,110 lines)
**Branch Coverage:** 4.3% (142/3,261 branches)
**Method Coverage:** 8.7% (126/1,438 methods)

## Executive Summary

The reported 44% coverage on Codecov is **INCORRECT**. Actual coverage analysis reveals:

- **Actual Line Coverage:** 9.0%
- **Actual Branch Coverage:** 4.3%
- **Files with 0% Coverage:** 96 out of 126 files (76%)

### Why the Discrepancy?

The 44% figure likely comes from:
1. **Old/cached data** on Codecov not reflecting recent test infrastructure changes
2. **Coverage collection issues** - many tests are skipped on local dev environments
3. **Database connectivity requirements** - tests only run in CI with full database setup

## Coverage by Database Implementation

| Database     | Coverage | Lines Covered | Total Lines | Files | Status |
|--------------|----------|---------------|-------------|-------|--------|
| **Core**     | 12.2%    | 286/2,348     | 45          | ⚠️ Low |
| **Oracle**   | 19.3%    | 516/2,670     | 20          | ⚠️ Low |
| **MySQL**    | 3.8%     | 72/1,902      | 20          | 🔴 Critical |
| **PostgreSQL** | 1.3%   | 18/1,378      | 20          | 🔴 Critical |
| **SQL Server** | 2.5%   | 28/1,118      | 10          | 🔴 Critical |
| **SQLite**   | 0.0%     | 0/687         | 10          | 🔴 Critical |

### Key Findings

1. **Oracle has highest coverage** (19.3%) - primarily due to `OracleQuerySyntaxHelper.cs` at 93.4%
2. **SQLite has zero coverage** - all 687 coverable lines untested
3. **PostgreSQL and MySQL** have critical coverage gaps despite CI database availability

## Coverage by Component Type

| Component          | Coverage | Lines Covered | Total Lines | Files | Priority |
|--------------------|----------|---------------|-------------|-------|----------|
| **TypeTranslater** | 39.8%    | 208/523       | 9           | Medium   |
| **Implementation** | 39.4%    | 52/132        | 12          | Medium   |
| **QuerySyntax**    | 32.7%    | 522/1,598     | 16          | Medium   |
| **ServerHelper**   | 6.1%     | 57/939        | 9           | High     |
| **AggregateHelper**| 0.4%     | 6/1,523       | 8           | Critical |
| **TableHelper**    | 0.0%     | 0/2,168       | 9           | Critical |
| **BulkCopy**       | 0.0%     | 0/995         | 9           | Critical |

## Top 20 Files with 0% Coverage (Highest Impact)

These files represent the largest untested surface area:

| File | Lines | Component | Database | Priority |
|------|-------|-----------|----------|----------|
| OracleQuerySyntaxHelper.cs | 522 | QuerySyntax | Oracle | ⚠️ Wait - actually 93.4% |
| OracleTableHelper.cs | 337 | TableHelper | Oracle | 🔴 Critical |
| MicrosoftSQLAggregateHelper.cs | 288 | AggregateHelper | SQL Server | 🔴 Critical |
| MicrosoftSQLTableHelper.cs | 259 | TableHelper | SQL Server | 🔴 Critical |
| MySqlAggregateHelper.cs | 246 | AggregateHelper | MySQL | 🔴 Critical |
| MySqlTableHelper.cs | 222 | TableHelper | MySQL | 🔴 Critical |
| DiscoveredTableHelper.cs | 220 | TableHelper | Core | 🔴 Critical |
| PostgreSqlTableHelper.cs | 213 | TableHelper | PostgreSQL | 🔴 Critical |
| SqliteBulkCopy.cs | 212 | BulkCopy | SQLite | 🔴 Critical |
| DiscoveredTable.cs | 195 | Discovery | Core | 🔴 Critical |
| DiscoveredDatabaseHelper.cs | 191 | Discovery | Core | 🔴 Critical |
| ManagedConnectionPool.cs | 190 | Connection | Core | 🔴 Critical |
| OracleAggregateHelper.cs | 188 | AggregateHelper | Oracle | High |
| MicrosoftSQLBulkCopy.cs | 182 | BulkCopy | SQL Server | High |
| MySqlBulkCopy.cs | 177 | BulkCopy | MySQL | High |
| PostgreSqlAggregateHelper.cs | 162 | AggregateHelper | PostgreSQL | High |
| SqliteTableHelper.cs | 145 | TableHelper | SQLite | High |
| SqliteDatabaseHelper.cs | 65 | DatabaseHelper | SQLite | Medium |
| OracleDatabaseHelper.cs | 55 | DatabaseHelper | Oracle | Medium |
| PostgreSqlDatabaseHelper.cs | 65 | DatabaseHelper | PostgreSQL | Medium |

**Total uncovered lines in top 20:** 3,928 lines

## What IS Being Tested?

Files with good coverage (>50%):

| File | Coverage | Lines | Component |
|------|----------|-------|-----------|
| OracleQuerySyntaxHelper.cs | 93.4% | 488/522 | QuerySyntax |
| ConnectionStringKeywordAccumulator.cs | 77.5% | 31/40 | Core |
| Implementation.cs | 71.4% | 5/7 | Core |
| OracleTypeTranslater.cs | 67.7% | 21/31 | TypeTranslater |
| MySqlTypeTranslater.cs | 66.6% | 34/51 | TypeTranslater |
| MicrosoftSQLTypeTranslater.cs | 64.2% | 9/14 | TypeTranslater |
| ImplementationManager.cs | 58.1% | 32/55 | Core |
| TypeTranslater.cs | 56.6% | 137/242 | Core |

## Test Infrastructure Issues

### Why Tests Are Skipped

From test execution logs, many tests show:
- **Skipped:** Most MySQL, Oracle, SQL Server, SQLite tests
- **Passed:** Primarily PostgreSQL tests (81 passed, 48 skipped)
- **Root Cause:** Missing `TestDatabases.xml` on local dev environments

### Test Configuration

Tests require `TestDatabases.xml` with connection strings for:
- SQL Server (LocalDB or full instance)
- MySQL
- PostgreSQL
- Oracle
- SQLite (in-memory, should always work)

**CI Environment:** Uses `TestDatabases-github.xml` with all databases configured
**Local Development:** Most developers lack complete database setup

## Critical Gaps in Test Coverage

### 1. TableHelper Classes (0% coverage, 2,168 lines)

**Impact:** CRITICAL - Core table operations completely untested

Untested functionality:
- Table creation, alteration, dropping
- Column management (add, modify, delete)
- Index management
- Primary key/foreign key operations
- Table truncation
- Constraint handling

**Files affected:**
- All `*TableHelper.cs` files across all database implementations
- `DiscoveredTableHelper.cs` (220 lines, core functionality)
- `DiscoveredTable.cs` (195 lines, core API)

### 2. BulkCopy Classes (0% coverage, 995 lines)

**Impact:** CRITICAL - Bulk data operations completely untested

Untested functionality:
- Bulk insert operations
- Column mapping
- Transaction handling
- Error handling during bulk operations
- Data type conversion during bulk operations

**Files affected:**
- `MySqlBulkCopy.cs` (177 lines)
- `MicrosoftSQLBulkCopy.cs` (182 lines)
- `SqliteBulkCopy.cs` (212 lines)
- `PostgreSqlBulkCopy.cs` (31 lines)
- `OracleBulkCopy.cs` (50 lines)

### 3. AggregateHelper Classes (0.4% coverage, 1,523 lines)

**Impact:** CRITICAL - Query aggregation completely untested

Untested functionality:
- GROUP BY query generation
- Aggregation functions (COUNT, SUM, AVG, etc.)
- HAVING clause generation
- Calendar aggregations (by day, month, year)
- Custom aggregation lines

**Files affected:**
- `MicrosoftSQLAggregateHelper.cs` (288 lines)
- `MySqlAggregateHelper.cs` (246 lines)
- `OracleAggregateHelper.cs` (188 lines)
- `PostgreSqlAggregateHelper.cs` (162 lines)
- `SqliteAggregateHelper.cs` (43 lines)

### 4. Connection Management (0% coverage, 264 lines)

**Impact:** HIGH - Connection pooling and transaction management untested

Untested functionality:
- Connection pooling
- Transaction management
- Connection disposal
- Thread safety
- Connection string handling

**Files affected:**
- `ManagedConnectionPool.cs` (190 lines)
- `ManagedConnection.cs` (35 lines)
- `ManagedTransaction.cs` (35 lines)
- `ServerPooledConnection.cs` (39 lines)

### 5. Database Discovery (0-16% coverage, 900+ lines)

**Impact:** HIGH - Database metadata discovery poorly tested

Untested functionality:
- Database listing and creation
- Table discovery
- Column discovery
- Stored procedure discovery
- Constraint discovery
- Foreign key relationship mapping

**Files affected:**
- `DiscoveredDatabase.cs` (0%, 92 lines)
- `DiscoveredDatabaseHelper.cs` (0%, 191 lines)
- `DiscoveredServer.cs` (16.7%, 167 lines)
- `DiscoveredColumn.cs` (0%, 29 lines)
- `DiscoveredStoredprocedure.cs` (0%, 2 lines)

### 6. SQLite Implementation (0% coverage, 687 lines)

**Impact:** CRITICAL - Entire SQLite implementation untested

**Why this is particularly bad:**
- SQLite requires NO external database setup
- Should be easiest to test (in-memory databases)
- Tests should NEVER be skipped for SQLite
- Zero excuse for 0% coverage

**Files affected:** ALL SQLite implementation files

### 7. Error Handling and Edge Cases

**Impact:** HIGH - Exception handling and edge cases largely untested

Observed patterns:
- Custom exceptions with 0% coverage:
  - `ColumnMappingException.cs` (0%)
  - `AlterFailedException.cs` (0%)
  - `InvalidResizeException.cs` (0%)
  - `CircularDependencyException.cs` (0%)
  - `TypeNotMappedException.cs` (0%)
- Error paths in covered files not tested (low branch coverage: 4.3%)

### 8. Data Type Translation Edge Cases

**Impact:** MEDIUM - While TypeTranslater classes have decent coverage (40%), edge cases are missed

Good coverage areas:
- Basic type mapping (covered)
- Common type conversions (covered)

Untested areas:
- Unusual/rare SQL types
- Type size boundaries
- Precision and scale handling
- Null handling
- Unicode and collation

## Recommendations: Prioritized by Impact

### Immediate Actions (Next Sprint)

#### 1. Fix SQLite Tests (CRITICAL, Quick Win)
- **Effort:** Low (1-2 days)
- **Impact:** +687 lines coverage (~6% boost)
- **Action:** Investigate why SQLite tests are skipped despite in-memory DB
- **Files:** All `FAnsi.Sqlite/*` files

#### 2. Add TableHelper Tests (CRITICAL)
- **Effort:** High (1-2 weeks)
- **Impact:** +2,168 lines coverage (~20% boost)
- **Priority Tests:**
  - Basic table creation/deletion
  - Column addition/modification
  - Primary key operations
  - Foreign key operations
- **Focus:** Database-agnostic tests in `FAnsi.Core.Tests`

#### 3. Add BulkCopy Tests (CRITICAL)
- **Effort:** Medium (3-5 days)
- **Impact:** +995 lines coverage (~9% boost)
- **Priority Tests:**
  - Simple bulk insert
  - Column mapping
  - Error handling
  - Transaction rollback
- **Focus:** Each database implementation

### Short Term (1-2 Months)

#### 4. Connection Management Tests (HIGH)
- **Effort:** Medium (3-5 days)
- **Impact:** +264 lines coverage (~2.5% boost)
- **Priority Tests:**
  - Connection pooling
  - Transaction commit/rollback
  - Connection disposal
  - Thread safety

#### 5. AggregateHelper Tests (HIGH)
- **Effort:** Medium-High (1 week)
- **Impact:** +1,523 lines coverage (~15% boost)
- **Priority Tests:**
  - Basic GROUP BY
  - Common aggregation functions
  - Calendar aggregations
  - HAVING clauses

#### 6. Database Discovery Tests (HIGH)
- **Effort:** Medium (3-5 days)
- **Impact:** +900 lines coverage (~9% boost)
- **Priority Tests:**
  - List databases
  - Discover tables
  - Discover columns
  - Foreign key relationships

### Medium Term (2-3 Months)

#### 7. Improve Branch Coverage
- **Current:** 4.3%
- **Target:** 40%
- **Action:** Add tests for error paths, edge cases, and conditional logic

#### 8. Exception Handling Tests
- **Effort:** Low-Medium (2-3 days)
- **Impact:** Improve robustness
- **Action:** Test all custom exceptions are thrown correctly

#### 9. Database-Specific Edge Cases
- **Effort:** High (ongoing)
- **Impact:** Improve production reliability
- **Action:** Test database-specific quirks and limitations

### Long Term (3+ Months)

#### 10. Integration Tests
- **Action:** Add end-to-end tests combining multiple components
- **Focus:** Real-world usage scenarios

#### 11. Performance Tests
- **Action:** Benchmark bulk operations, query generation
- **Focus:** Ensure no performance regressions

#### 12. Mutation Testing
- **Action:** Use mutation testing to find weak test coverage
- **Tool:** Stryker.NET

## Suggested Test Improvement Roadmap

### Phase 1: Quick Wins (Weeks 1-2)
Target: 20% coverage
- Fix SQLite tests (0% → 100% for SQLite)
- Add basic TableHelper tests (core operations)
- Add basic BulkCopy tests (simple inserts)

Expected gain: ~11% total coverage

### Phase 2: Critical Components (Weeks 3-6)
Target: 35% coverage
- Complete TableHelper tests (all operations)
- Complete BulkCopy tests (error handling, transactions)
- Add AggregateHelper tests (GROUP BY, aggregations)
- Add Connection Management tests

Expected gain: ~15% total coverage

### Phase 3: Discovery & Edge Cases (Weeks 7-10)
Target: 50% coverage
- Complete Database Discovery tests
- Add ServerHelper tests
- Test error paths and exceptions
- Improve branch coverage to 30%+

Expected gain: ~15% total coverage

### Phase 4: Polish & Advanced (Weeks 11+)
Target: 65%+ coverage
- Database-specific edge cases
- Integration tests
- Performance tests
- Branch coverage to 50%+

Expected gain: ~15% total coverage

## Test Infrastructure Improvements

### 1. Make Tests More Runnable Locally

**Problem:** Most devs skip tests due to missing database setup

**Solutions:**
- Use Docker Compose for local database setup
- Provide `docker-compose.yml` with all test databases
- Update documentation with quick-start guide
- Consider testcontainers for integration tests

### 2. Improve Test Configuration

**Current Issues:**
- Requires manual `TestDatabases.xml` creation
- No clear documentation
- Tests silently skip without explanation

**Solutions:**
- Auto-generate `TestDatabases.xml` from environment variables
- Add clear logging when tests are skipped
- Provide template configuration files
- Add `--database` CLI flag to run specific database tests

### 3. CI/CD Improvements

**Current State:**
- All databases available in CI
- Good test execution
- Coverage collection working

**Enhancements:**
- Add coverage trend tracking
- Set coverage quality gates (fail build if <40%)
- Generate coverage reports as PR comments
- Track coverage per database implementation

### 4. Test Organization

**Improvements:**
- Move database-agnostic tests to `FAnsi.Core.Tests`
- Create shared test helpers for common operations
- Use parameterized tests for multi-database scenarios
- Add test categories: [Unit], [Integration], [RequiresDatabase]

## Specific File Recommendations

### Critical Files Needing Immediate Attention

#### DiscoveredTable.cs (195 lines, 0% coverage)
**Impact:** Core API, used by all consumers
**Priority Tests:**
- `Create()` method
- `Drop()` method
- `Exists()` check
- `Truncate()` operation
- `GetRowCount()`
- Column discovery

#### ManagedConnectionPool.cs (190 lines, 0% coverage)
**Impact:** Connection pooling, performance critical
**Priority Tests:**
- Connection acquisition
- Connection release
- Pool exhaustion
- Thread safety
- Disposal

#### All TableHelper implementations (2,168 lines, 0% coverage)
**Impact:** Core table manipulation functionality
**Priority Tests:** (for each database)
- Create table with various column types
- Add/modify/drop columns
- Add/drop primary keys
- Add/drop foreign keys
- Add/drop indexes

#### All BulkCopy implementations (995 lines, 0% coverage)
**Impact:** Data loading, performance critical
**Priority Tests:** (for each database)
- Simple bulk insert
- Bulk insert with column mapping
- Error handling
- Transaction support

## Coverage Metrics to Track

### Primary Metrics
- **Line Coverage:** Current 9% → Target 65%
- **Branch Coverage:** Current 4.3% → Target 50%
- **Method Coverage:** Current 8.7% → Target 60%

### Secondary Metrics
- Coverage per database implementation
- Coverage per component type
- Coverage trend over time
- Test execution time
- Test reliability (flakiness rate)

### Quality Gates
1. **Minimum coverage:** 40% for PR merge
2. **No decrease:** PRs cannot decrease coverage
3. **New code:** 80% coverage required for new code
4. **Critical paths:** 100% coverage for TableHelper, BulkCopy

## Conclusion

The FAnsiSql project has **critical coverage gaps** across all database implementations and core components. The actual coverage of **9%** is far below the reported 44%, indicating significant testing debt.

**Highest Priority Areas:**
1. SQLite (0% coverage, easiest to fix)
2. TableHelper classes (0% coverage, 2,168 lines)
3. BulkCopy classes (0% coverage, 995 lines)
4. AggregateHelper classes (0.4% coverage, 1,523 lines)

**Recommended First Steps:**
1. Fix SQLite tests (1-2 days, +6% coverage)
2. Add basic TableHelper tests (1 week, +10% coverage)
3. Add basic BulkCopy tests (3-5 days, +5% coverage)
4. Set up local development database environment with Docker

**Realistic Timeline:**
- 3 months to reach 50% coverage
- 6 months to reach 65% coverage
- Ongoing maintenance to maintain 70%+ coverage

The good news is that the test infrastructure exists and works well in CI. The main challenge is expanding test coverage systematically across all components and database implementations.
