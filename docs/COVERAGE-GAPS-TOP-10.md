# FAnsiSql Top 10 Coverage Gaps

**Analysis Date:** 2025-01-16
**Branch:** feature/typeguesser-v2-migration
**Current Overall Coverage:** ~47% (before recent test additions)

## Executive Summary

Analysis reveals **~38,981 uncovered lines** across core components. Critical finding: Recent test additions (ColumnHelper: 19 tests, BulkCopy: 30+ tests, Update: 23 tests) show 0% coverage contribution, indicating a potential **coverage instrumentation issue** rather than missing tests.

## Top 10 Coverage Gaps (Ranked by Impact)

### #1 - TableHelper (All Databases)
- **Coverage:** 0.0%
- **Lines:** 8,376
- **Impact:** Critical
- **Description:** Core table operations (DROP, TRUNCATE, AddColumn, DropColumn, RenameColumn, GetTopX, indexes, primary keys)
- **Action:** Create comprehensive TableHelperTests.cs
- **Expected Gain:** +6,000-7,000 lines

### #2 - BulkCopy (All Databases)
- **Coverage:** 0.0%
- **Lines:** 4,422
- **Impact:** Critical
- **Description:** Bulk data loading operations
- **ISSUE:** BulkCopyTests.cs exists (953 lines, ~30 tests) but shows 0% coverage
- **Action:** **IMMEDIATE** - Investigate coverage instrumentation
- **Expected Gain:** +3,000-3,500 lines (if tests are executing)

### #3 - AggregateHelper (Non-MS Databases)
- **Coverage:** 0.4%
- **Lines:** 5,730
- **Impact:** High
- **Description:** SQL aggregation query generation (COUNT, AXIS, GROUP BY, HAVING, PIVOT)
- **ISSUE:** AggregationTests.cs has 30 tests but only 0.4% coverage
- **Action:** Expand tests to exercise actual SQL generation, not just constructors
- **Expected Gain:** +4,000-5,000 lines

### #4 - ServerHelper (Multiple Databases)
- **Coverage:** 5.9%
- **Lines:** 3,864
- **Impact:** High
- **Description:** Server-level operations (ListDatabases, CreateDatabase, DropDatabase, versioning)
- **Action:** Enhance ServerTests.cs
- **Expected Gain:** +3,000-3,500 lines

### #5 - DatabaseHelper (All Implementations)
- **Coverage:** 0.0%
- **Lines:** 3,354
- **Impact:** High
- **Description:** Database-level operations (table discovery, stored procedures, views, metadata)
- **Action:** Expand DatabaseLevelTests.cs and DiscoverTablesTests.cs
- **Expected Gain:** +2,500-3,000 lines

### #6 - QuerySyntaxHelper (Core & MS SQL)
- **Coverage:** 36.3% overall
- **Lines:** 5,790
- **Impact:** Medium
- **Description:** SQL syntax generation (identifier escaping, TOP/LIMIT, UPDATE FROM, CASE)
- **Note:** Oracle at 93.5% - use as reference
- **Action:** Enhance QuerySyntaxHelperDatabaseTests.cs
- **Expected Gain:** +2,000-3,000 lines

### #7 - Core Discovery Classes
- **Coverage:** 6.5%
- **Lines:** 7,740
- **Impact:** Medium
- **Description:** Core discovery API wrappers (DiscoveredTable, ManagedConnectionPool, DiscoveredDatabase)
- **Action:** Create integration tests for Discovery API end-to-end workflows
- **Expected Gain:** +3,000-4,000 lines

### #8 - TypeTranslater (MySQL & Others)
- **Coverage:** 39.0%
- **Lines:** 2,424
- **Impact:** Medium
- **Description:** Type translation between .NET and database-specific types
- **Action:** Expand TypeTranslaterTests.cs for edge cases
- **Expected Gain:** +1,000-1,500 lines

### #9 - UpdateHelper (All Databases)
- **Coverage:** 4.6%
- **Lines:** 612
- **Impact:** Medium
- **Description:** UPDATE statement generation with JOIN syntax
- **ISSUE:** UpdateTests.cs has 23 tests but only 4.6% coverage
- **Action:** Review existing tests for gaps, expand coverage
- **Expected Gain:** +400-500 lines

### #10 - ColumnHelper (All Databases)
- **Coverage:** 0.0%
- **Lines:** 498
- **Impact:** Medium
- **Description:** Column-level operations and metadata discovery
- **ISSUE:** ColumnHelperTests.cs exists (19 tests) but shows 0% coverage
- **Action:** **IMMEDIATE** - Investigate coverage instrumentation (same as BulkCopy)
- **Expected Gain:** +350-400 lines (if tests are executing)

## Summary Statistics

| Category | Total Lines | Current Coverage | Uncovered Lines | Priority |
|----------|-------------|------------------|-----------------|----------|
| **Top 10 Total** | **~42,160** | **5.3%** | **~38,981** | - |
| Critical (1-2) | 12,798 | 0.0% | 12,798 | ⚠️ Immediate |
| High (3-5) | 12,948 | 1.2% | 12,793 | 🔴 High |
| Medium (6-10) | 16,414 | 20.6% | 13,390 | 🟡 Medium |

## Immediate Actions Required

### 1. Investigate Coverage Instrumentation (CRITICAL)
Three test files show 0% coverage despite having comprehensive tests:
- `BulkCopyTests.cs` (30 tests, 953 lines)
- `ColumnHelperTests.cs` (19 tests, 805 lines)
- `UpdateTests.cs` (23 tests, 1,643 lines) - showing only 4.6%

**Potential Issues:**
- Tests not executing in CI pipeline
- Coverage.runsettings misconfiguration
- Test project reference issues
- Instrumentation not covering implementation assemblies

**Expected Impact:** If resolved, could add +4,920 lines of coverage immediately.

### 2. Create TableHelper Test Suite (HIGH PRIORITY)
- Largest gap: 8,376 uncovered lines
- No existing tests found
- Core DDL functionality critical to library
- Expected gain: +6,000-7,000 lines

### 3. Enhance AggregateHelper Tests (HIGH PRIORITY)
- 30 tests exist but only hitting 0.4%
- Tests likely only hitting constructors
- Need to test actual SQL generation
- Expected gain: +4,000-5,000 lines

## Projected Coverage Improvement

**Current:** ~47% overall (61,000 lines hit)
**After Instrumentation Fix + TableHelper Tests:** ~60-65% overall (~79,000-81,000 lines hit)
**Gain:** +18,000-20,000 lines covered

## Recommended Priorities

1. **IMMEDIATE:** Debug coverage instrumentation for existing tests
2. **Sprint 1:** Create TableHelper comprehensive test suite
3. **Sprint 2:** Fix AggregateHelper test effectiveness
4. **Sprint 3:** Expand ServerHelper and DatabaseHelper tests
5. **Ongoing:** Improve QuerySyntaxHelper and Discovery API coverage
