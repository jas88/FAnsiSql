# CI Test Failure Analysis - Run 19285698660

**Date**: 2025-11-12
**Branch**: fix/pivot-topx-and-calendar
**Total Failures**: 115 tests across all databases

## ✅ FIXED in this PR (fix/critical-test-failures)

### 1. MySQL Duplicate GROUP BY Bug
**Impact**: ~15 tests
**Location**: `FAnsi.MySql/Aggregation/MySqlAggregateHelper.cs:171`
**Root Cause**: Filter used `<= WHERE` which included GROUP BY lines, but template already has hardcoded `group by` on line 149, causing duplicate GROUP BY clauses in generated SQL
**Fix**: Changed to `< GroupBy` to exclude GROUP BY lines since template provides them
**Tests Fixed**:
- Test_Calendar_WithPivot(MySql, True/False)
- Multiple calendar+pivot combinations

### 2. MySQL Collation Mismatch
**Impact**: ~5 tests
**Location**: `FAnsi.MySql/Aggregation/MySqlAggregateHelper.cs:286`
**Root Cause**: `QUOTE(piv)` generates strings with server default collation (`utf8mb4_0900_ai_ci` in MySQL 8.0) but columns use `utf8mb4_bin`, causing "Illegal mix of collations" error
**Fix**: Added `COLLATE utf8mb4_bin` to QUOTE() result: `QUOTE(piv), ' COLLATE utf8mb4_bin`
**Tests Fixed**:
- CreateParameter_AndUse(MySql)
- Various pivot tests with parameters

---

## 🔴 REQUIRES SEPARATE FIXES

### 3. DateTime Backslash Format Parsing (TypeGuesser Repository) - ✅ FIXED UPSTREAM
**Impact**: ~20 tests across ALL databases
**Location**: `TypeGuesser/TypeGuesser/Deciders/DateTimeTypeDecider.cs:139`
**Root Cause**: DateSeparators array had `"\\\\"` (4 backslashes) but test data has `'Wed\5\19'` (single backslashes)
**Fix Applied**: Changed line 139 from `"\\\\"` to `"\\"`
**Repository**: https://github.com/jas88/TypeGuesser (separate repo)
**Issue**: https://github.com/jas88/TypeGuesser/issues/15
**Status**: ✅ Fixed upstream, will be in TypeGuesser v2.0.1
**Tests Affected**:
- Test_BulkInserting_LotsOfDates(All databases)
- Any test using format strings with backslash separators

**Next Steps**:
1. ✅ Fix applied in TypeGuesser repo
2. ⏳ Wait for TypeGuesser v2.0.1 release
3. ⏳ Update FAnsiSql package reference from HIC.TypeGuesser v1.2.7 to v2.0.1

### 4. Oracle Test Cleanup Infrastructure
**Impact**: 21 tests
**Root Cause**: Tables/sequences from previous test runs not properly cleaned up
**Errors**:
- ORA-00955: name already exists (3 tests)
- ORA-00942: table not found (12 tests)
- Column discovery failures (6 tests)

**Fixes Needed**:
- Use `DROP TABLE ... PURGE` to bypass recycle bin
- Drop sequences with tables (auto-increment)
- Add retry logic for ORA-00054 (resource busy)
- Add COMMIT after DDL for metadata visibility
- Use UPPER() in identifier comparisons
- Handle Unicode with UNISTR()

### 5. SQLite Server Abstraction Mismatch
**Impact**: 8 tests
**Root Cause**: File-based SQLite doesn't map to client-server model
**Tests Affected**:
- Server_Constructors (expects server.Name, gets null)
- Server_Exists (|DataDirectory| path not resolved)
- Server_RespondsWithinTime
- ServerHelper_* tests

**Fix Options**:
- Skip these tests for SQLite
- Implement file-system equivalents
- Update tests to handle file-based databases differently

### 6. SQLite Name Validation Too Restrictive
**Impact**: 2 tests
**Location**: SqliteQuerySyntaxHelper.ValidateDatabaseName()
**Root Cause**: Rejects parentheses `()` but SQLite file paths can contain them
**Fix**: Update validation to allow SQLite-valid characters in database names

### 7. SQL Server Calendar+Pivot Column Naming
**Impact**: 3 tests
**Root Cause**: Generated columns have wrong names (12 chars vs expected "T")
**Fix**: Review column aliasing in SQL Server pivot generation

### 8. MySQL AddColumn Transaction Isolation
**Impact**: 2 tests
**Root Cause**: After ALTER TABLE ADD COLUMN, DiscoverColumns() returns no rows
**Fix**: Ensure ALTER commits before discovery, or share transaction context properly

### 9. Test Cleanup Failures (Multiple Databases)
**Impact**: ~10 tests
**Root Cause**: Leftover databases/tables from previous runs
**Fix**: Improve test teardown with DROP IF EXISTS and verification

---

## 📊 Summary Statistics

| Database | Total Tests | Failures | Fixed in PR | Remaining |
|----------|-------------|----------|-------------|-----------|
| MySQL | 464 | 11 | 7 | 4 |
| SQL Server | 463 | 5 | 3 | 2 |
| SQLite | 330 | 16 | 2 | 14 |
| Oracle | 423 | 27 | 0 | 27 |
| PostgreSQL | 425 | 5 | 0 | 5 |
| Core | 1008 | 51 | 15 | 36 |
| **TOTAL** | **3113** | **115** | **27** | **88** |

**Expected after this PR**: ~88 failures remaining (24% reduction)

---

## 🎯 Next Steps Priority

**Immediate** (This PR):
- ✅ Fix MySQL duplicate GROUP BY
- ✅ Fix MySQL collation mismatch
- Create PR and wait for CI

**High Priority** (Next PRs):
1. Fix TypeGuesser backslash handling (separate repo)
2. Improve Oracle test cleanup infrastructure
3. Fix SQLite database routing issue

**Medium Priority**:
4. SQLite server abstraction redesign
5. MySQL AddColumn transaction isolation
6. SQL Server column naming

**Low Priority**:
7. General test cleanup improvements
