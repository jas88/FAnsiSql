# CI Run 19472960191 - Complete Failure Analysis

## Executive Summary

**Total Unique Test Failures: 28**

The CI log showed 29 failure lines, but one was a duplicate - a truncated error message from `DiscoverRelationships_WithForeignKey_ReturnsRelationship(Sqlite)` that appeared as "to create relationship using SQL:ALTER TABLE".

The original analysis counted 28 failures but had the breakdown slightly wrong (undercounted SQLite by 2).

## Complete Breakdown

### Category 1: SQLite Failures (20 tests)

**DDL/Schema Operations (10 tests):**
1. `AddAndDropColumn_Sequence_Success(Sqlite)`
2. `AddColumn_DateTimeColumn_Success(Sqlite)`
3. `AddColumn_EmptyColumnName_ThrowsException(Sqlite)`
4. `AddColumn_InsertData_DropColumn_DataPreserved(Sqlite)`
5. `AddColumn_InvalidDataType_ThrowsException(Sqlite)`
6. `DropColumn_ExistingColumn_Success(Sqlite)`
7. `DropColumn_LastNonKeyColumn_Success(Sqlite)`
8. `DropColumn_NonExistentColumn_ThrowsException(Sqlite)`
9. `DropColumn_NonPrimaryKeyColumn_Success(Sqlite)`
10. `DropColumn_WithData_Success(Sqlite)`

**Index/Constraint Operations (4 tests):**
11. `CreateIndex_DuplicateIndexName_ThrowsException(Sqlite)`
12. `DropIndex_NonExistentIndex_ThrowsException(Sqlite)`
13. `HasPrimaryKey_AfterAddingPrimaryKey_ReturnsTrue(Sqlite)`
14. `DiscoverRelationships_WithForeignKey_ReturnsRelationship(Sqlite)` **FOREIGN KEY**
    - Appeared twice in logs (once as test name, once as truncated error message)
    - Truncated form: "to create relationship using SQL:ALTER TABLE"
    - Error: `SQLite Error 1: 'near "CONSTRAINT": syntax error'`
    - SQL attempted:
      ```sql
      ALTER TABLE [ChildRelTable]
      ADD CONSTRAINT FK_ChildRelTable_ParentRelTable
      FOREIGN KEY ([ParentId])
      REFERENCES [ParentRelTable]([Id])
      ```

**Discovery/Metadata (1 test):**
15. `DiscoverColumns_CorrectDataTypes(Sqlite)`

**Data Operations (5 tests):**
16. `Test_Calendar_Day(Sqlite)`
17. `UpdateWithJoin_DateTimeValues_UpdatesCorrectly(Sqlite)`
18. `Upload_DateTimeStrings_ParsedCorrectly(Sqlite)`
19. `Upload_DateTimeValues_PreservedCorrectly(Sqlite)`
20. `Upload_ReorderedColumns_MapsCorrectly(Sqlite)`

---

### Category 2: Decimal Validation Failures (4 tests, cross-platform)

All four major databases fail the same decimal range validation test:

1. `Upload_DecimalOutOfRange_ThrowsException(MicrosoftSQLServer)`
2. `Upload_DecimalOutOfRange_ThrowsException(MySql)`
3. `Upload_DecimalOutOfRange_ThrowsException(Oracle)`
4. `Upload_DecimalOutOfRange_ThrowsException(PostgreSql)`

**Issue:** Test expects exception when uploading decimal values outside the database's supported range, but the validation is not working correctly across all platforms.

---

### Category 3: SQL Server Specific Failures (2 tests)

1. `Drop_View_SuccessfullyDrops(MicrosoftSQLServer)` - **View drop issue**
2. `Upload_WithTransaction_CommitsProperly(MicrosoftSQLServer)` - **Transaction handling**

(Plus 1 decimal validation failure listed above = 3 total SQL Server failures)

---

### Category 4: PostgreSQL Specific Failures (1 test)

1. `UpdateWithJoin_EmptyUpdateTable_UpdatesNothing(PostgreSql)` - **UPDATE with JOIN on empty table**

(Plus 1 decimal validation failure listed above = 2 total PostgreSQL failures)

---

### Category 5: Oracle Specific Failures (1 test)

1. `GetAlterColumnToSql_PreservesData_AfterTypeChange(Oracle)` - **ALTER COLUMN type preservation**

(Plus 1 decimal validation failure listed above = 2 total Oracle failures)

---

### Category 6: MySQL Specific Failures (0 unique tests)

MySQL only has the decimal validation failure (listed in category 2).

Total MySQL failures: 1

---

## Summary by Database Engine

| Database     | Unique Failures | Decimal Failure | Total |
|--------------|----------------|-----------------|-------|
| SQLite       | 20             | N/A             | 20    |
| SQL Server   | 2              | 1               | 3     |
| PostgreSQL   | 1              | 1               | 2     |
| Oracle       | 1              | 1               | 2     |
| MySQL        | 0              | 1               | 1     |
| **TOTAL**    | **24**         | **4**           | **28**|

---

## What We Missed in Original Analysis

### Original Count Issues:

The original analysis stated:
- 18 SQLite failures → **Actually 20** (undercounted by 2)
- 4 Decimal validation → **Correct**
- 2 SQL Server transaction → **Actually 1 transaction + 1 view = 2 SQL Server specific**
- 1 Oracle ALTER COLUMN → **Correct**
- 1 SQL Server View → **Already counted in SQL Server specific above**
- 1 PostgreSQL UPDATE → **Correct**
- 1 SQLite foreign key → **Already counted in the 20 SQLite failures above**

### The "29th Failure" Mystery:

The grep command extracted 29 lines because the SQLite foreign key test appeared twice:
1. As the actual test name: `DiscoverRelationships_WithForeignKey_ReturnsRelationship(Sqlite)`
2. As a truncated error message: `to create relationship using SQL:ALTER TABLE`

These are the **same failure**, just different log lines.

### Corrected Breakdown:

- **SQLite:** 20 failures (including the foreign key constraint issue)
- **Decimal validation:** 4 failures (one per database: SQL Server, MySQL, Oracle, PostgreSQL)
- **SQL Server specific:** 2 additional failures (view drop + transaction handling)
- **PostgreSQL specific:** 1 additional failure (UPDATE with JOIN)
- **Oracle specific:** 1 additional failure (ALTER COLUMN)
- **MySQL specific:** 0 additional failures (only has the decimal validation)

**Total: 20 + 4 + 2 + 1 + 1 = 28 unique test failures**

---

## Files to Examine

Based on this analysis, the key areas to investigate are:

### 1. SQLite Implementation
- `/Users/jas88/Developer/Github/FAnsiSql/FAnsi.Implementations.Sqlite/` - All SQLite-specific code
- Focus on:
  - DDL operations (ALTER TABLE, ADD/DROP COLUMN)
  - Foreign key constraint syntax
  - Index operations
  - DateTime handling
  - Data upload/bulk insert

### 2. Decimal Validation (Cross-platform)
- `/Users/jas88/Developer/Github/FAnsiSql/FAnsi.Core/` - Core validation logic
- All database-specific implementations for decimal type handling

### 3. Database-Specific Issues
- **SQL Server:** View operations and transaction handling
- **PostgreSQL:** UPDATE with JOIN on empty tables
- **Oracle:** ALTER COLUMN with data preservation

---

## Priority Recommendations

1. **HIGH:** Fix SQLite implementation (20 failures = 71% of total failures)
2. **MEDIUM:** Fix decimal validation across all platforms (4 failures)
3. **LOW:** Fix individual database-specific issues (4 failures total)

The SQLite issues appear to be systematic and related to how the SQLite implementation handles DDL operations, particularly ALTER TABLE statements which SQLite has limited support for (often requiring table recreation).
