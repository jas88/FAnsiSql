# SQLite DateTime Test Failures Investigation

**Date:** 2025-11-18
**CI Run:** 19472960191
**Branch:** fix/move-tests-to-shared

## Executive Summary

Five SQLite tests fail because SQLite stores DateTime values as TEXT strings, but ADO.NET's `SqliteDataAdapter.Fill()` doesn't automatically parse ISO 8601 formatted strings back to DateTime objects. The values remain as strings when retrieved, causing type assertion failures.

## Failed Tests

### 1. Upload_DateTimeStrings_ParsedCorrectly(Sqlite)
**Error:** Expected `DateTime`, got `String`
```
Assert.That(result.Rows[0]["EventDate"], Is.InstanceOf<DateTime>())
  Expected: instance of <System.DateTime>
  But was:  <System.String>
```
**Location:** BulkCopyTests.cs:738

### 2. Upload_DateTimeValues_PreservedCorrectly(Sqlite)
**Error:** Unable to cast String to DateTime
```
System.InvalidCastException : Unable to cast object of type 'System.String' to type 'System.DateTime'.
```
**Location:** BulkCopyTests.cs:711

### 3. UpdateWithJoin_DateTimeValues_UpdatesCorrectly(Sqlite)
**Error:** Expected `DateTime`, got `String`
```
Assert.That(result, Is.InstanceOf<DateTime>())
  Expected: instance of <System.DateTime>
  But was:  <System.String>
```
**Location:** TableHelperUpdateTests.cs:653

### 4. DiscoverColumns_CorrectDataTypes(Sqlite)
**Error:** DataType.GetCSharpDataType() returns String instead of DateTime
```
Assert.That(dateCol!.DataType?.GetCSharpDataType(), Is.EqualTo(typeof(DateTime)))
  Expected: <System.DateTime>
  But was:  <System.String>
```
**Location:** TableHelperMetadataTests.cs:259

### 5. Test_Calendar_Day(Sqlite)
**Error:** Cannot find expected DateTime row (values returned as strings)
```
Did not find expected row:01/01/2001 00:00:00|4
Assert.That(dt.Rows.Cast<DataRow>().Any(r => IsMatch(r, cells)), Is.True)
  Expected: True
  But was:  False
```
**Location:** CalendarAggregationTests.cs:202

## Root Cause Analysis

### SQLite's Type System

SQLite uses a **type affinity** system rather than strict typing:
- `TEXT`: String data
- `INTEGER`: Signed integers
- `REAL`: Floating point
- `BLOB`: Binary data
- `NULL`: Null value

DateTime values are stored as TEXT in ISO 8601 format: `"YYYY-MM-DD HH:MM:SS.SSS"`

### Current Implementation

**SqliteTypeTranslater.cs:**
```csharp
protected override string GetDateDateTimeDataType() => "TEXT";
```

The type translator correctly identifies that DateTime should map to TEXT for storage. However, the reverse mapping has issues:

**TypeTranslater.cs (base class):**
```csharp
public Type? TryGetCSharpTypeForSQLDBType(string? sqlType)
{
    // ... other checks ...

    if (IsDate(sqlType))
        return typeof(DateTime);

    if (IsString(sqlType))
        return typeof(string);

    return null;
}
```

**The problem:** When SQLite returns column metadata, the SQL type is "TEXT", which matches `IsString()` before it can match `IsDate()`. The base `TypeTranslater.IsDate()` uses a regex:

```csharp
protected bool IsDate(string sqlType) => DateRegex.IsMatch(sqlType);
// DateRegex matches: "(date)|(datetime)|(timestamp)"
```

The pattern `"(date)|(datetime)|(timestamp)"` doesn't match "TEXT", so SQLite TEXT columns are classified as string, not DateTime.

### Data Flow

1. **Storage:** DateTime → TEXT (ISO 8601 string) ✓ Works
2. **Schema Discovery:** TEXT column → `GetCSharpDataType()` → returns `typeof(string)` ✗ Wrong
3. **Data Retrieval:** `SqliteDataAdapter.Fill()` → ADO.NET sees TEXT → returns string ✗ Wrong
4. **Test Assertion:** Expects DateTime, receives string ✗ Fails

## Current SQLite Type Detection

**SqliteTypeTranslater.cs** overrides these checks:
```csharp
protected override bool IsBool(string sqlType) =>
    base.IsBool(sqlType) || StringComparisonHelper.SqlTypesEqual(sqlType, "BOOLEAN");

protected override bool IsInt(string sqlType) =>
    base.IsInt(sqlType) || StringComparisonHelper.SqlTypesEqual(sqlType, "INTEGER");

protected override bool IsString(string sqlType) =>
    base.IsString(sqlType) ||
    StringComparisonHelper.SqlTypesEqual(sqlType, "TEXT") ||
    StringComparisonHelper.SqlTypesEqual(sqlType, "CLOB");

protected override bool IsFloatingPoint(string sqlType) =>
    base.IsFloatingPoint(sqlType) || StringComparisonHelper.SqlTypesEqual(sqlType, "REAL");
```

**Missing:** No override for `IsDate()` to detect TEXT columns that contain DateTime values.

## User Suggestion: ISO Format Parsing

The suggestion is to parse strings as DateTime if they match ISO 8601 format.

### Challenges

1. **Column Type Ambiguity:** SQLite TEXT columns can store ANY text data:
   - DateTime values: `"2024-01-15 10:30:45"`
   - Regular strings: `"Hello World"`
   - Dates as text: `"2024-01-15"`
   - No way to distinguish at schema level

2. **No Column-Level Metadata:** SQLite doesn't store semantic information about whether a TEXT column is meant for DateTime values.

3. **Performance:** Parsing every TEXT value to check if it's a valid DateTime would be expensive.

4. **False Positives:** Some strings might accidentally parse as dates:
   - `"2024-01-15"` could be a product code, not a date
   - `"12:30:45"` could be a time or a version number

### Where Detection Could Happen

#### Option 1: Override IsDate() in SqliteTypeTranslater
```csharp
protected override bool IsDate(string sqlType) =>
    base.IsDate(sqlType) ||
    StringComparisonHelper.SqlTypesEqual(sqlType, "TEXT");  // Too aggressive!
```
**Problem:** This would classify ALL TEXT columns as DateTime, breaking string columns.

#### Option 2: Runtime Value Parsing
Modify `SqliteDataAdapter` to parse ISO 8601 strings during `Fill()`.

**Problem:**
- Microsoft.Data.Sqlite is external (NuGet package)
- Cannot modify without forking/wrapping
- Would affect ALL TEXT columns, not just DateTime ones

#### Option 3: Post-Fill Conversion
Add conversion logic after `DataTable` is filled.

**Problem:**
- Requires knowing which columns should be DateTime
- No metadata to determine this from SQLite schema
- Tests use generic `GetDataTable()` method

## Breaking Change Risk

If we implement automatic ISO parsing:

### What Could Break

1. **String Columns with Date-Like Values:**
   ```sql
   CREATE TABLE Products (
       Code TEXT  -- "2024-Q1-SPECIAL" might be partially parsed
   );
   ```

2. **Mixed Content Columns:**
   ```sql
   CREATE TABLE Logs (
       Message TEXT  -- Sometimes dates, sometimes not
   );
   ```

3. **Performance Impact:**
   - Every TEXT value would need DateTime.TryParse() check
   - Could slow down large result sets significantly

4. **Existing Applications:**
   - Code expecting strings from TEXT columns would break
   - Type mismatches in existing queries

## Recommended Solutions

### Solution 1: Skip Tests (Lowest Risk)
Mark these five tests as `[Ignore]` or use `Assert.Ignore()` for SQLite.

**Pros:**
- No code changes
- No risk of breaking existing functionality
- Documents known limitation

**Cons:**
- Tests don't verify SQLite DateTime behavior
- Technical debt

**Implementation:**
```csharp
[TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
public void Upload_DateTimeValues_PreservedCorrectly(DatabaseType type)
{
    if (type == DatabaseType.Sqlite)
    {
        Assert.Ignore("SQLite stores DateTime as TEXT; ADO.NET returns strings, not DateTime objects. " +
                     "This is a known limitation of SQLite's type system.");
    }
    // ... rest of test
}
```

### Solution 2: Explicit Type Conversion in Tests (Medium Risk)
Modify tests to handle SQLite's TEXT-based DateTime storage.

**Pros:**
- Tests pass
- No changes to production code
- Tests become more realistic

**Cons:**
- Test code becomes database-specific
- Doesn't solve the underlying issue

**Implementation:**
```csharp
using var result = tbl.GetDataTable();
var eventDate = result.Rows[0]["EventDate"];

if (db.Server.DatabaseType == DatabaseType.Sqlite)
{
    // SQLite returns TEXT for DateTime columns
    Assert.That(eventDate, Is.InstanceOf<string>());
    var parsed = DateTime.Parse((string)eventDate, CultureInfo.InvariantCulture);
    Assert.That(parsed.ToString("yyyy-MM-dd HH:mm:ss"), Is.EqualTo(...));
}
else
{
    Assert.That(eventDate, Is.InstanceOf<DateTime>());
    Assert.That(((DateTime)eventDate).ToString("yyyy-MM-dd HH:mm:ss"), Is.EqualTo(...));
}
```

### Solution 3: Custom SqliteDataAdapter with Type Hints (High Risk)
Create a custom data adapter that can parse TEXT→DateTime based on column metadata hints.

**Pros:**
- Solves the root cause
- Tests pass without modification
- Improves SQLite DateTime support

**Cons:**
- Complex implementation
- Requires maintaining fork of SqliteDataAdapter
- Risk of false positives
- Performance overhead

**Implementation Outline:**
1. Create `FAnsiSqliteDataAdapter : SqliteDataAdapter`
2. Override `Fill()` method
3. Add metadata system to mark TEXT columns that should be DateTime
4. Parse values during fill based on metadata
5. Update `SqliteServerHelper.GetDataAdapter()` to return custom adapter

### Solution 4: Document as Known Limitation (No Risk)
Add documentation explaining SQLite's DateTime behavior.

**Pros:**
- No code changes
- Sets correct expectations
- Guides users to workarounds

**Cons:**
- Doesn't fix the issue
- Tests still fail

## Feasibility Assessment

| Solution | Feasibility | Risk | Effort | Recommendation |
|----------|------------|------|--------|----------------|
| Skip Tests | High | None | Low | **Recommended Short-term** |
| Test Conversion | High | Low | Medium | Acceptable Alternative |
| Custom Adapter | Medium | High | High | Not Recommended |
| Documentation | High | None | Low | Should Do Regardless |

## Recommendation

**Primary:** Use Solution 1 (Skip Tests) with Solution 4 (Documentation)

**Rationale:**
1. SQLite's TEXT-based DateTime storage is a fundamental limitation
2. Automatic parsing is too risky (false positives, performance, breaking changes)
3. The five failing tests verify behavior that doesn't align with SQLite's design
4. Other databases (SQL Server, MySQL, PostgreSQL, Oracle) all pass these tests
5. SQLite-specific DateTime handling should be opt-in, not automatic

**Implementation Steps:**
1. Add `Assert.Ignore()` for SQLite in the five failing tests
2. Document the limitation in:
   - `/Users/jas88/Developer/Github/FAnsiSql/FAnsi.Sqlite/SqliteTypeTranslater.cs` (class docs)
   - `/Users/jas88/Developer/Github/FAnsiSql/README.md` (if exists)
   - Test comments explaining the skip
3. Consider adding SQLite-specific tests that verify TEXT-based DateTime behavior

**Example Documentation:**
```csharp
/// <remarks>
/// <para>SQLite doesn't have dedicated date/time types. Dates can be stored as:</para>
/// <list type="bullet">
/// <item><description>TEXT as ISO8601 strings ("YYYY-MM-DD HH:MM:SS.SSS")</description></item>
/// <item><description>REAL as Julian day numbers</description></item>
/// <item><description>INTEGER as Unix Time (seconds since 1970-01-01 00:00:00 UTC)</description></item>
/// </list>
/// <para><strong>Important:</strong> When retrieving data via ADO.NET, DateTime values stored
/// as TEXT will be returned as <see cref="string"/> objects, not <see cref="DateTime"/>.
/// Applications must explicitly parse these strings if DateTime objects are needed.</para>
/// </remarks>
```

## Alternative: Accept Strings for SQLite DateTime

If we want SQLite support to work "naturally", we could:

1. Accept that SQLite DateTime columns return strings
2. Update FAnsi's type system to report `typeof(string)` for SQLite TEXT/DateTime columns
3. Document this as expected behavior
4. Provide helper methods for parsing

This would be consistent with SQLite's design philosophy but might surprise users coming from other databases.

## Conclusion

The five failing tests expose a fundamental incompatibility between:
- FAnsi's expectation of native DateTime types
- SQLite's TEXT-based DateTime storage

**Short-term:** Skip the tests with clear documentation
**Long-term:** Consider whether FAnsi should support database-specific type behaviors more explicitly

The user suggestion to parse ISO format strings is technically possible but carries significant risk of false positives and breaking changes. It should only be implemented if:
1. There's a way to explicitly mark TEXT columns as DateTime-intended
2. Parsing is opt-in rather than automatic
3. Performance impact is acceptable
4. The complexity is justified by user demand
