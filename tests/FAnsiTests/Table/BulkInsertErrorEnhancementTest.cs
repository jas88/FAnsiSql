using System.Data;
using System.Globalization;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

/// <summary>
///     Tests for enhanced error messages in bulk insert operations, specifically testing
///     the AOT-compatible column mapping error enhancement implementation.
/// </summary>
internal sealed class BulkInsertErrorEnhancementTest : DatabaseTests
{
    /// <summary>
    ///     Tests that string length violations produce enhanced error messages with source column names.
    ///     This is the primary use case for error enhancement - helping users identify which column
    ///     in their DataTable is causing the problem.
    /// </summary>
    public void TestBulkInsert_StringLengthViolation_EnhancedErrorMessage(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        // Create table with various column lengths to test sorting
        var tbl = db.CreateTable("ErrorEnhancementTest",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
            {
                IsAutoIncrement = true,
                IsPrimaryKey = true
            },
            new DatabaseColumnRequest("ZColumn", new DatabaseTypeRequest(typeof(string), 10)),
            new DatabaseColumnRequest("AColumn", new DatabaseTypeRequest(typeof(string), 5)),
            new DatabaseColumnRequest("MColumn", new DatabaseTypeRequest(typeof(string), 15)),
            new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof(int)))
        ]);

        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));

        using var dt = new DataTable();

        // Note: DataTable column order is different from database order
        // This tests that the mapping works correctly regardless of order
        dt.Columns.Add("age");
        dt.Columns.Add("zcolumn");
        dt.Columns.Add("acolumn");
        dt.Columns.Add("mcolumn");

        // Add good data
        dt.Rows.Add(30, "short", "ok", "medium");
        dt.Rows.Add(40, "tiny", "hi", "test");

        // Add bad data - violates AColumn length constraint
        dt.Rows.Add(50, "valid", "TOOLONG", "alsogood");

        // Add more good data after the bad row
        dt.Rows.Add(60, "good", "fine", "excellent");

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Timeout = 30;

        var ex = Assert.Catch(() => bulk.Upload(dt),
            "Expected upload to fail because 'TOOLONG' exceeds AColumn's 5 character limit");

        Assert.That(ex, Is.Not.Null);

        // For SQL Server, verify the enhanced error message contains the source column name
        if (type == DatabaseType.MicrosoftSQLServer)
            Assert.Multiple(() =>
            {
                // Should mention it's on row 3 (0-based = row 2, but error messages are 1-based)
                Assert.That(ex!.Message, Does.Contain("data row 3"),
                    "Error should identify the specific row number");

                // Should mention the source column name from the DataTable
                Assert.That(ex.Message, Does.Contain("acolumn"),
                    "Error should contain the source column name from DataTable");

                // Should mention destination column
                Assert.That(ex.Message, Does.Contain("AColumn"),
                    "Error should contain the destination column name");

                // Should mention the actual problematic value
                Assert.That(ex.Message, Does.Contain("TOOLONG"),
                    "Error should contain the actual value that caused the error");

                // Should mention the max length constraint
                Assert.That(ex.Message, Does.Contain("5"),
                    "Error should mention the MaxLength constraint");
            });

        tbl.Drop();
    }

    /// <summary>
    ///     Tests error enhancement with multiple columns to verify the sorting logic works correctly.
    ///     SQL Server internally sorts column mappings by destination column name, and our cache
    ///     must replicate this sorting to correctly map colid to column names.
    /// </summary>
    public void TestBulkInsert_MultipleColumns_SortingWorks(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        // Create table with columns in non-alphabetical order
        var tbl = db.CreateTable("SortingTest",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
            {
                IsAutoIncrement = true,
                IsPrimaryKey = true
            },
            new DatabaseColumnRequest("Zebra", new DatabaseTypeRequest(typeof(string), 5)),
            new DatabaseColumnRequest("Apple", new DatabaseTypeRequest(typeof(string), 5)),
            new DatabaseColumnRequest("Mango", new DatabaseTypeRequest(typeof(string), 5)),
            new DatabaseColumnRequest("Banana", new DatabaseTypeRequest(typeof(string), 5))
        ]);

        using var dt = new DataTable();

        // Add columns in a different order than the table
        dt.Columns.Add("zebra");
        dt.Columns.Add("mango");
        dt.Columns.Add("banana");
        dt.Columns.Add("apple");

        // Violate the Banana column constraint (should be sorted to colid 2 after Apple)
        dt.Rows.Add("ok", "ok", "WAYTOOLONG", "ok");

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Timeout = 30;

        var ex = Assert.Catch(() => bulk.Upload(dt),
            "Expected upload to fail due to Banana column length violation");

        Assert.That(ex, Is.Not.Null);

        if (type == DatabaseType.MicrosoftSQLServer)
            Assert.Multiple(() =>
            {
                // Should correctly identify the banana column despite the ordering
                Assert.That(ex!.Message, Does.Contain("banana").IgnoreCase,
                    "Error should contain source column name 'banana'");

                Assert.That(ex.Message, Does.Contain("Banana").IgnoreCase,
                    "Error should contain destination column name 'Banana'");
            });

        tbl.Drop();
    }

    /// <summary>
    ///     Tests that the error enhancement works with case-insensitive column name matching,
    ///     as SQL Server uses case-insensitive collation by default.
    /// </summary>
    public void TestBulkInsert_CaseInsensitiveMatching(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("CaseTest",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
            {
                IsAutoIncrement = true,
                IsPrimaryKey = true
            },
            new DatabaseColumnRequest("TestColumn", new DatabaseTypeRequest(typeof(string), 3))
        ]);

        using var dt = new DataTable();

        // Use different case for column name
        dt.Columns.Add("TESTCOLUMN");
        dt.Rows.Add("FAILURE"); // Too long

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Timeout = 30;

        var ex = Assert.Catch(() => bulk.Upload(dt),
            "Expected upload to fail due to length violation");

        Assert.That(ex, Is.Not.Null);

        if (type == DatabaseType.MicrosoftSQLServer)
            // Should still work despite case mismatch
            Assert.That(ex!.Message, Does.Contain("TESTCOLUMN").Or.Contain("TestColumn"),
                "Error should identify column despite case difference");

        tbl.Drop();
    }

    /// <summary>
    ///     Tests that error enhancement gracefully handles edge cases where the column
    ///     might not be found in the cache.
    /// </summary>
    public void TestBulkInsert_EdgeCase_EmptyTable(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("EdgeCaseTest",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
            {
                IsPrimaryKey = true,
                AllowNulls = false
            }
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id");

        // Try to insert null into non-nullable primary key
        dt.Rows.Add(DBNull.Value);

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Timeout = 30;

        // Should throw an exception (even if error enhancement doesn't apply to this error type)
        Assert.Throws(Is.InstanceOf<Exception>(), () => bulk.Upload(dt));

        tbl.Drop();
    }
}
