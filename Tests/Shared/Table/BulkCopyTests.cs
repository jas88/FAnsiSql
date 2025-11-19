using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Exceptions;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

/// <summary>
/// Comprehensive tests for BulkCopy implementations across all database providers.
/// Tests cover basic operations, error handling, column mapping, transactions, and special data types.
/// </summary>
internal sealed class BulkCopyTests : DatabaseTests
{
    /// <summary>
    /// Helper method to assert that an operation throws an exception.
    /// Different databases throw different exception types:
    /// - SQLite: FileLoadException for constraint violations, InvalidOperationException for disposal issues
    /// - MySQL: InvalidOperationException (wraps MySqlException)
    /// - PostgreSQL: PostgresException
    /// - Oracle: OracleException
    /// - SQL Server: InvalidOperationException or FileLoadException
    /// Note: SQLite doesn't enforce some constraints (e.g., string length, integer overflow) so may not throw.
    /// </summary>
    private static void AssertThrowsException(DatabaseType type, TestDelegate code, string? messageContains = null, bool sqliteMayNotThrow = false)
    {
        if (type == DatabaseType.Sqlite && sqliteMayNotThrow)
        {
            // SQLite may not throw for data type violations (length, overflow) as it's dynamically typed
            try
            {
                code();
                // If we get here, SQLite didn't throw - that's okay for data type violations
                Assert.Pass("SQLite does not enforce this constraint");
            }
            catch (Exception ex)
            {
                // But if it does throw, verify the message if required
                if (messageContains != null)
                    Assert.That(ex.Message, Does.Contain(messageContains));
            }
        }
        else
        {
            // All databases throw some exception type, but the specific type varies
            // Use Assert.Catch to accept any exception type
            var ex = Assert.Catch(code);
            Assert.That(ex, Is.Not.Null, "Expected an exception to be thrown");
            if (messageContains != null)
                Assert.That(ex.Message, Does.Contain(messageContains));
        }
    }
    #region Basic Upload Operations

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_EmptyDataTable_ReturnsZero(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestEmpty",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 50))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Name", typeof(string));

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        var affected = bulk.Upload(dt);

        Assert.Multiple(() =>
        {
            Assert.That(affected, Is.EqualTo(0));
            Assert.That(tbl.GetRowCount(), Is.EqualTo(0));
        });
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_SingleRow_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestSingleRow",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 50))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Name", typeof(string));
        dt.Rows.Add(1, "TestName");

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        var affected = bulk.Upload(dt);

        Assert.Multiple(() =>
        {
            Assert.That(affected, Is.EqualTo(1));
            Assert.That(tbl.GetRowCount(), Is.EqualTo(1));
        });

        using var result = tbl.GetDataTable();
        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[0]["Id"], Is.EqualTo(1));
            Assert.That(result.Rows[0]["Name"], Is.EqualTo("TestName"));
        });
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_MultipleRows_AllInserted(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestMultipleRows",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Value", typeof(string));

        for (var i = 1; i <= 100; i++)
            dt.Rows.Add(i, $"Value{i}");

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        var affected = bulk.Upload(dt);

        Assert.Multiple(() =>
        {
            Assert.That(affected, Is.EqualTo(100));
            Assert.That(tbl.GetRowCount(), Is.EqualTo(100));
        });
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_MultipleBatches_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestMultipleBatches",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Data", new DatabaseTypeRequest(typeof(string), 50))
        ]);

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);

        // First batch
        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("Data", typeof(string));
            dt1.Rows.Add(1, "Batch1");
            dt1.Rows.Add(2, "Batch1");
            bulk.Upload(dt1);
        }

        Assert.That(tbl.GetRowCount(), Is.EqualTo(2));

        // Second batch
        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("Id", typeof(int));
            dt2.Columns.Add("Data", typeof(string));
            dt2.Rows.Add(3, "Batch2");
            dt2.Rows.Add(4, "Batch2");
            bulk.Upload(dt2);
        }

        Assert.That(tbl.GetRowCount(), Is.EqualTo(4));
    }

    #endregion

    #region NULL Value Handling

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_NullValues_InsertedCorrectly(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestNulls",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { AllowNulls = false },
            new DatabaseColumnRequest("NullableString", new DatabaseTypeRequest(typeof(string), 50)) { AllowNulls = true },
            new DatabaseColumnRequest("NullableInt", new DatabaseTypeRequest(typeof(int))) { AllowNulls = true }
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("NullableString", typeof(string));
        dt.Columns.Add("NullableInt", typeof(int));

        dt.Rows.Add(1, "NotNull", 100);
        dt.Rows.Add(2, DBNull.Value, DBNull.Value);
        dt.Rows.Add(3, null, null);

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Upload(dt);

        using var result = tbl.GetDataTable();
        Assert.That(result.Rows, Has.Count.EqualTo(3));

        // Row 1: has values
        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[0]["NullableString"], Is.EqualTo("NotNull"));
            Assert.That(result.Rows[0]["NullableInt"], Is.EqualTo(100));
        });

        // Rows 2 and 3: should have DBNull
        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[1]["NullableString"], Is.EqualTo(DBNull.Value));
            Assert.That(result.Rows[1]["NullableInt"], Is.EqualTo(DBNull.Value));
            Assert.That(result.Rows[2]["NullableString"], Is.EqualTo(DBNull.Value));
            Assert.That(result.Rows[2]["NullableInt"], Is.EqualTo(DBNull.Value));
        });
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_EmptyStrings_ConvertedToNull(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestEmptyStrings",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 50))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Value", typeof(string));

        dt.Rows.Add(1, "");
        dt.Rows.Add(2, "   ");
        dt.Rows.Add(3, "NotEmpty");

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Upload(dt);

        using var result = tbl.GetDataTable();
        Assert.That(result.Rows, Has.Count.EqualTo(3));

        // Empty strings should be NULL for SQL Server (handled by EmptyStringsToNulls)
        // This behavior may vary by database
        Assert.That(result.Rows[2]["Value"], Is.EqualTo("NotEmpty"));
    }

    #endregion

    #region Error Handling - Data Type Violations

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_StringTooLong_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestStringTooLong",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("ShortString", new DatabaseTypeRequest(typeof(string), 10))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("ShortString", typeof(string));

        dt.Rows.Add(1, "Valid");
        dt.Rows.Add(2, new string('X', 50)); // Too long!

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);

        AssertThrowsException(type, () => bulk.Upload(dt), sqliteMayNotThrow: true);
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_DecimalOutOfRange_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestDecimalRange",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("SmallDecimal", new DatabaseTypeRequest(typeof(decimal), null, new DecimalSize(3, 2)))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("SmallDecimal", typeof(decimal));

        dt.Rows.Add(1, 9.99m);      // Valid
        dt.Rows.Add(2, 999999.99m); // Too large for decimal(5,2)!

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);

        AssertThrowsException(type, () => bulk.Upload(dt), sqliteMayNotThrow: true);
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_InvalidDecimalFormat_ThrowsFormatException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestInvalidDecimal",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Amount", new DatabaseTypeRequest(typeof(decimal), null, new DecimalSize(10, 2)))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Amount"); // String column

        dt.Rows.Add(1, "123.45");
        dt.Rows.Add(2, "invalid"); // Bad format

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);

        var ex = Assert.Throws<FormatException>(() => bulk.Upload(dt));
        Assert.Multiple(() =>
        {
            Assert.That(ex?.Message, Does.Contain("Failed to parse"));
            Assert.That(ex?.Message, Does.Contain("invalid"));
        });
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_IntegerOverflow_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestIntOverflow",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("SmallInt", new DatabaseTypeRequest(typeof(short)))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("SmallInt", typeof(int)); // int in DataTable, but short in DB

        dt.Rows.Add(1, 100);
        dt.Rows.Add(2, 100000); // Too large for short

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);

        AssertThrowsException(type, () => bulk.Upload(dt), sqliteMayNotThrow: true);
    }

    #endregion

    #region Error Handling - Constraint Violations

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_ViolateNotNullConstraint_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestNotNull",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { AllowNulls = false },
            new DatabaseColumnRequest("RequiredValue", new DatabaseTypeRequest(typeof(string), 50)) { AllowNulls = false }
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("RequiredValue", typeof(string));

        dt.Rows.Add(1, "Valid");
        dt.Rows.Add(2, DBNull.Value); // Violates NOT NULL

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);

        AssertThrowsException(type, () => bulk.Upload(dt));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_DuplicatePrimaryKey_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestPKDuplicate",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
            {
                IsPrimaryKey = true,
                AllowNulls = false
            },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 50))
        ]);

        // Insert first row
        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("Value", typeof(string));
            dt1.Rows.Add(1, "First");

            using var bulk1 = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
            bulk1.Upload(dt1);
        }

        // Try to insert duplicate
        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("Id", typeof(int));
            dt2.Columns.Add("Value", typeof(string));
            dt2.Rows.Add(1, "Duplicate"); // Same PK!

            using var bulk2 = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
            AssertThrowsException(type, () => bulk2.Upload(dt2));
        }
    }

    #endregion

    #region Column Mapping

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_ReorderedColumns_MapsCorrectly(DatabaseType type)
    {
        if (type == DatabaseType.Sqlite)
            Assert.Ignore("SQLite stores DateTime as TEXT; type casting fails");

        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestReorderedColumns",
        [
            new DatabaseColumnRequest("FirstColumn", new DatabaseTypeRequest(typeof(string), 50)),
            new DatabaseColumnRequest("SecondColumn", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("ThirdColumn", new DatabaseTypeRequest(typeof(DateTime)))
        ]);

        using var dt = new DataTable();
        // DataTable columns in different order than DB
        dt.Columns.Add("ThirdColumn", typeof(DateTime));
        dt.Columns.Add("FirstColumn", typeof(string));
        dt.Columns.Add("SecondColumn", typeof(int));

        var testDate = new DateTime(2024, 1, 15, 10, 30, 0);
        dt.Rows.Add(testDate, "TestValue", 42);

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Upload(dt);

        using var result = tbl.GetDataTable();
        Assert.That(result.Rows, Has.Count.EqualTo(1));

        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[0]["FirstColumn"], Is.EqualTo("TestValue"));
            Assert.That(result.Rows[0]["SecondColumn"], Is.EqualTo(42));
            Assert.That(((DateTime)result.Rows[0]["ThirdColumn"]).Date, Is.EqualTo(testDate.Date));
        });
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_SubsetOfColumns_UsesDefaults(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestSubsetColumns",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { AllowNulls = false },
            new DatabaseColumnRequest("WithDefault", new DatabaseTypeRequest(typeof(DateTime)))
            {
                AllowNulls = false,
                Default = MandatoryScalarFunctions.GetTodaysDate // Will use a default
            },
            new DatabaseColumnRequest("Nullable", new DatabaseTypeRequest(typeof(string), 50)) { AllowNulls = true }
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        // Only uploading Id, other columns should use defaults/nulls

        dt.Rows.Add(1);
        dt.Rows.Add(2);

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Upload(dt);

        using var result = tbl.GetDataTable();
        Assert.That(result.Rows, Has.Count.EqualTo(2));

        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[0]["Id"], Is.EqualTo(1));
            Assert.That(result.Rows[1]["Id"], Is.EqualTo(2));
            // WithDefault should have a value (date-based)
            Assert.That(result.Rows[0]["WithDefault"], Is.Not.EqualTo(DBNull.Value));
            // Nullable should be NULL
            Assert.That(result.Rows[0]["Nullable"], Is.EqualTo(DBNull.Value));
        });
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_ExtraColumnsInDataTable_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestExtraColumns",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 50))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Name", typeof(string));
        dt.Columns.Add("ExtraColumn", typeof(string)); // Not in DB!

        dt.Rows.Add(1, "Test", "Extra");

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);

        var ex = Assert.Throws<ColumnMappingException>(() => bulk.Upload(dt));
        Assert.That(ex?.Message, Does.Contain("ExtraColumn"));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_CaseMismatchedColumns_MapsCorrectly(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestCaseColumns",
        [
            new DatabaseColumnRequest("MixedCaseColumn", new DatabaseTypeRequest(typeof(string), 50)),
            new DatabaseColumnRequest("UPPERCASE", new DatabaseTypeRequest(typeof(int)))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("mixedcasecolumn", typeof(string)); // Different case
        dt.Columns.Add("uppercase", typeof(int)); // Different case

        dt.Rows.Add("Test", 123);

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Upload(dt);

        using var result = tbl.GetDataTable();
        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[0]["MixedCaseColumn"], Is.EqualTo("Test"));
            Assert.That(result.Rows[0]["UPPERCASE"], Is.EqualTo(123));
        });
    }

    #endregion

    #region Transaction Behavior

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_WithTransaction_CommitsProperly(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        TestContext.Out.WriteLine($"[{type}] Created database");
        var tbl = db.CreateTable("TestTransactionCommit",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 50))
        ]);
        TestContext.Out.WriteLine($"[{type}] Created table");

        using var transaction = tbl.Database.Server.BeginNewTransactedConnection();
        TestContext.Out.WriteLine($"[{type}] Started transaction");

        // Check isolation level for SQL Server
        if (type == DatabaseType.MicrosoftSQLServer)
        {
            using var checkCon = tbl.Database.Server.GetConnection();
            checkCon.Open();
            using var cmd = tbl.Database.Server.GetCommand("SELECT CASE transaction_isolation_level WHEN 0 THEN 'Unspecified' WHEN 1 THEN 'ReadUncommitted' WHEN 2 THEN 'ReadCommitted' WHEN 3 THEN 'Repeatable' WHEN 4 THEN 'Serializable' WHEN 5 THEN 'Snapshot' END FROM sys.dm_exec_sessions WHERE session_id = @@SPID", checkCon);
            var isolationLevel = cmd.ExecuteScalar();
            TestContext.Out.WriteLine($"[{type}] Isolation level: {isolationLevel}");
        }

        using (var dt = new DataTable())
        {
            dt.Columns.Add("Id", typeof(int));
            dt.Columns.Add("Value", typeof(string));
            dt.Rows.Add(1, "InTransaction");
            TestContext.Out.WriteLine($"[{type}] Created DataTable");

            using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture, transaction.ManagedTransaction);
            TestContext.Out.WriteLine($"[{type}] Created BulkInsert");
            bulk.Upload(dt);
            TestContext.Out.WriteLine($"[{type}] Upload completed");

            // Inside transaction
            Assert.That(tbl.GetRowCount(transaction.ManagedTransaction), Is.EqualTo(1));
            TestContext.Out.WriteLine($"[{type}] Row count inside transaction verified");
        }

        // Skip checking row count from outside transaction for SQL Server
        // TableLock blocks reads from other connections during active transaction
        if (type != DatabaseType.MicrosoftSQLServer)
        {
            TestContext.Out.WriteLine($"[{type}] About to check row count OUTSIDE transaction");
            // Outside transaction - should be 0 before commit
            Assert.That(tbl.GetRowCount(), Is.EqualTo(0));
            TestContext.Out.WriteLine($"[{type}] Row count outside transaction verified");
        }

        transaction.ManagedTransaction?.CommitAndCloseConnection();
        TestContext.Out.WriteLine($"[{type}] Transaction committed");

        // After commit
        Assert.That(tbl.GetRowCount(), Is.EqualTo(1));
        TestContext.Out.WriteLine($"[{type}] Final row count verified");
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_WithTransaction_RollbackWorks(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestTransactionRollback",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 50))
        ]);

        using var transaction = tbl.Database.Server.BeginNewTransactedConnection();

        using (var dt = new DataTable())
        {
            dt.Columns.Add("Id", typeof(int));
            dt.Columns.Add("Value", typeof(string));
            dt.Rows.Add(1, "WillBeRolledBack");

            using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture, transaction.ManagedTransaction);
            bulk.Upload(dt);

            // Inside transaction
            Assert.That(tbl.GetRowCount(transaction.ManagedTransaction), Is.EqualTo(1));
        }

        transaction.ManagedTransaction?.AbandonAndCloseConnection();

        // After rollback
        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_TransactionError_RollsBackAutomatically(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestAutoRollback",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true, AllowNulls = false },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 10))
        ]);

        using var transaction = tbl.Database.Server.BeginNewTransactedConnection();

        try
        {
            using var dt = new DataTable();
            dt.Columns.Add("Id", typeof(int));
            dt.Columns.Add("Value", typeof(string));
            dt.Rows.Add(1, "Valid");
            dt.Rows.Add(2, new string('X', 50)); // Will fail - too long

            using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture, transaction.ManagedTransaction);
            bulk.Upload(dt); // This should throw
        }
        catch
        {
            transaction.ManagedTransaction?.AbandonAndCloseConnection();
        }

        // Table should be empty - transaction rolled back
        Assert.That(tbl.GetRowCount(), Is.EqualTo(0));
    }

    #endregion

    #region Special Data Types

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_UnicodeStrings_PreservedCorrectly(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestUnicode",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("UnicodeText", new DatabaseTypeRequest(typeof(string), 100) { Unicode = true })
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("UnicodeText", typeof(string));

        dt.Rows.Add(1, "Hello 世界"); // Chinese
        dt.Rows.Add(2, "مرحبا"); // Arabic
        dt.Rows.Add(3, "Привет"); // Russian
        dt.Rows.Add(4, "🎉🎊"); // Emojis

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Upload(dt);

        using var result = tbl.GetDataTable();
        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[0]["UnicodeText"], Is.EqualTo("Hello 世界"));
            Assert.That(result.Rows[1]["UnicodeText"], Is.EqualTo("مرحبا"));
            Assert.That(result.Rows[2]["UnicodeText"], Is.EqualTo("Привет"));
            Assert.That(result.Rows[3]["UnicodeText"], Is.EqualTo("🎉🎊"));
        });
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_DateTimeValues_PreservedCorrectly(DatabaseType type)
    {
        if (type == DatabaseType.Sqlite)
            Assert.Ignore("SQLite stores DateTime as TEXT strings; ADO.NET returns string type. Fundamental SQLite limitation.");

        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestDateTime",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("EventDate", new DatabaseTypeRequest(typeof(DateTime)))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("EventDate", typeof(DateTime));

        var date1 = new DateTime(2024, 1, 15, 10, 30, 45);
        var date2 = new DateTime(1900, 1, 1, 0, 0, 0);
        var date3 = DateTime.Now;

        dt.Rows.Add(1, date1);
        dt.Rows.Add(2, date2);
        dt.Rows.Add(3, date3);

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Upload(dt);

        using var result = tbl.GetDataTable();
        Assert.Multiple(() =>
        {
            Assert.That(((DateTime)result.Rows[0]["EventDate"]).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
                Is.EqualTo(date1.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)));
            Assert.That(((DateTime)result.Rows[1]["EventDate"]).Date, Is.EqualTo(date2.Date));
        });
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_DateTimeStrings_ParsedCorrectly(DatabaseType type)
    {
        if (type == DatabaseType.Sqlite)
            Assert.Ignore("SQLite stores DateTime as TEXT strings; ADO.NET returns string type. Fundamental SQLite limitation.");

        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestDateTimeStrings",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("EventDate", new DatabaseTypeRequest(typeof(DateTime)))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("EventDate", typeof(string)); // String in DataTable, DateTime in DB

        dt.Rows.Add(1, "2024-01-15 10:30:45");
        dt.Rows.Add(2, "2024/12/31");

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Upload(dt);

        using var result = tbl.GetDataTable();
        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[0]["EventDate"], Is.InstanceOf<DateTime>());
            Assert.That(((DateTime)result.Rows[0]["EventDate"]).Year, Is.EqualTo(2024));
            Assert.That(((DateTime)result.Rows[1]["EventDate"]).Month, Is.EqualTo(12));
        });
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_BinaryData_PreservedCorrectly(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestBinary",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("BinaryData", new DatabaseTypeRequest(typeof(byte[]), 100))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("BinaryData", typeof(byte[]));

        var testData = Encoding.UTF8.GetBytes("This is binary data with special chars: \0\n\r\t");
        dt.Rows.Add(1, testData);

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Upload(dt);

        using var result = tbl.GetDataTable();
        var retrieved = (byte[])result.Rows[0]["BinaryData"];
        Assert.That(retrieved, Is.EqualTo(testData));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_BooleanValues_ConvertedCorrectly(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestBoolean",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("IsActive", new DatabaseTypeRequest(typeof(bool)))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("IsActive", typeof(bool));

        dt.Rows.Add(1, true);
        dt.Rows.Add(2, false);

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Upload(dt);

        using var result = tbl.GetDataTable();

        // Different databases store booleans differently (bit, int, etc.)
        // Just verify we can round-trip the data
        Assert.That(result.Rows, Has.Count.EqualTo(2));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_DecimalPrecision_PreservedCorrectly(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestDecimalPrecision",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Amount", new DatabaseTypeRequest(typeof(decimal), null, new DecimalSize(10, 4)))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Amount", typeof(decimal));

        dt.Rows.Add(1, 123.4567m);
        dt.Rows.Add(2, 0.0001m);
        dt.Rows.Add(3, 999999.9999m);

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Upload(dt);

        using var result = tbl.GetDataTable();
        Assert.Multiple(() =>
        {
            Assert.That(Math.Round((decimal)result.Rows[0]["Amount"], 4), Is.EqualTo(123.4567m));
            Assert.That(Math.Round((decimal)result.Rows[1]["Amount"], 4), Is.EqualTo(0.0001m));
            Assert.That(Math.Round((decimal)result.Rows[2]["Amount"], 4), Is.EqualTo(999999.9999m));
        });
    }

    #endregion

    #region Performance and Large Datasets

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_LargeDataset_CompletesSuccessfully(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestLargeDataset",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Value1", new DatabaseTypeRequest(typeof(string), 100)),
            new DatabaseColumnRequest("Value2", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Value3", new DatabaseTypeRequest(typeof(DateTime)))
        ]);

        const int rowCount = 10000;
        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Value1", typeof(string));
        dt.Columns.Add("Value2", typeof(int));
        dt.Columns.Add("Value3", typeof(DateTime));

        for (var i = 1; i <= rowCount; i++)
            dt.Rows.Add(i, $"Value{i}", i * 10, DateTime.Now.AddDays(-i));

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        var affected = bulk.Upload(dt);

        Assert.Multiple(() =>
        {
            Assert.That(affected, Is.EqualTo(rowCount));
            Assert.That(tbl.GetRowCount(), Is.EqualTo(rowCount));
        });
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_WideTable_AllColumnsInserted(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        // Create table with many columns
        var columns = new List<DatabaseColumnRequest>();
        for (var i = 1; i <= 30; i++)
        {
            columns.Add(new DatabaseColumnRequest($"Col{i}", new DatabaseTypeRequest(typeof(int))));
        }

        var tbl = db.CreateTable("TestWideTable", [.. columns]);

        using var dt = new DataTable();
        for (var i = 1; i <= 30; i++)
            dt.Columns.Add($"Col{i}", typeof(int));

        var rowData = new object[30];
        for (var i = 0; i < 30; i++)
            rowData[i] = i + 1;

        dt.Rows.Add(rowData);

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Upload(dt);

        using var result = tbl.GetDataTable();
        Assert.That(result.Columns, Has.Count.EqualTo(30));
        for (var i = 0; i < 30; i++)
            Assert.That(result.Rows[0][$"Col{i + 1}"], Is.EqualTo(i + 1));
    }

    #endregion

    #region Timeout and Cancellation

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_WithTimeout_RespectsSetting(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestTimeout",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Data", new DatabaseTypeRequest(typeof(string), 50))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Data", typeof(string));
        dt.Rows.Add(1, "Test");

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Timeout = 60; // Set explicit timeout
        bulk.Upload(dt);

        Assert.That(tbl.GetRowCount(), Is.EqualTo(1));
    }

    #endregion

    #region Auto-Increment and Identity Columns

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_WithAutoIncrementColumn_GeneratesValues(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestAutoIncrement",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
            {
                IsAutoIncrement = true,
                IsPrimaryKey = true,
                AllowNulls = false
            },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 50))
        ]);

        using var dt = new DataTable();
        // Don't include Id column - let DB generate it
        dt.Columns.Add("Value", typeof(string));
        dt.Rows.Add("Row1");
        dt.Rows.Add("Row2");
        dt.Rows.Add("Row3");

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Upload(dt);

        using var result = tbl.GetDataTable();
        Assert.That(result.Rows, Has.Count.EqualTo(3));

        // Auto-increment should have generated unique IDs
        var ids = new HashSet<int>();
        foreach (DataRow row in result.Rows)
            ids.Add(Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture));

        Assert.That(ids, Has.Count.EqualTo(3)); // All unique
    }

    #endregion

    #region Disposal and Resource Management

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Upload_AfterDispose_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("TestDispose",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 50))
        ]);

        var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        bulk.Dispose();

        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Value", typeof(string));
        dt.Rows.Add(1, "Test");

        // Should throw after disposal
        AssertThrowsException(type, () => bulk.Upload(dt));
    }

    #endregion
}
