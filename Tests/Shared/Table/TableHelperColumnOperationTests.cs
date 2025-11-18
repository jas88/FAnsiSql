using System;
using System.Data;
using System.Linq;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

/// <summary>
/// Tests TableHelper column operations: AddColumn, DropColumn, and column alterations.
/// These tests target uncovered functionality in all TableHelper implementations.
/// </summary>
internal sealed class TableHelperColumnOperationTests : DatabaseTests
{
    #region AddColumn Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void AddColumn_IntColumn_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AddIntColTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            var columnsBefore = table.DiscoverColumns();
            Assert.That(columnsBefore, Has.Length.EqualTo(1));

            table.AddColumn("NewIntCol", "int", true, 30);

            var columnsAfter = table.DiscoverColumns();
            Assert.Multiple(() =>
            {
                Assert.That(columnsAfter, Has.Length.EqualTo(2));
                Assert.That(columnsAfter.Any(c => c.GetRuntimeName()?.Equals("NewIntCol", StringComparison.OrdinalIgnoreCase) == true), Is.True);
            });

            var newCol = columnsAfter.First(c => c.GetRuntimeName()?.Equals("NewIntCol", StringComparison.OrdinalIgnoreCase) == true);
            Assert.Multiple(() =>
            {
                Assert.That(newCol.DataType?.GetCSharpDataType(), Is.EqualTo(typeof(int)));
                Assert.That(newCol.AllowNulls, Is.True);
            });
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void AddColumn_StringColumn_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AddStringColTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            var syntax = table.GetQuerySyntaxHelper();
            var stringType = syntax.TypeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(string), 255));

            table.AddColumn("NewStringCol", stringType, true, 30);

            var newCol = table.DiscoverColumn("NewStringCol");
            Assert.Multiple(() =>
            {
                Assert.That(newCol, Is.Not.Null);
                Assert.That(newCol.DataType?.GetCSharpDataType(), Is.EqualTo(typeof(string)));
                Assert.That(newCol.AllowNulls, Is.True);
            });
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void AddColumn_NotNullColumn_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AddNotNullColTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            // Add with a default value for non-nullable column on empty table
            table.AddColumn("NotNullCol", "int", false, 30);

            var newCol = table.DiscoverColumn("NotNullCol");
            Assert.Multiple(() =>
            {
                Assert.That(newCol, Is.Not.Null);
                Assert.That(newCol.AllowNulls, Is.False);
            });
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void AddColumn_DateTimeColumn_Success(DatabaseType type)
    {
        if (type == DatabaseType.Sqlite)
            Assert.Ignore("SQLite stores DateTime as TEXT which causes type mapping issues in discovery");

        var db = GetTestDatabase(type);
        var table = db.CreateTable("AddDateColTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            var syntax = table.GetQuerySyntaxHelper();
            var dateType = syntax.TypeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(DateTime)));

            table.AddColumn("DateCol", dateType, true, 30);

            var newCol = table.DiscoverColumn("DateCol");
            Assert.Multiple(() =>
            {
                Assert.That(newCol, Is.Not.Null);
                Assert.That(newCol.DataType?.GetCSharpDataType(), Is.EqualTo(typeof(DateTime)));
            });
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void AddColumn_DecimalColumn_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AddDecimalColTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            var syntax = table.GetQuerySyntaxHelper();
            var decimalType = syntax.TypeTranslater.GetSQLDBTypeForCSharpType(
                new DatabaseTypeRequest(typeof(decimal), null, new DecimalSize(10, 2)));

            table.AddColumn("DecimalCol", decimalType, true, 30);

            var newCol = table.DiscoverColumn("DecimalCol");
            Assert.Multiple(() =>
            {
                Assert.That(newCol, Is.Not.Null);
                Assert.That(newCol.DataType?.GetCSharpDataType(), Is.EqualTo(typeof(decimal)));
            });
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void AddColumn_MultipleColumns_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AddMultiColTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            var syntax = table.GetQuerySyntaxHelper();

            table.AddColumn("Col1", "int", true, 30);
            table.AddColumn("Col2", syntax.TypeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(string), 100)), true, 30);
            table.AddColumn("Col3", "int", true, 30);

            var columns = table.DiscoverColumns();
            Assert.That(columns, Has.Length.EqualTo(4), "Should have original column plus 3 new ones");
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region DropColumn Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void DropColumn_ExistingColumn_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("DropColTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("ToDelete", new DatabaseTypeRequest(typeof(int)))
        ]);

        try
        {
            var columnsBefore = table.DiscoverColumns();
            Assert.That(columnsBefore, Has.Length.EqualTo(2));

            var colToDelete = table.DiscoverColumn("ToDelete");
            table.DropColumn(colToDelete);

            var columnsAfter = table.DiscoverColumns();
            Assert.Multiple(() =>
            {
                Assert.That(columnsAfter, Has.Length.EqualTo(1));
                Assert.That(columnsAfter.Any(c => c.GetRuntimeName()?.Equals("ToDelete", StringComparison.OrdinalIgnoreCase) == true), Is.False);
            });
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void DropColumn_WithData_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Value", typeof(string));
        dt.Columns.Add("ToDelete", typeof(int));
        dt.Rows.Add(1, "Test1", 100);
        dt.Rows.Add(2, "Test2", 200);

        var table = db.CreateTable("DropColWithDataTable", dt);

        try
        {
            Assert.That(table.GetRowCount(), Is.EqualTo(2));

            var colToDelete = table.DiscoverColumn("ToDelete");
            table.DropColumn(colToDelete);

            var columnsAfter = table.DiscoverColumns();
            Assert.Multiple(() =>
            {
                Assert.That(columnsAfter, Has.Length.EqualTo(2));
                Assert.That(table.GetRowCount(), Is.EqualTo(2), "Row count should be preserved");
            });
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void DropColumn_NonPrimaryKeyColumn_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("DropNonPkTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Col1", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Col2", new DatabaseTypeRequest(typeof(int)))
        ]);

        try
        {
            var col1 = table.DiscoverColumn("Col1");
            table.DropColumn(col1);

            var columns = table.DiscoverColumns();
            Assert.Multiple(() =>
            {
                Assert.That(columns, Has.Length.EqualTo(2));
                Assert.That(columns.Any(c => c.IsPrimaryKey), Is.True, "Primary key should still exist");
            });
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void DropColumn_LastNonKeyColumn_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("DropLastColTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("LastCol", new DatabaseTypeRequest(typeof(int)))
        ]);

        try
        {
            var colToDelete = table.DiscoverColumn("LastCol");
            table.DropColumn(colToDelete);

            var columns = table.DiscoverColumns();
            Assert.That(columns, Has.Length.EqualTo(1), "Should only have primary key column left");
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region Column Discovery After Operations Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void DiscoverColumn_AfterAddColumn_FindsNewColumn(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("DiscoverNewColTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            table.AddColumn("NewCol", "int", true, 30);

            var newCol = table.DiscoverColumn("NewCol");
            Assert.That(newCol, Is.Not.Null, "Should be able to discover newly added column");
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void DiscoverColumn_CaseInsensitive_FindsColumn(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("CaseInsensitiveColTable",
        [
            new DatabaseColumnRequest("MyColumn", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            var colUpper = table.DiscoverColumn("MYCOLUMN");
            var colLower = table.DiscoverColumn("mycolumn");
            var colMixed = table.DiscoverColumn("MyColumn");

            Assert.Multiple(() =>
            {
                Assert.That(colUpper, Is.Not.Null);
                Assert.That(colLower, Is.Not.Null);
                Assert.That(colMixed, Is.Not.Null);
            });
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region Add and Drop Combined Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void AddAndDropColumn_Sequence_Success(DatabaseType type)
    {
        if (type == DatabaseType.Sqlite)
            Assert.Ignore("SQLite does not support DROP COLUMN operations");

        var db = GetTestDatabase(type);
        var table = db.CreateTable("AddDropSeqTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            // Add a column
            table.AddColumn("TempCol", "int", true, 30);
            Assert.That(table.DiscoverColumns(), Has.Length.EqualTo(2));

            // Drop it
            var tempCol = table.DiscoverColumn("TempCol");
            table.DropColumn(tempCol);
            Assert.That(table.DiscoverColumns(), Has.Length.EqualTo(1));

            // Add another one with the same name
            table.AddColumn("TempCol", "int", true, 30);
            Assert.That(table.DiscoverColumns(), Has.Length.EqualTo(2));
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void AddColumn_InsertData_DropColumn_DataPreserved(DatabaseType type)
    {
        if (type == DatabaseType.Sqlite)
            Assert.Ignore("SQLite does not support DROP COLUMN operations");

        var db = GetTestDatabase(type);
        var table = db.CreateTable("AddInsertDropTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            // Insert data
            table.Insert(new System.Collections.Generic.Dictionary<string, object>
            {
                { "Id", 1 },
                { "Value", "Test1" }
            });

            // Add column
            table.AddColumn("Extra", "int", true, 30);

            // Insert more data
            table.Insert(new System.Collections.Generic.Dictionary<string, object>
            {
                { "Id", 2 },
                { "Value", "Test2" },
                { "Extra", 42 }
            });

            Assert.That(table.GetRowCount(), Is.EqualTo(2));

            // Drop the added column
            var extraCol = table.DiscoverColumn("Extra");
            table.DropColumn(extraCol);

            Assert.Multiple(() =>
            {
                Assert.That(table.GetRowCount(), Is.EqualTo(2), "Rows should be preserved");
                Assert.That(table.DiscoverColumns(), Has.Length.EqualTo(2), "Should have original 2 columns");
            });
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region Column Type Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void AddColumn_AllBasicTypes_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AllTypesTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            var syntax = table.GetQuerySyntaxHelper();

            // Add columns of different types
            table.AddColumn("IntCol", syntax.TypeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(int))), true, 30);
            table.AddColumn("LongCol", syntax.TypeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(long))), true, 30);
            table.AddColumn("ShortCol", syntax.TypeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(short))), true, 30);
            table.AddColumn("StringCol", syntax.TypeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(string), 100)), true, 30);
            table.AddColumn("DateCol", syntax.TypeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(DateTime))), true, 30);

            var columns = table.DiscoverColumns();
            Assert.That(columns.Length, Is.GreaterThanOrEqualTo(6), "Should have at least 6 columns");
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion
}
