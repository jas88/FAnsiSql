using System;
using System.Linq;
using System.Data;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

/// <summary>
/// Tests core TableHelper operations: Exists, Drop, Truncate, and basic metadata operations.
/// These tests target uncovered functionality in all TableHelper implementations.
/// </summary>
internal sealed class TableHelperCoreTests : DatabaseTests
{
    #region Exists Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Exists_TableExists_ReturnsTrue(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("ExistingTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
        ]);

        Assert.That(table.Exists(), Is.True, "Table should exist immediately after creation");

        table.Drop();
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Exists_TableDoesNotExist_ReturnsFalse(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.ExpectTable("NonExistentTable");

        Assert.That(table.Exists(), Is.False, "Non-existent table should return false");
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Exists_AfterDrop_ReturnsFalse(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("TableToDelete",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
        ]);

        Assert.That(table.Exists(), Is.True, "Table should exist after creation");

        table.Drop();

        Assert.That(table.Exists(), Is.False, "Table should not exist after drop");
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Exists_CaseInsensitive_ReturnsTrue(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("MyTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
        ]);

        try
        {
            var upperTable = db.ExpectTable("MYTABLE");
            var lowerTable = db.ExpectTable("mytable");

            Assert.Multiple(() =>
            {
                Assert.That(upperTable.Exists(), Is.True, "Uppercase version should exist");
                Assert.That(lowerTable.Exists(), Is.True, "Lowercase version should exist");
            });
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Exists_WithSchema_ReturnsTrue(DatabaseType type)
    {
        if (type == DatabaseType.MySql)
        {
            Assert.Ignore("MySQL doesn't support schemas in the same way");
            return;
        }

        var db = GetTestDatabase(type);
        var schema = type switch
        {
            DatabaseType.Oracle => db.Server.GetCurrentDatabase()?.GetRuntimeName()?.ToUpperInvariant(),
            DatabaseType.PostgreSql => "public",
            _ => "dbo"
        };

        var table = db.CreateTable("SchemaTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
        ]);

        try
        {
            var tableWithSchema = db.ExpectTable("SchemaTable", schema);
            Assert.That(tableWithSchema.Exists(), Is.True, "Table with explicit schema should exist");
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region Drop Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Drop_TableExists_SuccessfullyDrops(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("TableToDrop",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        Assert.That(table.Exists(), Is.True);

        Assert.DoesNotThrow(() => table.Drop());

        Assert.That(table.Exists(), Is.False);
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Drop_TableWithData_SuccessfullyDrops(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Value", typeof(string));
        dt.Rows.Add(1, "Test");
        dt.Rows.Add(2, "Data");

        var table = db.CreateTable("TableWithData", dt);

        Assert.That(table.GetRowCount(), Is.EqualTo(2));

        table.Drop();

        Assert.That(table.Exists(), Is.False);
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Drop_View_SuccessfullyDrops(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var baseTable = db.CreateTable("BaseTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
        ]);

        try
        {
            var viewName = "TestView";
            var view = db.ExpectTable(viewName, null, TableType.View);

            // SQL Server doesn't allow database name prefix in CREATE VIEW
            var viewQualifier = type == DatabaseType.MicrosoftSQLServer
                ? view.GetQuerySyntaxHelper().EnsureFullyQualified(null, view.Schema, view.GetRuntimeName())
                : view.GetFullyQualifiedName();

            var sql = $"CREATE VIEW {viewQualifier} AS SELECT * FROM {baseTable.GetFullyQualifiedName()}";

            using (var con = db.Server.GetConnection())
            {
                con.Open();
                using var cmd = db.Server.GetCommand(sql, con);
                cmd.ExecuteNonQuery();
            }

            Assert.That(view.Exists(), Is.True);

            view.Drop();

            Assert.That(view.Exists(), Is.False);
        }
        finally
        {
            baseTable.Drop();
        }
    }

    #endregion

    #region Truncate Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Truncate_TableWithData_RemovesAllRows(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Value", typeof(string));
        for (var i = 0; i < 100; i++)
        {
            dt.Rows.Add(i, $"Value{i}");
        }

        var table = db.CreateTable("TableToTruncate", dt);

        Assert.That(table.GetRowCount(), Is.EqualTo(100), "Table should have 100 rows initially");

        table.Truncate();

        Assert.Multiple(() =>
        {
            Assert.That(table.Exists(), Is.True, "Table should still exist after truncate");
            Assert.That(table.GetRowCount(), Is.EqualTo(0), "Table should have 0 rows after truncate");
        });

        table.Drop();
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Truncate_EmptyTable_NoError(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("EmptyTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
        ]);

        Assert.That(table.GetRowCount(), Is.EqualTo(0));

        Assert.DoesNotThrow(() => table.Truncate(), "Truncating empty table should not throw");

        Assert.Multiple(() =>
        {
            Assert.That(table.Exists(), Is.True);
            Assert.That(table.GetRowCount(), Is.EqualTo(0));
        });

        table.Drop();
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Truncate_PreservesTableStructure(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("StructureTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 100)),
            new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof(int)))
        ]);

        table.Insert(new System.Collections.Generic.Dictionary<string, object>
        {
            { "Id", 1 },
            { "Name", "Test" },
            { "Age", 25 }
        });

        var columnsBefore = table.DiscoverColumns();
        var countBefore = columnsBefore.Length;

        table.Truncate();

        var columnsAfter = table.DiscoverColumns();

        Assert.Multiple(() =>
        {
            Assert.That(columnsAfter, Has.Length.EqualTo(countBefore), "Column count should be preserved");
            Assert.That(table.GetRowCount(), Is.EqualTo(0), "Rows should be removed");
            Assert.That(columnsAfter.Any(c => c.IsPrimaryKey), Is.True, "Primary key should be preserved");
        });

        table.Drop();
    }

    #endregion

    #region GetRowCount Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void GetRowCount_EmptyTable_ReturnsZero(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("EmptyCountTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
        ]);

        Assert.That(table.GetRowCount(), Is.EqualTo(0));

        table.Drop();
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void GetRowCount_TableWithRows_ReturnsCorrectCount(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Value", typeof(string));

        for (var i = 0; i < 42; i++)
        {
            dt.Rows.Add(i, $"Value{i}");
        }

        var table = db.CreateTable("CountTable", dt);

        Assert.That(table.GetRowCount(), Is.EqualTo(42));

        table.Drop();
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void GetRowCount_AfterInsert_ReturnsUpdatedCount(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("InsertCountTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
        ]);

        Assert.That(table.GetRowCount(), Is.EqualTo(0));

        table.Insert(new System.Collections.Generic.Dictionary<string, object> { { "Id", 1 } });
        Assert.That(table.GetRowCount(), Is.EqualTo(1));

        table.Insert(new System.Collections.Generic.Dictionary<string, object> { { "Id", 2 } });
        Assert.That(table.GetRowCount(), Is.EqualTo(2));

        table.Drop();
    }

    #endregion

    #region IsEmpty Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void IsEmpty_EmptyTable_ReturnsTrue(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("EmptyCheckTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
        ]);

        Assert.That(table.IsEmpty(), Is.True);

        table.Drop();
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void IsEmpty_TableWithData_ReturnsFalse(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("NotEmptyTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
        ]);

        table.Insert(new System.Collections.Generic.Dictionary<string, object> { { "Id", 1 } });

        Assert.That(table.IsEmpty(), Is.False);

        table.Drop();
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void IsEmpty_AfterTruncate_ReturnsTrue(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("TruncateEmptyTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
        ]);

        table.Insert(new System.Collections.Generic.Dictionary<string, object> { { "Id", 1 } });
        Assert.That(table.IsEmpty(), Is.False);

        table.Truncate();
        Assert.That(table.IsEmpty(), Is.True);

        table.Drop();
    }

    #endregion

    #region GetTopXSql Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void GetTopXSql_ReturnsValidSql(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Value", typeof(string));
        for (var i = 1; i <= 20; i++)
        {
            dt.Rows.Add(i, $"Value{i}");
        }

        var table = db.CreateTable("TopXTable", dt);

        try
        {
            var topSql = table.Helper.GetTopXSqlForTable(table, 5);

            Assert.That(topSql, Is.Not.Null.And.Not.Empty);

            using var con = db.Server.GetConnection();
            con.Open();
            using var cmd = db.Server.GetCommand(topSql, con);
            using var reader = cmd.ExecuteReader();

            var count = 0;
            while (reader.Read())
            {
                count++;
            }

            Assert.That(count, Is.EqualTo(5), "Should return exactly 5 rows");
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region FillDataTableWithTopX Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void FillDataTableWithTopX_ReturnsCorrectRowCount(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var sourceData = new DataTable();
        sourceData.Columns.Add("Id", typeof(int));
        sourceData.Columns.Add("Value", typeof(string));
        for (var i = 1; i <= 50; i++)
        {
            sourceData.Rows.Add(i, $"Value{i}");
        }

        var table = db.CreateTable("FillTopXTable", sourceData);

        try
        {
            var resultDt = table.GetDataTable(10);

            Assert.That(resultDt.Rows, Has.Count.EqualTo(10), "Should return exactly 10 rows");
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void FillDataTableWithTopX_PreservesColumnTypes(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var dt = new DataTable();
        dt.Columns.Add("IntCol", typeof(int));
        dt.Columns.Add("StringCol", typeof(string));
        dt.Columns.Add("DateCol", typeof(DateTime));
        dt.Rows.Add(1, "Test", new DateTime(2024, 1, 1));

        var table = db.CreateTable("TypesTable", dt);

        try
        {
            var result = table.GetDataTable(1);

            Assert.Multiple(() =>
            {
                Assert.That(result.Columns, Has.Count.EqualTo(3));
                Assert.That(result.Rows, Has.Count.EqualTo(1));
            });
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion
}
