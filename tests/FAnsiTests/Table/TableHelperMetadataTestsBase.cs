using System.Data;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

/// <summary>
///     Tests TableHelper metadata operations: HasPrimaryKey, DiscoverColumns, DiscoverRelationships.
///     These tests target uncovered functionality in all TableHelper implementations.
/// </summary>
internal abstract class TableHelperMetadataTestsBase : DatabaseTests
{
    #region HasPrimaryKey Tests

    protected void HasPrimaryKey_NoPrimaryKey_ReturnsFalse(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("NoPkTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            Assert.That(table.DiscoverColumns().Any(c => c.IsPrimaryKey), Is.False,
                "Table without primary key should return false");
        }
        finally
        {
            table.Drop();
        }
    }

    protected void HasPrimaryKey_WithPrimaryKey_ReturnsTrue(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("WithPkTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            Assert.That(table.DiscoverColumns().Any(c => c.IsPrimaryKey), Is.True,
                "Table with primary key should return true");
        }
        finally
        {
            table.Drop();
        }
    }

    protected void HasPrimaryKey_CompositePrimaryKey_ReturnsTrue(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("CompositePkTable",
        [
            new DatabaseColumnRequest("Id1", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Id2", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            Assert.That(table.DiscoverColumns().Any(c => c.IsPrimaryKey), Is.True,
                "Table with composite primary key should return true");
        }
        finally
        {
            table.Drop();
        }
    }

    protected void HasPrimaryKey_AfterAddingPrimaryKey_ReturnsTrue(DatabaseType type)
    {
        if (type == DatabaseType.Sqlite)
            Assert.Ignore("SQLite does not support adding primary keys to existing tables");

        var db = GetTestDatabase(type);
        var table = db.CreateTable("AddPkTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { AllowNulls = false },
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            Assert.That(table.DiscoverColumns().Any(c => c.IsPrimaryKey), Is.False,
                "Initially should have no primary key");

            var idColumn = table.DiscoverColumn("Id");
            table.CreatePrimaryKey(idColumn);

            Assert.That(table.DiscoverColumns().Any(c => c.IsPrimaryKey), Is.True,
                "After adding primary key, should return true");
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region DiscoverColumns Tests

    protected void DiscoverColumns_EmptyTable_ReturnsColumns(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("DiscoverColsTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 100)),
            new DatabaseColumnRequest("Age", new DatabaseTypeRequest(typeof(int)))
        ]);

        try
        {
            var columns = table.DiscoverColumns();

            Assert.Multiple(() =>
            {
                Assert.That(columns, Has.Length.EqualTo(3));
                Assert.That(
                    columns.Any(c => c.GetRuntimeName()?.Equals("Id", StringComparison.OrdinalIgnoreCase) == true),
                    Is.True);
                Assert.That(
                    columns.Any(c => c.GetRuntimeName()?.Equals("Name", StringComparison.OrdinalIgnoreCase) == true),
                    Is.True);
                Assert.That(
                    columns.Any(c => c.GetRuntimeName()?.Equals("Age", StringComparison.OrdinalIgnoreCase) == true),
                    Is.True);
            });
        }
        finally
        {
            table.Drop();
        }
    }

    protected void DiscoverColumns_IdentifiesPrimaryKey(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("PkColTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            var columns = table.DiscoverColumns();
            var idColumn = columns.FirstOrDefault(c =>
                c.GetRuntimeName()?.Equals("Id", StringComparison.OrdinalIgnoreCase) == true);

            Assert.Multiple(() =>
            {
                Assert.That(idColumn, Is.Not.Null);
                Assert.That(idColumn!.IsPrimaryKey, Is.True);
            });
        }
        finally
        {
            table.Drop();
        }
    }

    protected void DiscoverColumns_IdentifiesAutoIncrement(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AutoIncTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
            {
                IsAutoIncrement = true,
                IsPrimaryKey = true
            },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            var columns = table.DiscoverColumns();
            var idColumn = columns.FirstOrDefault(c =>
                c.GetRuntimeName()?.Equals("Id", StringComparison.OrdinalIgnoreCase) == true);

            Assert.Multiple(() =>
            {
                Assert.That(idColumn, Is.Not.Null);
                Assert.That(idColumn!.IsAutoIncrement, Is.True);
            });
        }
        finally
        {
            table.Drop();
        }
    }

    protected void DiscoverColumns_IdentifiesNullability(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("NullableColTable",
        [
            new DatabaseColumnRequest("NotNull", new DatabaseTypeRequest(typeof(int))) { AllowNulls = false },
            new DatabaseColumnRequest("Nullable", new DatabaseTypeRequest(typeof(int))) { AllowNulls = true }
        ]);

        try
        {
            var columns = table.DiscoverColumns();
            var notNullCol = columns.FirstOrDefault(c =>
                c.GetRuntimeName()?.Equals("NotNull", StringComparison.OrdinalIgnoreCase) == true);
            var nullableCol = columns.FirstOrDefault(c =>
                c.GetRuntimeName()?.Equals("Nullable", StringComparison.OrdinalIgnoreCase) == true);

            Assert.Multiple(() =>
            {
                Assert.That(notNullCol, Is.Not.Null);
                Assert.That(notNullCol!.AllowNulls, Is.False);
                Assert.That(nullableCol, Is.Not.Null);
                Assert.That(nullableCol!.AllowNulls, Is.True);
            });
        }
        finally
        {
            table.Drop();
        }
    }

    protected void DiscoverColumns_CorrectDataTypes(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("DataTypesTable",
        [
            new DatabaseColumnRequest("IntCol", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("StringCol", new DatabaseTypeRequest(typeof(string), 50)),
            new DatabaseColumnRequest("DateCol", new DatabaseTypeRequest(typeof(DateTime))),
            new DatabaseColumnRequest("DecimalCol",
                new DatabaseTypeRequest(typeof(decimal), null, new DecimalSize(10, 2)))
        ]);

        try
        {
            var columns = table.DiscoverColumns();

            Assert.That(columns, Has.Length.EqualTo(4));

            var intCol = columns.FirstOrDefault(c =>
                c.GetRuntimeName()?.Equals("IntCol", StringComparison.OrdinalIgnoreCase) == true);
            var stringCol = columns.FirstOrDefault(c =>
                c.GetRuntimeName()?.Equals("StringCol", StringComparison.OrdinalIgnoreCase) == true);
            var dateCol = columns.FirstOrDefault(c =>
                c.GetRuntimeName()?.Equals("DateCol", StringComparison.OrdinalIgnoreCase) == true);
            var decimalCol = columns.FirstOrDefault(c =>
                c.GetRuntimeName()?.Equals("DecimalCol", StringComparison.OrdinalIgnoreCase) == true);

            Assert.Multiple(() =>
            {
                Assert.That(intCol, Is.Not.Null);
                Assert.That(intCol!.DataType?.GetCSharpDataType(), Is.EqualTo(typeof(int)));

                Assert.That(stringCol, Is.Not.Null);
                Assert.That(stringCol!.DataType?.GetCSharpDataType(), Is.EqualTo(typeof(string)));
                Assert.That(stringCol.DataType?.GetLengthIfString(), Is.EqualTo(50));

                Assert.That(dateCol, Is.Not.Null);
                if (type != DatabaseType.Sqlite) // SQLite stores DateTime as TEXT
                    Assert.That(dateCol!.DataType?.GetCSharpDataType(), Is.EqualTo(typeof(DateTime)));

                Assert.That(decimalCol, Is.Not.Null);
                Assert.That(decimalCol!.DataType?.GetCSharpDataType(), Is.EqualTo(typeof(decimal)));
            });
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region DiscoverRelationships Tests

    protected void DiscoverRelationships_NoRelationships_ReturnsEmpty(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("NoRelTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            var relationships = table.DiscoverRelationships();

            Assert.That(relationships, Is.Empty, "Table with no relationships should return empty array");
        }
        finally
        {
            table.Drop();
        }
    }

    protected void DiscoverRelationships_WithForeignKey_ReturnsRelationship(DatabaseType type)
    {
        if (type == DatabaseType.Sqlite)
            Assert.Ignore(
                "SQLite does not support ALTER TABLE ADD CONSTRAINT for foreign keys - they must be defined at table creation");

        var db = GetTestDatabase(type);

        var parentTable = db.CreateTable("ParentRelTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        var childTable = db.CreateTable("ChildRelTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("ParentId", new DatabaseTypeRequest(typeof(int)))
        ]);

        try
        {
            var parentId = parentTable.DiscoverColumn("Id");
            var childParentId = childTable.DiscoverColumn("ParentId");

            childTable.AddForeignKey(childParentId, parentId, false);

            var relationships = parentTable.DiscoverRelationships();

            Assert.Multiple(() =>
            {
                Assert.That(relationships, Has.Length.EqualTo(1));
                Assert.That(relationships[0].PrimaryKeyTable.GetRuntimeName(),
                    Is.EqualTo(parentTable.GetRuntimeName()).IgnoreCase);
                Assert.That(relationships[0].ForeignKeyTable.GetRuntimeName(),
                    Is.EqualTo(childTable.GetRuntimeName()).IgnoreCase);
            });
        }
        finally
        {
            childTable.Drop();
            parentTable.Drop();
        }
    }

    #endregion

    #region ScriptTableCreation Tests

    protected void ScriptTableCreation_ReturnsValidSql(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("ScriptTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            var sql = table.ScriptTableCreation(false, false, false);

            Assert.Multiple(() =>
            {
                Assert.That(sql, Is.Not.Null.And.Not.Empty);
                Assert.That(sql.ToLowerInvariant(), Does.Contain("create"));
                Assert.That(sql.ToLowerInvariant(), Does.Contain("table"));
            });
        }
        finally
        {
            table.Drop();
        }
    }

    protected void ScriptTableCreation_DropPrimaryKeys_OmitsPrimaryKey(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("ScriptPkTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            var sqlWithPk = table.ScriptTableCreation(false, false, false);
            var sqlWithoutPk = table.ScriptTableCreation(true, false, false);

            Assert.Multiple(() =>
            {
                Assert.That(sqlWithPk, Is.Not.EqualTo(sqlWithoutPk));
                // The script without PK should have different constraints
            });
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region MakeDistinct Tests

    protected void MakeDistinct_TableWithDuplicates_RemovesDuplicates(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var dt = new DataTable();
        dt.Columns.Add("Value", typeof(string));
        dt.Rows.Add("A");
        dt.Rows.Add("B");
        dt.Rows.Add("A"); // Duplicate
        dt.Rows.Add("C");
        dt.Rows.Add("B"); // Duplicate

        var table = db.CreateTable("DistinctTable", dt);

        try
        {
            Assert.That(table.GetRowCount(), Is.EqualTo(5), "Should have 5 rows initially");

            table.MakeDistinct();

            Assert.That(table.GetRowCount(), Is.EqualTo(3), "Should have 3 distinct rows after MakeDistinct");
        }
        finally
        {
            table.Drop();
        }
    }

    protected void MakeDistinct_TableWithPrimaryKey_NoChange(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("DistinctPkTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            table.Insert(new Dictionary<string, object> { { "Id", 1 }, { "Value", "A" } });
            table.Insert(new Dictionary<string, object> { { "Id", 2 }, { "Value", "B" } });

            Assert.That(table.GetRowCount(), Is.EqualTo(2));

            // Should not throw and should not modify the table
            Assert.DoesNotThrow(() => table.MakeDistinct());

            Assert.That(table.GetRowCount(), Is.EqualTo(2), "Row count should remain the same");
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion
}
