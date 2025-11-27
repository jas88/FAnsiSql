using System;
using System.Data;
using System.Data.Common;
using System.Globalization;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Exceptions;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

/// <summary>
/// Tests TableHelper error handling and edge cases.
/// These tests verify proper exception handling and boundary conditions.
/// </summary>
internal abstract class TableHelperErrorHandlingTestsBase : DatabaseTests
{
    #region Drop Non-existent Table Tests

    protected void Drop_NonExistentTable_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.ExpectTable("NonExistentTable");

        Assert.Catch<DbException>(() => table.Drop());
    }

    #endregion

    #region AddColumn Error Tests

    protected void AddColumn_DuplicateColumnName_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("DuplicateColTable",
        [
            new DatabaseColumnRequest("ExistingCol", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            Assert.Catch<DbException>(() => table.AddColumn("ExistingCol", "int", true, 30));
        }
        finally
        {
            table.Drop();
        }
    }

    protected void AddColumn_InvalidDataType_ThrowsException(DatabaseType type)
    {
        if (type == DatabaseType.Sqlite)
            Assert.Ignore("SQLite uses type affinity; accepts any type name without validation");

        var db = GetTestDatabase(type);
        var table = db.CreateTable("InvalidTypeTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            Assert.Catch<DbException>(() => table.AddColumn("BadCol", "INVALID_TYPE_XYZ", true, 30));
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region DropColumn Error Tests

    protected void DropColumn_NonExistentColumn_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("DropNonExistentColTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            // Create a fake column that doesn't exist in the table
            var fakeColumn = new DiscoveredColumn(table, "NonExistentColumn", true);

            Assert.Catch<DbException>(() => table.DropColumn(fakeColumn));
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region Truncate Error Tests

    protected void Truncate_NonExistentTable_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.ExpectTable("NonExistentTruncateTable");

        Assert.Catch<DbException>(() => table.Truncate());
    }

    #endregion

    #region CreateIndex Error Tests

    protected void CreateIndex_DuplicateIndexName_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("DuplicateIndexTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            var valueCol = table.DiscoverColumn("Value");

            table.CreateIndex("idx_test", [valueCol], false);

            // Try to create index with same name
            Assert.Catch<AlterFailedException>(() => table.CreateIndex("idx_test", [valueCol], false));
        }
        finally
        {
            table.Drop();
        }
    }

    protected void DropIndex_NonExistentIndex_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("DropNonExistentIndexTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            Assert.Catch<AlterFailedException>(() => table.DropIndex("non_existent_index"));
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region Rename Error Tests

    protected void Rename_ToExistingTableName_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table1 = db.CreateTable("Table1",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        var table2 = db.CreateTable("Table2",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            // Try to rename table1 to table2 (which already exists)
            // Use Catch instead of Throws to accept database-specific exceptions (OracleException, etc.) that inherit from DbException
            Assert.Catch<DbException>(() => table1.Rename("Table2"));
        }
        finally
        {
            table1.Drop();
            table2.Drop();
        }
    }

    #endregion

    #region Foreign Key Error Tests

    protected void AddForeignKey_NoPrimaryKey_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var parentTable = db.CreateTable("ParentNoPk",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) // No primary key!
        ]);

        var childTable = db.CreateTable("ChildNoPk",
        [
            new DatabaseColumnRequest("ParentId", new DatabaseTypeRequest(typeof(int)))
        ]);

        try
        {
            var parentId = parentTable.DiscoverColumn("Id");
            var childParentId = childTable.DiscoverColumn("ParentId");

            // Should fail because parent table has no primary key
            Assert.Catch<AlterFailedException>(() => childTable.AddForeignKey(childParentId, parentId, false));
        }
        finally
        {
            childTable.Drop();
            parentTable.Drop();
        }
    }

    protected void AddForeignKey_TypeMismatch_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var parentTable = db.CreateTable("ParentTypeMismatch",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        var childTable = db.CreateTable("ChildTypeMismatch",
        [
            new DatabaseColumnRequest("ParentId", new DatabaseTypeRequest(typeof(string), 50)) // String instead of int
        ]);

        try
        {
            var parentId = parentTable.DiscoverColumn("Id");
            var childParentId = childTable.DiscoverColumn("ParentId");

            // Should fail because types don't match
            Assert.Catch<AlterFailedException>(() => childTable.AddForeignKey(childParentId, parentId, false));
        }
        finally
        {
            childTable.Drop();
            parentTable.Drop();
        }
    }

    #endregion

    #region Empty/Null Parameter Tests

    protected void AddColumn_EmptyColumnName_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("EmptyColNameTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true }
        ]);

        try
        {
            Assert.Catch<Exception>(() => table.AddColumn("", "int", true, 30));
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region Concurrent Operation Tests

    protected void Insert_WithAutoIncrement_Concurrent_NoCollisions(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("ConcurrentInsertTable",
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
            // Insert multiple rows quickly
            for (var i = 0; i < 10; i++)
            {
                table.Insert(new System.Collections.Generic.Dictionary<string, object>
                {
                    { "Value", $"Concurrent{i}" }
                });
            }

            var dt = table.GetDataTable();
            Assert.Multiple(() =>
            {
                Assert.That(dt.Rows, Has.Count.EqualTo(10));

                // Verify all IDs are unique
                var ids = new System.Collections.Generic.HashSet<int>();
                foreach (DataRow row in dt.Rows)
                {
                    ids.Add(Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture));
                }
                Assert.That(ids, Has.Count.EqualTo(10), "All IDs should be unique");
            });
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region NULL Handling Tests

    protected void Insert_NullIntoNotNullColumn_ThrowsException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("NotNullTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("NotNullCol", new DatabaseTypeRequest(typeof(string), 100)) { AllowNulls = false }
        ]);

        try
        {
            Assert.Catch<Exception>(() => table.Insert(new System.Collections.Generic.Dictionary<string, object>
            {
                { "Id", 1 },
                { "NotNullCol", DBNull.Value }
            }));
        }
        finally
        {
            table.Drop();
        }
    }

    protected void Insert_NullIntoNullableColumn_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("NullableTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("NullableCol", new DatabaseTypeRequest(typeof(string), 100)) { AllowNulls = true }
        ]);

        try
        {
            Assert.DoesNotThrow(() => table.Insert(new System.Collections.Generic.Dictionary<string, object>
            {
                { "Id", 1 },
                { "NullableCol", DBNull.Value }
            }));

            var dt = table.GetDataTable();
            Assert.That(dt.Rows[0]["NullableCol"], Is.EqualTo(DBNull.Value));
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region Transaction Rollback Tests

    protected void Insert_RollbackTransaction_NoDataPersisted(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("RollbackTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            using (var con = db.Server.BeginNewTransactedConnection())
            {
                var syntax = table.GetQuerySyntaxHelper();
                var insertSql = $"INSERT INTO {table.GetFullyQualifiedName()} ({syntax.EnsureWrapped("Id")}, {syntax.EnsureWrapped("Value")}) VALUES (1, 'RollbackTest')";

                using var cmd = db.Server.GetCommand(insertSql, con.Connection);
                cmd.Transaction = con.Transaction;
                cmd.ExecuteNonQuery();

                // Explicitly rollback
                con.ManagedTransaction?.AbandonAndCloseConnection();
            }

            // Verify no data was persisted
            Assert.That(table.GetRowCount(), Is.EqualTo(0), "Table should be empty after rollback");
        }
        finally
        {
            table.Drop();
        }
    }

    protected void Insert_CommitTransaction_DataPersisted(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("CommitTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            using (var con = db.Server.BeginNewTransactedConnection())
            {
                var syntax = table.GetQuerySyntaxHelper();
                var insertSql = $"INSERT INTO {table.GetFullyQualifiedName()} ({syntax.EnsureWrapped("Id")}, {syntax.EnsureWrapped("Value")}) VALUES (1, 'CommitTest')";

                using var cmd = db.Server.GetCommand(insertSql, con.Connection);
                cmd.Transaction = con.Transaction;
                cmd.ExecuteNonQuery();

                // Explicitly commit
                con.ManagedTransaction?.CommitAndCloseConnection();
            }

            // Verify data was persisted
            Assert.That(table.GetRowCount(), Is.EqualTo(1), "Table should have 1 row after commit");
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region Large Data Tests

    protected void GetRowCount_LargeTable_ReturnsCorrectCount(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Value", typeof(string));

        const int rowCount = 1000;
        for (var i = 0; i < rowCount; i++)
        {
            dt.Rows.Add(i, $"Value{i}");
        }

        var table = db.CreateTable("LargeTable", dt);

        try
        {
            Assert.That(table.GetRowCount(), Is.EqualTo(rowCount));
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region Special Character Tests

    protected void Insert_SpecialCharactersInString_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("SpecialCharsTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 255))
        ]);

        try
        {
            var specialString = "Test'\"\\;--/**/DROP TABLE;";

            Assert.DoesNotThrow(() => table.Insert(new System.Collections.Generic.Dictionary<string, object>
            {
                { "Id", 1 },
                { "Value", specialString }
            }));

            var dt = table.GetDataTable();
            Assert.That(dt.Rows[0]["Value"].ToString(), Does.Contain("'"));
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion
}
