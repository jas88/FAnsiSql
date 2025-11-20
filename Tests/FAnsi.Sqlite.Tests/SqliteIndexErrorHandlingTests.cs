using System;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Exceptions;
using Microsoft.Data.Sqlite;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests;

/// <summary>
/// SQLite-specific tests for index error handling.
/// Tests error paths in SqliteTableHelper.CreateIndex and DropIndex that catch SqliteException
/// and wrap them in AlterFailedException (lines 238-241 and 253-257).
/// </summary>
internal sealed class SqliteIndexErrorHandlingTests : DatabaseTests
{
    #region CreateIndex Error Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void CreateIndex_DuplicateIndexName_ThrowsAlterFailedException(DatabaseType type)
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

            // Create first index
            table.CreateIndex("idx_duplicate_test", [valueCol], false);

            // Try to create index with same name - should throw AlterFailedException wrapping SqliteException
            var ex = Assert.Throws<AlterFailedException>(() => table.CreateIndex("idx_duplicate_test", [valueCol], false));

            Assert.Multiple(() =>
            {
                Assert.That(ex.Message, Does.Contain("Failed to create index"));
                Assert.That(ex.Message, Does.Contain("idx_duplicate_test"));
                Assert.That(ex.InnerException, Is.InstanceOf<SqliteException>(), "Inner exception should be SqliteException");
            });
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void CreateIndex_OnNonExistentColumn_ThrowsAlterFailedException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("NonExistentColumnIndexTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            // Create a fake column that doesn't exist in the table
            var fakeColumn = new DiscoveredColumn(table, "NonExistentColumn", true);

            // Try to create index on non-existent column - should throw AlterFailedException wrapping SqliteException
            var ex = Assert.Throws<AlterFailedException>(() => table.CreateIndex("idx_bad_column", [fakeColumn], false));

            Assert.Multiple(() =>
            {
                Assert.That(ex.Message, Does.Contain("Failed to create index"));
                Assert.That(ex.Message, Does.Contain("idx_bad_column"));
                Assert.That(ex.InnerException, Is.InstanceOf<SqliteException>(), "Inner exception should be SqliteException");
            });
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region DropIndex Error Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void DropIndex_NonExistentIndex_ThrowsAlterFailedException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("DropNonExistentIndexTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            // Try to drop index that doesn't exist - should throw AlterFailedException wrapping SqliteException
            var ex = Assert.Throws<AlterFailedException>(() => table.DropIndex("non_existent_index"));

            Assert.Multiple(() =>
            {
                Assert.That(ex.Message, Does.Contain("Failed to drop index"));
                Assert.That(ex.Message, Does.Contain("non_existent_index"));
                Assert.That(ex.InnerException, Is.InstanceOf<SqliteException>(), "Inner exception should be SqliteException");
            });
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void DropIndex_AlreadyDropped_ThrowsAlterFailedException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AlreadyDroppedIndexTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            var valueCol = table.DiscoverColumn("Value");

            // Create index
            table.CreateIndex("idx_to_drop_twice", [valueCol], false);

            // Drop it once - should succeed
            Assert.DoesNotThrow(() => table.DropIndex("idx_to_drop_twice"));

            // Try to drop again - should throw AlterFailedException wrapping SqliteException
            var ex = Assert.Throws<AlterFailedException>(() => table.DropIndex("idx_to_drop_twice"));

            Assert.Multiple(() =>
            {
                Assert.That(ex.Message, Does.Contain("Failed to drop index"));
                Assert.That(ex.Message, Does.Contain("idx_to_drop_twice"));
                Assert.That(ex.InnerException, Is.InstanceOf<SqliteException>(), "Inner exception should be SqliteException");
            });
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region Edge Cases

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void CreateIndex_UniqueConstraintViolation_ThrowsAlterFailedException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("UniqueConstraintTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            // Insert duplicate values
            table.Insert(new System.Collections.Generic.Dictionary<string, object>
            {
                { "Id", 1 },
                { "Value", "Duplicate" }
            });

            table.Insert(new System.Collections.Generic.Dictionary<string, object>
            {
                { "Id", 2 },
                { "Value", "Duplicate" }
            });

            var valueCol = table.DiscoverColumn("Value");

            // Try to create unique index on column with duplicate values - should throw AlterFailedException wrapping SqliteException
            var ex = Assert.Throws<AlterFailedException>(() => table.CreateIndex("idx_unique_violation", [valueCol], isUnique: true));

            Assert.Multiple(() =>
            {
                Assert.That(ex.Message, Does.Contain("Failed to create index"));
                Assert.That(ex.Message, Does.Contain("idx_unique_violation"));
                Assert.That(ex.InnerException, Is.InstanceOf<SqliteException>(), "Inner exception should be SqliteException");
            });
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion
}
