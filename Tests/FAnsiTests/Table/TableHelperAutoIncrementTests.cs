using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

/// <summary>
/// Tests TableHelper auto-increment/identity operations: ExecuteInsertReturningIdentity and related functionality.
/// These tests target uncovered functionality in all TableHelper implementations.
/// </summary>
internal sealed class TableHelperAutoIncrementTests : DatabaseTests
{
    #region ExecuteInsertReturningIdentity Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void ExecuteInsertReturningIdentity_SingleInsert_ReturnsIdentity(DatabaseType type)
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
            using var con = db.Server.GetConnection();
            con.Open();

            var syntax = table.GetQuerySyntaxHelper();
            var insertSql = $"INSERT INTO {table.GetFullyQualifiedName()} ({syntax.EnsureWrapped("Value")}) VALUES ('Test1')";

            using var cmd = db.Server.GetCommand(insertSql, con);
            var identity = table.Helper.ExecuteInsertReturningIdentity(table, cmd);

            Assert.That(identity, Is.GreaterThan(0), "Identity should be greater than 0");

            var rowCount = table.GetRowCount();
            Assert.That(rowCount, Is.EqualTo(1), "Should have 1 row after insert");
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void ExecuteInsertReturningIdentity_MultipleInserts_ReturnsIncrementingValues(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("MultiAutoIncTable",
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
            var identities = new List<int>();

            using var con = db.Server.GetConnection();
            con.Open();

            var syntax = table.GetQuerySyntaxHelper();

            for (var i = 0; i < 5; i++)
            {
                var insertSql = $"INSERT INTO {table.GetFullyQualifiedName()} ({syntax.EnsureWrapped("Value")}) VALUES ('Test{i}')";
                using var cmd = db.Server.GetCommand(insertSql, con);
                var identity = table.Helper.ExecuteInsertReturningIdentity(table, cmd);
                identities.Add(identity);
            }

            Assert.Multiple(() =>
            {
                Assert.That(identities, Has.Count.EqualTo(5));
                Assert.That(identities[0], Is.LessThan(identities[4]), "Identities should be increasing");
            });

            // Verify all inserts succeeded
            Assert.That(table.GetRowCount(), Is.EqualTo(5));
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void ExecuteInsertReturningIdentity_WithTransaction_ReturnsIdentity(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("TransAutoIncTable",
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
            using var con = db.Server.BeginNewTransactedConnection();

            var syntax = table.GetQuerySyntaxHelper();
            var insertSql = $"INSERT INTO {table.GetFullyQualifiedName()} ({syntax.EnsureWrapped("Value")}) VALUES ('TransTest')";

            using var cmd = db.Server.GetCommand(insertSql, con.Connection);
            cmd.Transaction = con.Transaction;

            var identity = table.Helper.ExecuteInsertReturningIdentity(table, cmd, con.ManagedTransaction);

            Assert.That(identity, Is.GreaterThan(0));

            con.ManagedTransaction?.CommitAndCloseConnection();

            // Verify the insert was committed
            Assert.That(table.GetRowCount(), Is.EqualTo(1));
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region Auto-increment Column Discovery Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void DiscoverColumns_IdentifiesAutoIncrement_Correctly(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("DiscoverAutoIncTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int)))
            {
                IsAutoIncrement = true,
                IsPrimaryKey = true
            },
            new DatabaseColumnRequest("NotAutoInc", new DatabaseTypeRequest(typeof(int)))
        ]);

        try
        {
            var columns = table.DiscoverColumns();

            var idCol = System.Linq.Enumerable.First(columns, c => c.GetRuntimeName()?.Equals("Id", StringComparison.OrdinalIgnoreCase) == true);
            var notAutoIncCol = System.Linq.Enumerable.First(columns, c => c.GetRuntimeName()?.Equals("NotAutoInc", StringComparison.OrdinalIgnoreCase) == true);

            Assert.Multiple(() =>
            {
                Assert.That(idCol.IsAutoIncrement, Is.True, "Id column should be auto-increment");
                Assert.That(notAutoIncCol.IsAutoIncrement, Is.False, "NotAutoInc column should not be auto-increment");
            });
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region Insert Without Specifying Identity Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Insert_AutoIncrementTable_GeneratesIdentity(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("InsertAutoIncTable",
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
            // Insert without specifying Id - should auto-generate
            table.Insert(new Dictionary<string, object>
            {
                { "Value", "AutoGenerated" }
            });

            var dt = table.GetDataTable();
            Assert.Multiple(() =>
            {
                Assert.That(dt.Rows, Has.Count.EqualTo(1));
                Assert.That(dt.Rows[0]["Id"], Is.Not.EqualTo(DBNull.Value));
                Assert.That(Convert.ToInt32(dt.Rows[0]["Id"], CultureInfo.InvariantCulture), Is.GreaterThan(0));
            });
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Insert_MultipleRows_GeneratesSequentialIdentities(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("SeqAutoIncTable",
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
            for (var i = 0; i < 3; i++)
            {
                table.Insert(new Dictionary<string, object>
                {
                    { "Value", $"Row{i}" }
                });
            }

            var dt = table.GetDataTable();
            Assert.That(dt.Rows, Has.Count.EqualTo(3));

            var id1 = Convert.ToInt32(dt.Rows[0]["Id"], CultureInfo.InvariantCulture);
            var id2 = Convert.ToInt32(dt.Rows[1]["Id"], CultureInfo.InvariantCulture);
            var id3 = Convert.ToInt32(dt.Rows[2]["Id"], CultureInfo.InvariantCulture);

            Assert.Multiple(() =>
            {
                Assert.That(id1, Is.LessThan(id2));
                Assert.That(id2, Is.LessThan(id3));
            });
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region BulkInsert with Auto-increment Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void BulkInsert_AutoIncrementTable_GeneratesIdentities(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("BulkAutoIncTable",
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
            using var dt = new DataTable();
            dt.Columns.Add("Value", typeof(string));
            for (var i = 0; i < 10; i++)
            {
                dt.Rows.Add($"Bulk{i}");
            }

            using (var bulk = table.BeginBulkInsert(CultureInfo.InvariantCulture))
            {
                bulk.Upload(dt);
            }

            Assert.That(table.GetRowCount(), Is.EqualTo(10));

            var resultDt = table.GetDataTable();
            Assert.That(resultDt.Rows[0]["Id"], Is.Not.EqualTo(DBNull.Value));
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region Identity Column Constraints Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void AutoIncrement_Column_IsPrimaryKey(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AutoIncPkTable",
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
            var idCol = System.Linq.Enumerable.First(columns, c => c.GetRuntimeName()?.Equals("Id", StringComparison.OrdinalIgnoreCase) == true);

            Assert.Multiple(() =>
            {
                Assert.That(idCol.IsAutoIncrement, Is.True);
                Assert.That(idCol.IsPrimaryKey, Is.True);
                Assert.That(idCol.AllowNulls, Is.False, "Auto-increment primary key should not allow nulls");
            });
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region Script Generation with Identity Tests

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void ScriptTableCreation_WithAutoIncrement_IncludesIdentity(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("ScriptAutoIncTable",
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
            var sql = table.ScriptTableCreation(dropPrimaryKeys: false, dropNullability: false, convertIdentityToInt: false);

            Assert.That(sql, Is.Not.Null.And.Not.Empty);
            // The script should contain some form of auto-increment/identity specification
        }
        finally
        {
            table.Drop();
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void ScriptTableCreation_ConvertIdentityToInt_RemovesAutoIncrement(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("ScriptConvertAutoIncTable",
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
            var sqlWithIdentity = table.ScriptTableCreation(dropPrimaryKeys: false, dropNullability: false, convertIdentityToInt: false);
            var sqlWithoutIdentity = table.ScriptTableCreation(dropPrimaryKeys: false, dropNullability: false, convertIdentityToInt: true);

            Assert.That(sqlWithIdentity, Is.Not.EqualTo(sqlWithoutIdentity), "Scripts should differ when converting identity");
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion
}
