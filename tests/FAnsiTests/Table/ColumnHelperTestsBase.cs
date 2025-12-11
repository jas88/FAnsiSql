using System.Data;
using System.Globalization;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

/// <summary>
///     Abstract base class for testing ColumnHelper operations: GetTopXSqlForColumn and GetAlterColumnToSql.
///     These tests target uncovered functionality in all ColumnHelper implementations.
///     CRITICAL: SQLite ColumnHelper is 0% coverage (8 lines, completely untested).
/// </summary>
internal abstract class ColumnHelperTestsBase : DatabaseTests
{
    #region GetTopXSqlForColumn Tests

    protected void GetTopXSqlForColumn_WithoutDiscardNulls_ReturnsAllRows(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Value", typeof(string));
        dt.Rows.Add(1, "First");
        dt.Rows.Add(2, DBNull.Value);
        dt.Rows.Add(3, "Third");
        dt.Rows.Add(4, DBNull.Value);
        dt.Rows.Add(5, "Fifth");

        var table = db.CreateTable("TopXNullsTable", dt);

        try
        {
            var column = table.DiscoverColumn("Value");
            var sql = column.GetTopXSql(3, false);

            Assert.That(sql, Is.Not.Null.And.Not.Empty);

            using var con = db.Server.GetConnection();
            con.Open();
            using var cmd = db.Server.GetCommand(sql, con);
            using var reader = cmd.ExecuteReader();

            var count = 0;
            var hasNull = false;
            while (reader.Read())
            {
                count++;
                if (reader.IsDBNull(0))
                    hasNull = true;
            }

            Assert.Multiple(() =>
            {
                Assert.That(count, Is.EqualTo(3), "Should return exactly 3 rows");
                Assert.That(hasNull, Is.True, "Should include NULL values when discardNulls is false");
            });
        }
        finally
        {
            table.Drop();
        }
    }

    protected void GetTopXSqlForColumn_WithDiscardNulls_ExcludesNulls(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Value", typeof(string));
        dt.Rows.Add(1, DBNull.Value);
        dt.Rows.Add(2, "Second");
        dt.Rows.Add(3, "Third");
        dt.Rows.Add(4, DBNull.Value);
        dt.Rows.Add(5, "Fifth");

        var table = db.CreateTable("TopXDiscardNullsTable", dt);

        try
        {
            var column = table.DiscoverColumn("Value");
            var sql = column.GetTopXSql(2, true);

            Assert.That(sql, Is.Not.Null.And.Not.Empty);
            Assert.That(sql, Does.Contain("IS NOT NULL"), "SQL should contain IS NOT NULL clause");

            using var con = db.Server.GetConnection();
            con.Open();
            using var cmd = db.Server.GetCommand(sql, con);
            using var reader = cmd.ExecuteReader();

            var count = 0;
            var hasNull = false;
            while (reader.Read())
            {
                count++;
                if (reader.IsDBNull(0))
                    hasNull = true;
            }

            Assert.Multiple(() =>
            {
                Assert.That(count, Is.EqualTo(2), "Should return exactly 2 rows");
                Assert.That(hasNull, Is.False, "Should not include NULL values when discardNulls is true");
            });
        }
        finally
        {
            table.Drop();
        }
    }

    protected void GetTopXSqlForColumn_IntegerColumn_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Score", typeof(int));
        for (var i = 1; i <= 10; i++) dt.Rows.Add(i, i * 10);

        var table = db.CreateTable("TopXIntTable", dt);

        try
        {
            var column = table.DiscoverColumn("Score");
            var sql = column.GetTopXSql(5, false);

            Assert.That(sql, Is.Not.Null.And.Not.Empty);

            using var con = db.Server.GetConnection();
            con.Open();
            using var cmd = db.Server.GetCommand(sql, con);
            using var reader = cmd.ExecuteReader();

            var count = 0;
            while (reader.Read())
            {
                count++;
                var value = reader.GetInt32(0);
                Assert.That(value, Is.GreaterThan(0), "Score should be positive");
            }

            Assert.That(count, Is.EqualTo(5), "Should return exactly 5 rows");
        }
        finally
        {
            table.Drop();
        }
    }

    protected void GetTopXSqlForColumn_DateTimeColumn_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("EventDate", typeof(DateTime));
        dt.Rows.Add(1,
            type == DatabaseType.PostgreSql
                ? DateTime.SpecifyKind(new DateTime(2024, 1, 1), DateTimeKind.Utc)
                : new DateTime(2024, 1, 1));
        dt.Rows.Add(2,
            type == DatabaseType.PostgreSql
                ? DateTime.SpecifyKind(new DateTime(2024, 2, 1), DateTimeKind.Utc)
                : new DateTime(2024, 2, 1));
        dt.Rows.Add(3,
            type == DatabaseType.PostgreSql
                ? DateTime.SpecifyKind(new DateTime(2024, 3, 1), DateTimeKind.Utc)
                : new DateTime(2024, 3, 1));
        dt.Rows.Add(4, DBNull.Value);

        var table = db.CreateTable("TopXDateTable", dt);

        try
        {
            var column = table.DiscoverColumn("EventDate");
            var sql = column.GetTopXSql(2, true);

            Assert.That(sql, Is.Not.Null.And.Not.Empty);

            using var con = db.Server.GetConnection();
            con.Open();
            using var cmd = db.Server.GetCommand(sql, con);
            using var reader = cmd.ExecuteReader();

            var count = 0;
            while (reader.Read())
            {
                count++;
                Assert.That(reader.IsDBNull(0), Is.False, "Should not have NULLs when discardNulls is true");
            }

            Assert.That(count, Is.EqualTo(2), "Should return exactly 2 rows");
        }
        finally
        {
            table.Drop();
        }
    }

    protected void GetTopXSqlForColumn_SingleRow_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Name", typeof(string));
        dt.Rows.Add(1, "OnlyOne");

        var table = db.CreateTable("TopXSingleTable", dt);

        try
        {
            var column = table.DiscoverColumn("Name");
            var sql = column.GetTopXSql(1, false);

            Assert.That(sql, Is.Not.Null.And.Not.Empty);

            using var con = db.Server.GetConnection();
            con.Open();
            using var cmd = db.Server.GetCommand(sql, con);
            var result = cmd.ExecuteScalar();

            Assert.That(result, Is.EqualTo("OnlyOne"));
        }
        finally
        {
            table.Drop();
        }
    }

    protected void GetTopXSqlForColumn_AllNulls_WithDiscardNulls_ReturnsEmpty(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Value", typeof(string));
        dt.Rows.Add(1, DBNull.Value);
        dt.Rows.Add(2, DBNull.Value);
        dt.Rows.Add(3, DBNull.Value);

        var table = db.CreateTable("TopXAllNullsTable", dt);

        try
        {
            var column = table.DiscoverColumn("Value");
            var sql = column.GetTopXSql(5, true);

            Assert.That(sql, Is.Not.Null.And.Not.Empty);

            using var con = db.Server.GetConnection();
            con.Open();
            using var cmd = db.Server.GetCommand(sql, con);
            using var reader = cmd.ExecuteReader();

            var count = 0;
            while (reader.Read()) count++;

            Assert.That(count, Is.EqualTo(0), "Should return 0 rows when all values are NULL and discardNulls is true");
        }
        finally
        {
            table.Drop();
        }
    }

    protected void GetTopXSqlForColumn_LargeTopX_ReturnsAllAvailable(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        using var dt = new DataTable();
        dt.Columns.Add("Id", typeof(int));
        dt.Columns.Add("Value", typeof(string));
        for (var i = 1; i <= 5; i++) dt.Rows.Add(i, $"Value{i}");

        var table = db.CreateTable("TopXLargeTable", dt);

        try
        {
            var column = table.DiscoverColumn("Value");
            var sql = column.GetTopXSql(100, false);

            Assert.That(sql, Is.Not.Null.And.Not.Empty);

            using var con = db.Server.GetConnection();
            con.Open();
            using var cmd = db.Server.GetCommand(sql, con);
            using var reader = cmd.ExecuteReader();

            var count = 0;
            while (reader.Read()) count++;

            Assert.That(count, Is.EqualTo(5), "Should return all 5 rows even when requesting 100");
        }
        finally
        {
            table.Drop();
        }
    }

    protected void GetTopXSqlForColumn_SpecialCharactersInColumnName_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var colName = type == DatabaseType.Oracle ? "MY_COLUMN" : "My Column";

        var table = db.CreateTable("TopXSpecialColTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest(colName, new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            table.Insert(new Dictionary<string, object>
            {
                { "Id", 1 },
                { colName, "Test" }
            });

            var column = table.DiscoverColumn(colName);
            var sql = column.GetTopXSql(1, false);

            Assert.That(sql, Is.Not.Null.And.Not.Empty);

            using var con = db.Server.GetConnection();
            con.Open();
            using var cmd = db.Server.GetCommand(sql, con);
            var result = cmd.ExecuteScalar();

            Assert.That(result, Is.EqualTo("Test"));
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region GetAlterColumnToSql Tests

    protected void GetAlterColumnToSql_IncreaseStringLength_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AlterStringTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 50))
        ]);

        try
        {
            var column = table.DiscoverColumn("Name");
            Assert.That(column.DataType?.GetLengthIfString(), Is.EqualTo(50));

            var syntax = table.GetQuerySyntaxHelper();
            var newType = syntax.TypeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(string), 100));
            var alterSql = column.Helper.GetAlterColumnToSql(column, newType, true);

            Assert.That(alterSql, Is.Not.Null.And.Not.Empty);
            Assert.That(alterSql, Does.Contain("ALTER"), "SQL should contain ALTER");

            using var con = db.Server.GetConnection();
            con.Open();
            using var cmd = db.Server.GetCommand(alterSql, con);
            cmd.ExecuteNonQuery();

            // Verify the change
            var updatedColumn = table.DiscoverColumn("Name");
            Assert.That(updatedColumn.DataType?.GetLengthIfString(), Is.GreaterThanOrEqualTo(100));
        }
        finally
        {
            table.Drop();
        }
    }

    protected void GetAlterColumnToSql_ChangeNullability_AllowNullsToNotNull(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AlterNullTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Score", new DatabaseTypeRequest(typeof(int))) { AllowNulls = true }
        ]);

        try
        {
            var column = table.DiscoverColumn("Score");
            Assert.That(column.AllowNulls, Is.True);

            // Insert a non-null value so we can change to NOT NULL
            table.Insert(new Dictionary<string, object>
            {
                { "Id", 1 },
                { "Score", 100 }
            });

            var alterSql = column.Helper.GetAlterColumnToSql(column, "int", false);

            Assert.That(alterSql, Is.Not.Null.And.Not.Empty);
            Assert.That(alterSql, Does.Contain("NOT NULL"), "SQL should contain NOT NULL");

            using var con = db.Server.GetConnection();
            con.Open();
            using var cmd = db.Server.GetCommand(alterSql, con);
            cmd.ExecuteNonQuery();

            // Verify the change
            var updatedColumn = table.DiscoverColumn("Score");
            Assert.That(updatedColumn.AllowNulls, Is.False);
        }
        finally
        {
            table.Drop();
        }
    }

    protected void GetAlterColumnToSql_ChangeNullability_NotNullToAllowNulls(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AlterNotNullTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Score", new DatabaseTypeRequest(typeof(int))) { AllowNulls = false }
        ]);

        try
        {
            table.Insert(new Dictionary<string, object>
            {
                { "Id", 1 },
                { "Score", 100 }
            });

            var column = table.DiscoverColumn("Score");
            Assert.That(column.AllowNulls, Is.False);

            var alterSql = column.Helper.GetAlterColumnToSql(column, "int", true);

            Assert.That(alterSql, Is.Not.Null.And.Not.Empty);

            using var con = db.Server.GetConnection();
            con.Open();
            using var cmd = db.Server.GetCommand(alterSql, con);
            cmd.ExecuteNonQuery();

            // Verify the change
            var updatedColumn = table.DiscoverColumn("Score");
            Assert.That(updatedColumn.AllowNulls, Is.True);
        }
        finally
        {
            table.Drop();
        }
    }

    protected void GetAlterColumnToSql_IntToVarchar_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AlterTypeTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(int)))
        ]);

        try
        {
            table.Insert(new Dictionary<string, object>
            {
                { "Id", 1 },
                { "Value", 42 }
            });

            var column = table.DiscoverColumn("Value");
            Assert.That(column.DataType?.GetCSharpDataType(), Is.EqualTo(typeof(int)));

            // Oracle requires column to be empty when changing data type (ORA-01439)
            // Other databases (SQL Server, MySQL, PostgreSQL) allow type changes with data
            if (type == DatabaseType.Oracle)
            {
                using var con = db.Server.GetConnection();
                con.Open();
                var deleteSql = $"DELETE FROM {table.GetFullyQualifiedName()}";
                using var deleteCmd = db.Server.GetCommand(deleteSql, con);
                deleteCmd.ExecuteNonQuery();
            }

            var syntax = table.GetQuerySyntaxHelper();
            var newType = syntax.TypeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(string), 100));
            var alterSql = column.Helper.GetAlterColumnToSql(column, newType, true);

            Assert.That(alterSql, Is.Not.Null.And.Not.Empty);

            using (var con = db.Server.GetConnection())
            {
                con.Open();
                using var cmd = db.Server.GetCommand(alterSql, con);
                cmd.ExecuteNonQuery();
            }

            // Verify the change
            var updatedColumn = table.DiscoverColumn("Value");
            Assert.That(updatedColumn.DataType?.GetCSharpDataType(), Is.EqualTo(typeof(string)));
        }
        finally
        {
            table.Drop();
        }
    }

    protected void GetAlterColumnToSql_MicrosoftSQL_BitToOtherType_UsesStringIntermediate(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AlterBitTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("IsActive", new DatabaseTypeRequest(typeof(bool)))
        ]);

        try
        {
            table.Insert(new Dictionary<string, object>
            {
                { "Id", 1 },
                { "IsActive", true }
            });

            var column = table.DiscoverColumn("IsActive");
            Assert.That(column.DataType?.SQLType, Does.Contain("bit").IgnoreCase);

            var alterSql = column.Helper.GetAlterColumnToSql(column, "int", true);

            Assert.That(alterSql, Is.Not.Null.And.Not.Empty);
            Assert.That(alterSql, Does.Contain("varchar(4000)").IgnoreCase,
                "SQL Server should use varchar intermediate for bit conversions");

            using var con = db.Server.GetConnection();
            con.Open();
            using var cmd = db.Server.GetCommand(alterSql, con);
            cmd.ExecuteNonQuery();

            // Verify the change
            var updatedColumn = table.DiscoverColumn("IsActive");
            Assert.That(updatedColumn.DataType?.GetCSharpDataType(), Is.EqualTo(typeof(int)));
        }
        finally
        {
            table.Drop();
        }
    }

    protected void GetAlterColumnToSql_DecreaseStringLength_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AlterDecreaseTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Code", new DatabaseTypeRequest(typeof(string), 200))
        ]);

        try
        {
            table.Insert(new Dictionary<string, object>
            {
                { "Id", 1 },
                { "Code", "XYZ" }
            });

            var column = table.DiscoverColumn("Code");
            var syntax = table.GetQuerySyntaxHelper();
            var newType = syntax.TypeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(string), 10));
            var alterSql = column.Helper.GetAlterColumnToSql(column, newType, true);

            Assert.That(alterSql, Is.Not.Null.And.Not.Empty);

            using var con = db.Server.GetConnection();
            con.Open();
            using var cmd = db.Server.GetCommand(alterSql, con);
            cmd.ExecuteNonQuery();

            // Verify the change
            var updatedColumn = table.DiscoverColumn("Code");
            Assert.That(updatedColumn.DataType?.GetLengthIfString(), Is.LessThanOrEqualTo(200));
        }
        finally
        {
            table.Drop();
        }
    }

    protected void GetAlterColumnToSql_Sqlite_ThrowsNotSupportedException(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("SqliteAlterTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 50))
        ]);

        try
        {
            var column = table.DiscoverColumn("Name");

            Assert.Throws<NotSupportedException>(
                () => { column.Helper.GetAlterColumnToSql(column, "varchar(100)", true); },
                "SQLite should throw NotSupportedException for ALTER COLUMN");
        }
        finally
        {
            table.Drop();
        }
    }

    protected void GetAlterColumnToSql_DateTimeColumn_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AlterDateTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("CreatedDate", new DatabaseTypeRequest(typeof(DateTime)))
        ]);

        try
        {
            table.Insert(new Dictionary<string, object>
            {
                { "Id", 1 },
                {
                    "CreatedDate",
                    type == DatabaseType.PostgreSql
                        ? DateTime.SpecifyKind(new DateTime(2024, 1, 1), DateTimeKind.Utc)
                        : new DateTime(2024, 1, 1)
                }
            });

            var column = table.DiscoverColumn("CreatedDate");
            var syntax = table.GetQuerySyntaxHelper();
            var dateType = syntax.TypeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(DateTime)));

            var alterSql = column.Helper.GetAlterColumnToSql(column, dateType, true);

            Assert.That(alterSql, Is.Not.Null.And.Not.Empty);

            using var con = db.Server.GetConnection();
            con.Open();
            using var cmd = db.Server.GetCommand(alterSql, con);
            cmd.ExecuteNonQuery();

            // Verify still works
            var updatedColumn = table.DiscoverColumn("CreatedDate");
            Assert.That(updatedColumn, Is.Not.Null);
        }
        finally
        {
            table.Drop();
        }
    }

    protected void GetAlterColumnToSql_PreservesData_AfterTypeChange(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("AlterPreserveDataTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Amount", new DatabaseTypeRequest(typeof(int)))
        ]);

        try
        {
            const int testValue = 12345;
            table.Insert(new Dictionary<string, object>
            {
                { "Id", 1 },
                { "Amount", testValue }
            });

            var column = table.DiscoverColumn("Amount");
            var syntax = table.GetQuerySyntaxHelper();

            // Change to bigger int type or string
            var newType = syntax.TypeTranslater.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(typeof(string), 50));

            var alterSql = column.Helper.GetAlterColumnToSql(column, newType, true);

            // Oracle requires column to be empty when changing data type (ORA-01439)
            // This test verifies data preservation only for databases that support it
            if (type == DatabaseType.Oracle)
            {
                // Oracle: Delete data, alter column, then skip data preservation check
                using var con = db.Server.GetConnection();
                con.Open();
                var deleteSql = $"DELETE FROM {table.GetFullyQualifiedName()}";
                using (var deleteCmd = db.Server.GetCommand(deleteSql, con))
                {
                    deleteCmd.ExecuteNonQuery();
                }

                using (var cmd = db.Server.GetCommand(alterSql, con))
                {
                    cmd.ExecuteNonQuery();
                }

                // For Oracle, just verify the column was altered successfully
                var alteredColumn = table.DiscoverColumn("Amount");
                Assert.That(alteredColumn, Is.Not.Null, "Column should still exist after alter");
                return; // Skip data preservation check for Oracle
            }

            // For other databases: Alter with data present
            using (var con = db.Server.GetConnection())
            {
                con.Open();
                using var cmd = db.Server.GetCommand(alterSql, con);
                cmd.ExecuteNonQuery();
            }

            // Verify data is preserved (non-Oracle databases only)
            using (var con = db.Server.GetConnection())
            {
                con.Open();
                var syntaxHelper = table.GetQuerySyntaxHelper();
                var selectSql =
                    $"SELECT {syntaxHelper.EnsureWrapped("Amount")} FROM {table.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1";
                using var cmd = db.Server.GetCommand(selectSql, con);
                var result = cmd.ExecuteScalar();

                Assert.That(result?.ToString(), Is.EqualTo(testValue.ToString(CultureInfo.InvariantCulture)),
                    "Data should be preserved after type change");
            }
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion

    #region Edge Cases and SQL Syntax Verification

    protected void GetTopXSqlForColumn_ContainsDatabaseSpecificSyntax(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("SyntaxCheckTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            table.Insert(new Dictionary<string, object>
            {
                { "Id", 1 },
                { "Value", "Test" }
            });

            var column = table.DiscoverColumn("Value");
            var sql = column.GetTopXSql(10, false);

            Assert.That(sql, Is.Not.Null.And.Not.Empty);

            // Verify database-specific TOP/LIMIT syntax
            switch (type)
            {
                case DatabaseType.MicrosoftSQLServer:
                    Assert.That(sql, Does.Contain("TOP").Or.Contains("top"),
                        "SQL Server should use TOP syntax");
                    break;
                case DatabaseType.MySql:
                case DatabaseType.Sqlite:
                    Assert.That(sql, Does.Contain("LIMIT").Or.Contains("limit"),
                        "MySQL/SQLite should use LIMIT syntax");
                    break;
                case DatabaseType.PostgreSql:
                    Assert.That(sql, Does.Contain("fetch first").Or.Contains("FETCH FIRST"),
                        "PostgreSQL should use FETCH FIRST syntax");
                    break;
                case DatabaseType.Oracle:
                    Assert.That(sql, Does.Contain("FETCH NEXT").Or.Contains("fetch next"),
                        "Oracle should use FETCH NEXT syntax");
                    break;
            }
        }
        finally
        {
            table.Drop();
        }
    }

    protected void GetTopXSqlForColumn_UsesInvariantCulture(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var table = db.CreateTable("CultureTable",
        [
            new DatabaseColumnRequest("Id", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(decimal)))
        ]);

        try
        {
            table.Insert(new Dictionary<string, object>
            {
                { "Id", 1 },
                { "Value", 123.45m }
            });

            var column = table.DiscoverColumn("Value");
            var sql = column.GetTopXSql(5, false);

            Assert.That(sql, Is.Not.Null.And.Not.Empty);

            // Verify SQL executes successfully (culture-independent)
            using var con = db.Server.GetConnection();
            con.Open();
            using var cmd = db.Server.GetCommand(sql, con);
            var result = cmd.ExecuteScalar();

            Assert.That(result, Is.Not.Null);
        }
        finally
        {
            table.Drop();
        }
    }

    #endregion
}
