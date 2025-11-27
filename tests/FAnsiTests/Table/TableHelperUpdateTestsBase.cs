using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

/// <summary>
/// Comprehensive tests for Update operations to improve coverage from 0-17% to 60%+.
/// Tests cover UpdateHelper implementations, WHERE clauses, JOINs, NULL handling, and edge cases.
/// </summary>
internal abstract class TableHelperUpdateTestsBase : DatabaseTests
{
    #region Basic Update Tests

    protected void UpdateWithJoin_SingleColumn_UpdatesCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Name");
            dt1.Columns.Add("Score");

            dt1.Rows.Add("Alice", 100);
            dt1.Rows.Add("Bob", 200);
            dt1.Rows.Add("Charlie", 300);

            tbl1 = db.CreateTable("Scores", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("Name");
            dt2.Columns.Add("NewScore");
            dt2.Rows.Add("Alice", 150);
            dt2.Rows.Add("Bob", 250);

            tbl2 = db.CreateTable("Updates", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Score")} = t2.{syntaxHelper.EnsureWrapped("NewScore")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Name")} = t2.{syntaxHelper.EnsureWrapped("Name")}", QueryComponent.JoinInfoJoin)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
            {
                var rowsAffected = cmd.ExecuteNonQuery();
                Assert.That(rowsAffected, Is.EqualTo(2), "Should update 2 rows");
            }

            // Verify Alice's score was updated
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Score")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Name")} = 'Alice'", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(150));

            // Verify Bob's score was updated
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Score")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Name")} = 'Bob'", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(250));

            // Verify Charlie's score was NOT updated
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Score")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Name")} = 'Charlie'", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(300));
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    protected void UpdateWithJoin_MultipleColumns_UpdatesCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("Name");
            dt1.Columns.Add("Age", typeof(int));

            // Use initial values as long as the update values to avoid VARCHAR truncation in MySQL
            dt1.Rows.Add(1, "AliceXX", 25);
            dt1.Rows.Add(2, "RobertX", 30);

            tbl1 = db.CreateTable("People", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("PersonId", typeof(int));
            dt2.Columns.Add("NewName");
            dt2.Columns.Add("NewAge", typeof(int));

            dt2.Rows.Add(1, "Alicia", 26);
            dt2.Rows.Add(2, "Robert", 31);

            tbl2 = db.CreateTable("PeopleUpdates", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Name")} = t2.{syntaxHelper.EnsureWrapped("NewName")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Age")} = t2.{syntaxHelper.EnsureWrapped("NewAge")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("PersonId")}", QueryComponent.JoinInfoJoin)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
            {
                var rowsAffected = cmd.ExecuteNonQuery();
                Assert.That(rowsAffected, Is.EqualTo(2), "Should update 2 rows");
            }

            // Verify both columns were updated for Alice
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Name")}, {syntaxHelper.EnsureWrapped("Age")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
            using (var reader = cmd.ExecuteReader())
            {
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader[0], Is.EqualTo("Alicia"));
                Assert.That(reader[1], Is.EqualTo(26));
            }
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    #endregion

    #region WHERE Clause Tests

    protected void UpdateWithJoin_WithWhereClause_FiltersCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("Value", typeof(int));

            dt1.Rows.Add(1, 10);
            dt1.Rows.Add(2, 20);
            dt1.Rows.Add(3, 30);

            tbl1 = db.CreateTable("Numbers", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("NumId", typeof(int));
            dt2.Columns.Add("NewValue", typeof(int));

            dt2.Rows.Add(1, 100);
            dt2.Rows.Add(2, 200);
            dt2.Rows.Add(3, 300);

            tbl2 = db.CreateTable("NumberUpdates", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            // Only update rows where t1.Value < 25
            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Value")} = t2.{syntaxHelper.EnsureWrapped("NewValue")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("NumId")}", QueryComponent.JoinInfoJoin),
                new($"t1.{syntaxHelper.EnsureWrapped("Value")} < 25", QueryComponent.WHERE)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
            {
                var rowsAffected = cmd.ExecuteNonQuery();
                Assert.That(rowsAffected, Is.EqualTo(2), "Should update 2 rows (values 10 and 20)");
            }

            // Verify updates
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(100), "Id=1 should be updated");

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 2", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(200), "Id=2 should be updated");

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 3", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(30), "Id=3 should NOT be updated");
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    protected void UpdateWithJoin_MultipleWhereConditions_AppliesAllConditions(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("Status");
            dt1.Columns.Add("Value", typeof(int));

            dt1.Rows.Add(1, "Active", 10);
            dt1.Rows.Add(2, "Active", 20);
            dt1.Rows.Add(3, "Inactive", 30);

            tbl1 = db.CreateTable("Records", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("RecordId", typeof(int));
            dt2.Columns.Add("NewValue", typeof(int));

            dt2.Rows.Add(1, 100);
            dt2.Rows.Add(2, 200);
            dt2.Rows.Add(3, 300);

            tbl2 = db.CreateTable("RecordUpdates", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            // Update only where Status='Active' AND Value < 15
            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Value")} = t2.{syntaxHelper.EnsureWrapped("NewValue")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("RecordId")}", QueryComponent.JoinInfoJoin),
                new($"t1.{syntaxHelper.EnsureWrapped("Status")} = 'Active'", QueryComponent.WHERE),
                new($"t1.{syntaxHelper.EnsureWrapped("Value")} < 15", QueryComponent.WHERE)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
            {
                var rowsAffected = cmd.ExecuteNonQuery();
                Assert.That(rowsAffected, Is.EqualTo(1), "Should update only 1 row (Id=1)");
            }

            // Verify only Id=1 was updated
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(100));

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 2", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(20), "Should not be updated (Value >= 15)");

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 3", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(30), "Should not be updated (Status='Inactive')");
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    #endregion

    #region NULL Value Tests

    protected void UpdateWithJoin_SetToNull_UpdatesCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("Value");

            dt1.Rows.Add(1, "Something");
            dt1.Rows.Add(2, "Another");

            tbl1 = db.CreateTable("Data", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("DataId", typeof(int));
            dt2.Columns.Add("NewValue");

            dt2.Rows.Add(1, DBNull.Value);
            dt2.Rows.Add(2, "Updated");

            tbl2 = db.CreateTable("DataUpdates", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Value")} = t2.{syntaxHelper.EnsureWrapped("NewValue")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("DataId")}", QueryComponent.JoinInfoJoin)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
                cmd.ExecuteNonQuery();

            // Verify NULL was set correctly
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(DBNull.Value));

            // Verify non-NULL was set correctly
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 2", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo("Updated"));
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    protected void UpdateWithJoin_WhereIsNull_FiltersCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("Value");

            dt1.Rows.Add(1, DBNull.Value);
            dt1.Rows.Add(2, "Something");

            tbl1 = db.CreateTable("NullTest", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("TestId", typeof(int));
            dt2.Columns.Add("NewValue");

            dt2.Rows.Add(1, "Updated");
            dt2.Rows.Add(2, "AlsoUpdated");

            tbl2 = db.CreateTable("NullTestUpdates", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Value")} = t2.{syntaxHelper.EnsureWrapped("NewValue")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("TestId")}", QueryComponent.JoinInfoJoin),
                new($"t1.{syntaxHelper.EnsureWrapped("Value")} IS NULL", QueryComponent.WHERE)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
            {
                var rowsAffected = cmd.ExecuteNonQuery();
                Assert.That(rowsAffected, Is.EqualTo(1), "Should update only 1 row with NULL value");
            }

            // Verify NULL row was updated
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo("Updated"));

            // Verify non-NULL row was NOT updated
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 2", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo("Something"));
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    #endregion

    #region Self-Join Tests

    protected void UpdateWithJoin_SelfJoin_UpdatesCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl;

        using (var dt = new DataTable())
        {
            dt.Columns.Add("Id", typeof(int));
            dt.Columns.Add("ParentId", typeof(int));
            dt.Columns.Add("Value", typeof(int));

            dt.Rows.Add(1, DBNull.Value, 10);
            dt.Rows.Add(2, 1, 20);
            dt.Rows.Add(3, 1, 30);

            tbl = db.CreateTable("Hierarchy", dt);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            // Update children to have parent's value + their own
            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Value")} = t1.{syntaxHelper.EnsureWrapped("Value")} + t2.{syntaxHelper.EnsureWrapped("Value")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("ParentId")} = t2.{syntaxHelper.EnsureWrapped("Id")}", QueryComponent.JoinInfoJoin),
                new($"t1.{syntaxHelper.EnsureWrapped("ParentId")} IS NOT NULL", QueryComponent.WHERE)
            };

            var sql = updateHelper.BuildUpdate(tbl, tbl, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
            {
                var rowsAffected = cmd.ExecuteNonQuery();
                Assert.That(rowsAffected, Is.EqualTo(2), "Should update 2 child rows");
            }

            // Verify parent wasn't changed
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(10));

            // Verify children were updated
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 2", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(30), "20 + 10 (parent value)");

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 3", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(40), "30 + 10 (parent value)");
        }
        finally
        {
            tbl.Drop();
        }
    }

    #endregion

    #region Multiple Join Conditions Tests

    protected void UpdateWithJoin_MultipleJoinConditions_JoinsCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("FirstName");
            dt1.Columns.Add("LastName");
            dt1.Columns.Add("Score", typeof(int));

            dt1.Rows.Add("John", "Doe", 100);
            dt1.Rows.Add("John", "Smith", 200);
            dt1.Rows.Add("Jane", "Doe", 300);

            tbl1 = db.CreateTable("Employees", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("First");
            dt2.Columns.Add("Last");
            dt2.Columns.Add("NewScore", typeof(int));

            dt2.Rows.Add("John", "Doe", 150);
            dt2.Rows.Add("John", "Smith", 250);

            tbl2 = db.CreateTable("EmployeeUpdates", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            // Join on both FirstName and LastName
            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Score")} = t2.{syntaxHelper.EnsureWrapped("NewScore")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("FirstName")} = t2.{syntaxHelper.EnsureWrapped("First")}", QueryComponent.JoinInfoJoin),
                new($"t1.{syntaxHelper.EnsureWrapped("LastName")} = t2.{syntaxHelper.EnsureWrapped("Last")}", QueryComponent.JoinInfoJoin)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
            {
                var rowsAffected = cmd.ExecuteNonQuery();
                Assert.That(rowsAffected, Is.EqualTo(2), "Should update 2 rows that match both conditions");
            }

            // Verify correct rows were updated
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Score")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("FirstName")} = 'John' AND {syntaxHelper.EnsureWrapped("LastName")} = 'Doe'", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(150));

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Score")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("FirstName")} = 'John' AND {syntaxHelper.EnsureWrapped("LastName")} = 'Smith'", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(250));

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Score")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("FirstName")} = 'Jane' AND {syntaxHelper.EnsureWrapped("LastName")} = 'Doe'", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(300), "Should not be updated");
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    #endregion

    #region Data Type Tests

    protected void UpdateWithJoin_DateTimeValues_UpdatesCorrectly(DatabaseType dbType)
    {
        if (dbType == DatabaseType.Sqlite)
            Assert.Ignore("SQLite stores DateTime as TEXT strings; ADO.NET returns string type. Fundamental SQLite limitation.");

        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        var oldDate = new DateTime(2020, 1, 1, 12, 0, 0);
        var newDate = new DateTime(2024, 6, 15, 14, 30, 0);

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("EventDate", typeof(DateTime));

            dt1.Rows.Add(1, oldDate);
            dt1.Rows.Add(2, oldDate);

            tbl1 = db.CreateTable("Events", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("EventId", typeof(int));
            dt2.Columns.Add("NewDate", typeof(DateTime));

            dt2.Rows.Add(1, newDate);

            tbl2 = db.CreateTable("EventUpdates", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("EventDate")} = t2.{syntaxHelper.EnsureWrapped("NewDate")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("EventId")}", QueryComponent.JoinInfoJoin)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
                cmd.ExecuteNonQuery();

            // Verify date was updated
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("EventDate")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
            {
                var result = cmd.ExecuteScalar();
                Assert.That(result, Is.InstanceOf<DateTime>());
                Assert.That(((DateTime)result).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
                    Is.EqualTo(newDate.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)));
            }

            // Verify other row wasn't updated
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("EventDate")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 2", con))
            {
                var result = cmd.ExecuteScalar();
                Assert.That(((DateTime)result).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
                    Is.EqualTo(oldDate.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)));
            }
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    protected void UpdateWithJoin_DecimalValues_UpdatesCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("Price", typeof(decimal));

            dt1.Rows.Add(1, 10.50m);
            dt1.Rows.Add(2, 20.75m);

            tbl1 = db.CreateTable("Products", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("ProductId", typeof(int));
            dt2.Columns.Add("NewPrice", typeof(decimal));

            dt2.Rows.Add(1, 12.99m);
            dt2.Rows.Add(2, 25.49m);

            tbl2 = db.CreateTable("ProductUpdates", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Price")} = t2.{syntaxHelper.EnsureWrapped("NewPrice")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("ProductId")}", QueryComponent.JoinInfoJoin)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
                cmd.ExecuteNonQuery();

            // Verify prices were updated
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Price")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
            {
                var result = Convert.ToDecimal(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
                Assert.That(result, Is.EqualTo(12.99m));
            }

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Price")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 2", con))
            {
                var result = Convert.ToDecimal(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
                Assert.That(result, Is.EqualTo(25.49m));
            }
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    #endregion

    #region Special Character Tests

    protected void UpdateWithJoin_SpecialCharactersInData_UpdatesCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("Text");

            // Use initial values as long as the update values to avoid VARCHAR truncation in MySQL/Oracle
            dt1.Rows.Add(1, "InitialTextValue_"); // 17 chars to match "O'Reilly's \"Book\""
            dt1.Rows.Add(2, "LineOneLineTwo___"); // 17 chars to match "Line1\r\nLine2" (13 chars)

            tbl1 = db.CreateTable("TextData", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("TextId", typeof(int));
            dt2.Columns.Add("NewText");

            dt2.Rows.Add(1, "O'Reilly's \"Book\"");
            dt2.Rows.Add(2, "Line1\r\nLine2");

            tbl2 = db.CreateTable("TextUpdates", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Text")} = t2.{syntaxHelper.EnsureWrapped("NewText")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("TextId")}", QueryComponent.JoinInfoJoin)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
                cmd.ExecuteNonQuery();

            // Verify special characters were preserved
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Text")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo("O'Reilly's \"Book\""));

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Text")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 2", con))
            {
                var result = cmd.ExecuteScalar()?.ToString();
                Assert.That(result?.Replace("\r", "").Replace("\n", ""), Is.EqualTo("Line1Line2"));
            }
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    #endregion

    #region Edge Cases

    protected void UpdateWithJoin_NoMatchingRows_UpdatesNothing(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("Value");

            dt1.Rows.Add(1, "Original");
            dt1.Rows.Add(2, "Data");

            tbl1 = db.CreateTable("Source", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("DifferentId", typeof(int));
            dt2.Columns.Add("NewValue");

            dt2.Rows.Add(99, "Updated");
            dt2.Rows.Add(100, "Changed");

            tbl2 = db.CreateTable("SourceUpdates", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Value")} = t2.{syntaxHelper.EnsureWrapped("NewValue")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("DifferentId")}", QueryComponent.JoinInfoJoin)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
            {
                var rowsAffected = cmd.ExecuteNonQuery();
                Assert.That(rowsAffected, Is.EqualTo(0), "Should update 0 rows when no matches");
            }

            // Verify original data unchanged
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo("Original"));

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 2", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo("Data"));
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    protected void UpdateWithJoin_EmptyUpdateTable_UpdatesNothing(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("Value", typeof(string));

            dt1.Rows.Add(1, "Data1");
            dt1.Rows.Add(2, "Data2");

            tbl1 = db.CreateTable("MainTable", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("MainId", typeof(int));
            dt2.Columns.Add("NewValue", typeof(string));
            // No rows added - types must be explicit for empty DataTables to avoid TypeGuesser creating wrong types

            tbl2 = db.CreateTable("EmptyUpdates", dt2);
        }

        // DEBUG: Check what column types were actually created
        var tbl2Cols = tbl2.DiscoverColumns();
        TestContext.Out.WriteLine($"DEBUG tbl2 actual columns: {string.Join(", ", tbl2Cols.Select(c => $"{c.GetRuntimeName()}={c.DataType?.SQLType}"))}");

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Value")} = t2.{syntaxHelper.EnsureWrapped("NewValue")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("MainId")}", QueryComponent.JoinInfoJoin)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
            {
                var rowsAffected = cmd.ExecuteNonQuery();
                Assert.That(rowsAffected, Is.EqualTo(0), "Should update 0 rows when update table is empty");
            }

            // Verify original data unchanged
            using (var cmd = db.Server.GetCommand($"SELECT COUNT(*) FROM {tbl1.GetFullyQualifiedName()}", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(2));
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    protected void UpdateWithJoin_LargeDataSet_UpdatesCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("Value", typeof(int));

            for (var i = 1; i <= 1000; i++)
                dt1.Rows.Add(i, i * 10);

            tbl1 = db.CreateTable("LargeTable", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("LargeId", typeof(int));
            dt2.Columns.Add("NewValue", typeof(int));

            for (var i = 1; i <= 500; i++)
                dt2.Rows.Add(i, i * 100);

            tbl2 = db.CreateTable("LargeUpdates", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Value")} = t2.{syntaxHelper.EnsureWrapped("NewValue")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("LargeId")}", QueryComponent.JoinInfoJoin)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
            {
                var rowsAffected = cmd.ExecuteNonQuery();
                Assert.That(rowsAffected, Is.EqualTo(500), "Should update 500 rows");
            }

            // Verify first updated row
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(100));

            // Verify last updated row
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 500", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(50000));

            // Verify first non-updated row
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Value")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 501", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(5010));
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    #endregion

    #region Error Handling Tests

    protected void UpdateWithJoin_InvalidLocationToInsert_ThrowsException(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            tbl1 = db.CreateTable("Table1", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("Id", typeof(int));
            tbl2 = db.CreateTable("Table2", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            // Try to use an invalid QueryComponent (SELECT instead of SET/WHERE/JoinInfoJoin)
            var queryLines = new List<CustomLine>
            {
                new("t1.Value = 123", QueryComponent.SELECT)
            };

            Assert.Throws<NotSupportedException>(() => updateHelper.BuildUpdate(tbl1, tbl2, queryLines));
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    #endregion

    #region Complex Expression Tests

    protected void UpdateWithJoin_CalculatedExpression_UpdatesCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("Quantity", typeof(int));
            dt1.Columns.Add("Total", typeof(int));

            dt1.Rows.Add(1, 10, 0);
            dt1.Rows.Add(2, 20, 0);

            tbl1 = db.CreateTable("Orders", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("OrderId", typeof(int));
            dt2.Columns.Add("Price", typeof(int));

            dt2.Rows.Add(1, 5);
            dt2.Rows.Add(2, 3);

            tbl2 = db.CreateTable("Prices", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            // Calculate Total = Quantity * Price
            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Total")} = t1.{syntaxHelper.EnsureWrapped("Quantity")} * t2.{syntaxHelper.EnsureWrapped("Price")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("OrderId")}", QueryComponent.JoinInfoJoin)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
                cmd.ExecuteNonQuery();

            // Verify calculations
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Total")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(50), "10 * 5");

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Total")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 2", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(60), "20 * 3");
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    protected void UpdateWithJoin_CaseWhenExpression_UpdatesCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("Status");

            dt1.Rows.Add(1, "Unknown");
            dt1.Rows.Add(2, "Unknown");

            tbl1 = db.CreateTable("Items", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("ItemId", typeof(int));
            dt2.Columns.Add("Score", typeof(int));

            dt2.Rows.Add(1, 90);
            dt2.Rows.Add(2, 60);

            tbl2 = db.CreateTable("Scores", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            // Use CASE expression to set status based on score
            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Status")} = CASE WHEN t2.{syntaxHelper.EnsureWrapped("Score")} >= 80 THEN 'High' ELSE 'Low' END", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("ItemId")}", QueryComponent.JoinInfoJoin)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
                cmd.ExecuteNonQuery();

            // Verify CASE expression results
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Status")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo("High"), "Score 90 >= 80");

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Status")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 2", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo("Low"), "Score 60 < 80");
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    #endregion

    #region String Operation Tests

    protected void UpdateWithJoin_StringConcatenation_UpdatesCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("FullName");

            // Use initial values as long as the concatenated result to avoid VARCHAR truncation
            dt1.Rows.Add(1, "InitialName");
            dt1.Rows.Add(2, "AnotherName");

            tbl1 = db.CreateTable("People", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("PersonId", typeof(int));
            dt2.Columns.Add("FirstName");
            dt2.Columns.Add("LastName");

            dt2.Rows.Add(1, "John", "Doe");
            dt2.Rows.Add(2, "Jane", "Smith");

            tbl2 = db.CreateTable("Names", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            // Concatenate FirstName and LastName using database-specific syntax
            string concatExpression;
            if (dbType == DatabaseType.MicrosoftSQLServer)
            {
                concatExpression = $"t2.{syntaxHelper.EnsureWrapped("FirstName")} + ' ' + t2.{syntaxHelper.EnsureWrapped("LastName")}";
            }
            else if (dbType == DatabaseType.MySql)
            {
                // MySQL requires CONCAT() function as || is logical OR by default
                concatExpression = $"CONCAT(t2.{syntaxHelper.EnsureWrapped("FirstName")}, ' ', t2.{syntaxHelper.EnsureWrapped("LastName")})";
            }
            else
            {
                // PostgreSQL and Oracle support || for concatenation
                concatExpression = $"t2.{syntaxHelper.EnsureWrapped("FirstName")} || ' ' || t2.{syntaxHelper.EnsureWrapped("LastName")}";
            }

            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("FullName")} = {concatExpression}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("PersonId")}", QueryComponent.JoinInfoJoin)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
                cmd.ExecuteNonQuery();

            // Verify concatenation
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("FullName")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo("John Doe"));

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("FullName")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 2", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo("Jane Smith"));
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    #endregion

    #region OR Condition Tests

    protected void UpdateWithJoin_OrConditionInWhere_UpdatesCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("Category");
            dt1.Columns.Add("Flag", typeof(int));

            dt1.Rows.Add(1, "A", 0);
            dt1.Rows.Add(2, "B", 0);
            dt1.Rows.Add(3, "C", 0);

            tbl1 = db.CreateTable("Categories", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("CatId", typeof(int));
            dt2.Columns.Add("NewFlag", typeof(int));

            dt2.Rows.Add(1, 1);
            dt2.Rows.Add(2, 1);
            dt2.Rows.Add(3, 1);

            tbl2 = db.CreateTable("CategoryUpdates", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            // Update where Category = 'A' OR Category = 'B'
            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Flag")} = t2.{syntaxHelper.EnsureWrapped("NewFlag")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("CatId")}", QueryComponent.JoinInfoJoin),
                new($"t1.{syntaxHelper.EnsureWrapped("Category")} = 'A' OR t1.{syntaxHelper.EnsureWrapped("Category")} = 'B'", QueryComponent.WHERE)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
            {
                var rowsAffected = cmd.ExecuteNonQuery();
                Assert.That(rowsAffected, Is.EqualTo(2), "Should update rows with Category A or B");
            }

            // Verify updates
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Flag")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(1), "Category A should be updated");

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Flag")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 2", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(1), "Category B should be updated");

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Flag")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 3", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(0), "Category C should NOT be updated");
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    #endregion

    #region Comparison Operator Tests

    protected void UpdateWithJoin_LessThanComparison_UpdatesCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("OldValue", typeof(int));
            dt1.Columns.Add("CurrentValue", typeof(int));

            dt1.Rows.Add(1, 50, 100);
            dt1.Rows.Add(2, 150, 200);

            tbl1 = db.CreateTable("Comparisons", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("CompId", typeof(int));
            dt2.Columns.Add("NewValue", typeof(int));

            dt2.Rows.Add(1, 75);
            dt2.Rows.Add(2, 175);

            tbl2 = db.CreateTable("CompUpdates", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            // Update only if OldValue < NewValue < CurrentValue
            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("CurrentValue")} = t2.{syntaxHelper.EnsureWrapped("NewValue")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("CompId")}", QueryComponent.JoinInfoJoin),
                new($"t1.{syntaxHelper.EnsureWrapped("OldValue")} < t2.{syntaxHelper.EnsureWrapped("NewValue")}", QueryComponent.WHERE),
                new($"t2.{syntaxHelper.EnsureWrapped("NewValue")} < t1.{syntaxHelper.EnsureWrapped("CurrentValue")}", QueryComponent.WHERE)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
            {
                var rowsAffected = cmd.ExecuteNonQuery();
                Assert.That(rowsAffected, Is.EqualTo(2), "Both rows satisfy the condition");
            }

            // Verify updates
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("CurrentValue")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(75));

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("CurrentValue")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 2", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(175));
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    #endregion

    #region Boolean Value Tests

    protected void UpdateWithJoin_BooleanValues_UpdatesCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("IsActive", typeof(bool));

            dt1.Rows.Add(1, false);
            dt1.Rows.Add(2, true);

            tbl1 = db.CreateTable("Flags", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("FlagId", typeof(int));
            dt2.Columns.Add("NewActive", typeof(bool));

            dt2.Rows.Add(1, true);
            dt2.Rows.Add(2, false);

            tbl2 = db.CreateTable("FlagUpdates", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("IsActive")} = t2.{syntaxHelper.EnsureWrapped("NewActive")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("FlagId")}", QueryComponent.JoinInfoJoin)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
                cmd.ExecuteNonQuery();

            // Verify boolean updates (handle database-specific representations)
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("IsActive")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
            {
                var result = cmd.ExecuteScalar();
                Assert.That(Convert.ToBoolean(result, CultureInfo.InvariantCulture), Is.True);
            }

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("IsActive")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 2", con))
            {
                var result = cmd.ExecuteScalar();
                Assert.That(Convert.ToBoolean(result, CultureInfo.InvariantCulture), Is.False);
            }
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    #endregion

    #region IN Operator Tests

    protected void UpdateWithJoin_InOperator_UpdatesCorrectly(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Id", typeof(int));
            dt1.Columns.Add("Status");
            dt1.Columns.Add("Priority", typeof(int));

            dt1.Rows.Add(1, "New", 0);
            dt1.Rows.Add(2, "Active", 0);
            dt1.Rows.Add(3, "Closed", 0);
            dt1.Rows.Add(4, "Pending", 0);

            tbl1 = db.CreateTable("Tasks", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("TaskId", typeof(int));
            dt2.Columns.Add("NewPriority", typeof(int));

            dt2.Rows.Add(1, 1);
            dt2.Rows.Add(2, 1);
            dt2.Rows.Add(3, 1);
            dt2.Rows.Add(4, 1);

            tbl2 = db.CreateTable("TaskUpdates", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            // Update only tasks with Status IN ('New', 'Active')
            var queryLines = new List<CustomLine>
            {
                new($"t1.{syntaxHelper.EnsureWrapped("Priority")} = t2.{syntaxHelper.EnsureWrapped("NewPriority")}", QueryComponent.SET),
                new($"t1.{syntaxHelper.EnsureWrapped("Id")} = t2.{syntaxHelper.EnsureWrapped("TaskId")}", QueryComponent.JoinInfoJoin),
                new($"t1.{syntaxHelper.EnsureWrapped("Status")} IN ('New', 'Active')", QueryComponent.WHERE)
            };

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
            {
                var rowsAffected = cmd.ExecuteNonQuery();
                Assert.That(rowsAffected, Is.EqualTo(2), "Should update 2 rows");
            }

            // Verify updates
            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Priority")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 1", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(1), "New should be updated");

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Priority")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 2", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(1), "Active should be updated");

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Priority")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 3", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(0), "Closed should NOT be updated");

            using (var cmd = db.Server.GetCommand($"SELECT {syntaxHelper.EnsureWrapped("Priority")} FROM {tbl1.GetFullyQualifiedName()} WHERE {syntaxHelper.EnsureWrapped("Id")} = 4", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(0), "Pending should NOT be updated");
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    #endregion

    #region Existing Test (from UpdateTests.cs)

    protected void Test_UpdateTableFromJoin_OriginalTest(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        DiscoveredTable tbl1;
        DiscoveredTable tbl2;

        using (var dt1 = new DataTable())
        {
            dt1.Columns.Add("Name");
            dt1.Columns.Add("HighScore");

            dt1.Rows.Add("Dave", 100);
            dt1.Rows.Add("Frank", DBNull.Value);
            dt1.Rows.Add("Levo", DBNull.Value);

            tbl1 = db.CreateTable("HighScoresTable", dt1);
        }

        using (var dt2 = new DataTable())
        {
            dt2.Columns.Add("Name");
            dt2.Columns.Add("Score");
            dt2.Rows.Add("Dave", 50);
            dt2.Rows.Add("Frank", 900);

            tbl2 = db.CreateTable("NewScoresTable", dt2);
        }

        try
        {
            var syntaxHelper = db.Server.GetQuerySyntaxHelper();
            var updateHelper = syntaxHelper.UpdateHelper;

            var queryLines = new List<CustomLine>();

            var highScore = syntaxHelper.EnsureWrapped("HighScore");
            var score = syntaxHelper.EnsureWrapped("Score");
            var name = syntaxHelper.EnsureWrapped("Name");

            queryLines.Add(new CustomLine($"t1.{highScore} = t2.{score}", QueryComponent.SET));
            queryLines.Add(new CustomLine($"t1.{highScore} < t2.{score} OR t1.{highScore} is null", QueryComponent.WHERE));
            queryLines.Add(new CustomLine($"t1.{name} = t2.{name}", QueryComponent.JoinInfoJoin));

            var sql = updateHelper.BuildUpdate(tbl1, tbl2, queryLines);

            using var con = db.Server.GetConnection();
            con.Open();

            using (var cmd = db.Server.GetCommand(sql, con))
                Assert.That(cmd.ExecuteNonQuery(), Is.EqualTo(1));

            //Frank should have got a new high score of 900
            using (var cmd = db.Server.GetCommand($"SELECT {highScore} from {tbl1.GetFullyQualifiedName()} WHERE {name} = 'Frank'", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(900));

            //Dave should have his old score of 100
            using (var cmd = db.Server.GetCommand($"SELECT {highScore} from {tbl1.GetFullyQualifiedName()} WHERE {name} = 'Dave'", con))
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(100));
        }
        finally
        {
            tbl1.Drop();
            tbl2.Drop();
        }
    }

    #endregion
}
