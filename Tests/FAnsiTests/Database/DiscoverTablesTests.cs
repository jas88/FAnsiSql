using System;
using System.Linq;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Database;

internal sealed class DiscoverTablesTests : DatabaseTests
{
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_DiscoverTables_Normal(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        db.CreateTable("AA",
            [
                new DatabaseColumnRequest("F",new DatabaseTypeRequest(typeof(int)))
            ]);

        db.CreateTable("BB",
            [
                new DatabaseColumnRequest("F",new DatabaseTypeRequest(typeof(int)))
            ]);

        var tbls = db.DiscoverTables(false);

        Assert.That(tbls, Has.Length.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(tbls.Count(static t => t.GetRuntimeName().Equals("AA", StringComparison.OrdinalIgnoreCase)), Is.EqualTo(1));
            Assert.That(tbls.Count(static t => t.GetRuntimeName().Equals("BB", StringComparison.OrdinalIgnoreCase)), Is.EqualTo(1));
        });

    }
    /// <summary>
    /// Tests that <see cref="DiscoveredDatabase.DiscoverTables"/> correctly discovers tables with special characters
    /// in their names (parentheses, brackets, etc.) when using quoted identifiers.
    ///
    /// After removing IllegalNameChars validation, ALL databases now support special characters in quoted identifiers.
    /// Tables like "BB (ff)" and "FF (troll)" should be discoverable across all database types.
    /// </summary>
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_DiscoverTables_WithSpecialCharacters(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        //FAnsi now allows creating tables with special characters in quoted identifiers
        CreateBadTable(db);

        // After removing IllegalNameChars validation, ALL databases now support special characters in quoted identifiers
        db.CreateTable("FF (troll)",
            [
                new DatabaseColumnRequest("F", new DatabaseTypeRequest(typeof(int)))
            ]);

        // Both tables should be discovered for all databases
        var tbls = db.DiscoverTables(false);
        Assert.That(tbls, Has.Length.EqualTo(2));
        Assert.That(tbls.Count(static t => t.GetRuntimeName().Equals("FF (troll)", StringComparison.OrdinalIgnoreCase)), Is.EqualTo(1));
        Assert.That(tbls.Count(static t => t.GetRuntimeName().Equals("BB (ff)", StringComparison.OrdinalIgnoreCase)), Is.EqualTo(1));

        DropBadTable(db, false);
    }

    /// <summary>
    /// Tests that <see cref="DiscoveredDatabase.DiscoverTables"/> correctly discovers views with special characters
    /// in their names when using quoted identifiers. Similar to <see cref="Test_DiscoverTables_WithSpecialCharacters"/>
    /// but for views instead of tables.
    /// </summary>
    /// <param name="dbType"></param>
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_DiscoverViews_WithSpecialCharacters(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        //FAnsi now allows creating views with special characters in quoted identifiers
        CreateBadView(db);

        // After removing IllegalNameChars validation, ALL databases support special characters
        // CreateBadView creates a view "BB (ff)", which should be discoverable on all databases
        db.CreateTable("FF",
            [
                new DatabaseColumnRequest("F",new DatabaseTypeRequest(typeof(int)))
            ]);

        var tbls = db.DiscoverTables(true);

        //should be 3 objects: table "ABC", view "BB (ff)", table "FF"
        Assert.That(tbls, Has.Length.EqualTo(3));

        Assert.Multiple(() =>
        {
            // View with special characters should be discovered on all databases
            Assert.That(tbls.Count(static t => t.TableType == TableType.View), Is.EqualTo(1));
            Assert.That(tbls.Count(static t => t.GetRuntimeName().Equals("BB (ff)", StringComparison.OrdinalIgnoreCase)), Is.EqualTo(1));
            Assert.That(tbls.Count(static t => t.GetRuntimeName().Equals("FF", StringComparison.OrdinalIgnoreCase)), Is.EqualTo(1));
            Assert.That(tbls.Count(static t => t.GetRuntimeName().Equals("ABC", StringComparison.OrdinalIgnoreCase)), Is.EqualTo(1));
        });

        DropBadView(db, false);
    }

    private static void DropBadTable(DiscoveredDatabase db, bool ignoreFailure)
    {
        using var con = db.Server.GetConnection();
        con.Open();
        var cmd = db.Server.GetCommand($"DROP TABLE {GetBadTableName(db)}", con);
        try
        {
            cmd.ExecuteNonQuery();
        }
        catch (Exception)
        {
            if (!ignoreFailure)
                throw;

            //TestContext.Out.WriteLine("Drop table failed, this is expected, since FAnsi won't see this dodgy table name we can't drop it as normal before tests");
        }
    }

    private static string GetBadTableName(DiscoveredDatabase db) =>
        db.Server.DatabaseType switch
        {
            DatabaseType.MicrosoftSQLServer => "[BB (ff)]",
            DatabaseType.MySql => "`BB (ff)`",
            DatabaseType.Oracle => $"{db.GetRuntimeName()}.\"BB (ff)\"",
            DatabaseType.PostgreSql => $"\"{db.GetRuntimeName()}\".public.\"BB (ff)\"",
            DatabaseType.Sqlite => "\"BB (ff)\"",
            _ => throw new ArgumentOutOfRangeException(nameof(db), db.Server.DatabaseType, $"Unknown database type {db.Server.DatabaseType}")
        };

    private static void CreateBadTable(DiscoveredDatabase db)
    {
        //drop it if it exists
        DropBadTable(db, true);

        using var con = db.Server.GetConnection();
        con.Open();
        var cmd = db.Server.GetCommand($"CREATE TABLE {GetBadTableName(db)} (A int not null)", con);
        cmd.ExecuteNonQuery();
    }


    private static void DropBadView(DiscoveredDatabase db, bool ignoreFailure)
    {

        using (var con = db.Server.GetConnection())
        {
            con.Open();
            var cmd = db.Server.GetCommand($"DROP VIEW {GetBadTableName(db)}", con);
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (Exception)
            {
                if (!ignoreFailure)
                    throw;

                //TestContext.Out.WriteLine("Drop view failed, this is expected, since FAnsi won't see this dodgy table name we can't drop it as normal before tests");
            }
        }

        //the table that the view reads from
        var abc = db.ExpectTable("ABC");
        if (abc.Exists())
            abc.Drop();

    }


    private static void CreateBadView(DiscoveredDatabase db)
    {
        //drop it if it exists
        DropBadView(db, true);

        db.CreateTable("ABC", [new DatabaseColumnRequest("A", new DatabaseTypeRequest(typeof(int)))]);

        using var con = db.Server.GetConnection();
        con.Open();

        var viewname = db.Server.GetQuerySyntaxHelper().EnsureWrapped("ABC");

        var cmd = db.Server.GetCommand($"CREATE VIEW {GetBadTableName(db)} as select * from {viewname}", con);
        cmd.ExecuteNonQuery();
    }
}
