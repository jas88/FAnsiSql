using System;
using System.Data;
using System.Globalization;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;

namespace FAnsiTests.Server;

/// <summary>
/// Tests that involve moving data between different database types.
/// These tests exercise cross-database compatibility.
/// </summary>
internal sealed class CrossDatabaseServerTests : DatabaseTests
{
    // Test all cross-database combinations
    // From SQL Server
    [TestCase(DatabaseType.MicrosoftSQLServer, DatabaseType.MySql)]
    [TestCase(DatabaseType.MicrosoftSQLServer, DatabaseType.Oracle)]
    [TestCase(DatabaseType.MicrosoftSQLServer, DatabaseType.PostgreSql)]
    [TestCase(DatabaseType.MicrosoftSQLServer, DatabaseType.Sqlite)]

    // From MySQL
    [TestCase(DatabaseType.MySql, DatabaseType.MicrosoftSQLServer)]
    [TestCase(DatabaseType.MySql, DatabaseType.Oracle)]
    [TestCase(DatabaseType.MySql, DatabaseType.PostgreSql)]
    [TestCase(DatabaseType.MySql, DatabaseType.Sqlite)]

    // From Oracle
    [TestCase(DatabaseType.Oracle, DatabaseType.MicrosoftSQLServer)]
    [TestCase(DatabaseType.Oracle, DatabaseType.MySql)]
    [TestCase(DatabaseType.Oracle, DatabaseType.PostgreSql)]
    [TestCase(DatabaseType.Oracle, DatabaseType.Sqlite)]

    // From PostgreSQL
    [TestCase(DatabaseType.PostgreSql, DatabaseType.MicrosoftSQLServer)]
    [TestCase(DatabaseType.PostgreSql, DatabaseType.MySql)]
    [TestCase(DatabaseType.PostgreSql, DatabaseType.Oracle)]
    [TestCase(DatabaseType.PostgreSql, DatabaseType.Sqlite)]

    // From SQLite
    [TestCase(DatabaseType.Sqlite, DatabaseType.MicrosoftSQLServer)]
    [TestCase(DatabaseType.Sqlite, DatabaseType.MySql)]
    [TestCase(DatabaseType.Sqlite, DatabaseType.Oracle)]
    [TestCase(DatabaseType.Sqlite, DatabaseType.PostgreSql)]
    public void MoveData_BetweenServerTypes(DatabaseType from, DatabaseType to)
    {
        //Create some test data
        using var dtToMove = new DataTable();
        dtToMove.Columns.Add("MyCol");
        dtToMove.Columns.Add("DateOfBirth");
        dtToMove.Columns.Add("Sanity");

        dtToMove.Rows.Add("Frank", new DateTime(2001, 01, 01), "0.50");
        dtToMove.Rows.Add("Tony", null, "9.99");
        dtToMove.Rows.Add("Jez", new DateTime(2001, 05, 01), "100.0");

        dtToMove.PrimaryKey = [dtToMove.Columns["MyCol"] ?? throw new InvalidOperationException()];

        //Upload it to the first database
        var fromDb = GetTestDatabase(from);
        var tblFrom = fromDb.CreateTable("MyTable", dtToMove);
        Assert.That(tblFrom.Exists());

        //Get pointer to the second database table (which doesn't exist yet)
        var toDb = GetTestDatabase(to);
        var toTable = toDb.ExpectTable("MyNewTable");
        Assert.That(toTable.Exists(), Is.False);

        //Get the clone table sql adjusted to work on the other DBMS
        var sql = tblFrom.ScriptTableCreation(false, false, false, toTable);

        //open connection and run the code to create the new table
        using (var con = toDb.Server.GetConnection())
        {
            con.Open();
            var cmd = toDb.Server.GetCommand(sql, con);
            cmd.ExecuteNonQuery();
        }

        //new table should exist
        Assert.That(tblFrom.Exists());

        using (var insert = toTable.BeginBulkInsert(CultureInfo.InvariantCulture))
        {
            //fetch the data from the source table
            var fromData = tblFrom.GetDataTable();

            //put it into the destination table
            insert.Upload(fromData);
        }

        Assert.Multiple(() =>
        {
            Assert.That(tblFrom.GetRowCount(), Is.EqualTo(3));
            Assert.That(toTable.GetRowCount(), Is.EqualTo(3));
        });

        AssertAreEqual(toTable.GetDataTable(), tblFrom.GetDataTable());
    }
}
