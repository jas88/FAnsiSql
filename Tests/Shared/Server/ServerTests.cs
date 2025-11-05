using System;
using System.Data;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementation;
using NUnit.Framework;

namespace FAnsiTests.Server;

internal sealed class ServerLevelTests : DatabaseTests
{
    [TestCaseSource(typeof(TestProjectDatabaseTypes), nameof(TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypes))]
    public void Server_Exists(DatabaseType type)
    {
        var server = GetTestServer(type);
        Assert.That(server.Exists(), "Server " + server + " did not exist");
    }


    [TestCaseSource(typeof(TestProjectDatabaseTypes), nameof(TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypes))]
    public void Server_Constructors(DatabaseType dbType)
    {
        var helper = ImplementationManager.GetImplementation(dbType).GetServerHelper();
        var server =
            new DiscoveredServer(
                helper.GetConnectionStringBuilder("localhost", null, "franko", "wacky").ConnectionString, dbType);

        Assert.That(server.Name, Is.EqualTo("localhost"));
    }

    [TestCaseSource(typeof(TestProjectDatabaseTypes), nameof(TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypes))]
    public void Server_RespondsWithinTime(DatabaseType type)
    {
        var server = GetTestServer(type);

        Assert.That(server.RespondsWithinTime(3, out _));
    }

    /// <summary>
    /// Tests systems ability to deal with missing information in the connection string
    /// </summary>
    /// <param name="type"></param>
    [TestCaseSource(typeof(TestProjectDatabaseTypes), nameof(TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypes))]
    public void ServerHelper_GetCurrentDatabase_WhenNoneSpecified(DatabaseType type)
    {
        var helper = ImplementationManager.GetImplementation(type).GetServerHelper();
        var builder = helper.GetConnectionStringBuilder("");
        var server = new DiscoveredServer(builder);

        Assert.Multiple(() =>
        {
            Assert.That(server.Name, Is.EqualTo(null));
            Assert.That(server.GetCurrentDatabase(), Is.EqualTo(null));
        });
    }

    [TestCaseSource(typeof(TestProjectDatabaseTypes), nameof(TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypes))]
    public void ServerHelper_GetConnectionStringBuilder(DatabaseType type)
    {
        var helper = ImplementationManager.GetImplementation(type).GetServerHelper();
        var builder = helper.GetConnectionStringBuilder("loco", "bob", "franko", "wacky");

        var server = new DiscoveredServer(builder);

        Assert.Multiple(() =>
        {
            // SQLite doesn't have traditional server names - it uses file paths
            // For SQLite, the "server" parameter becomes the database file path
            if (type == DatabaseType.Sqlite)
            {
                Assert.That(server.Name, Is.EqualTo("bob")); // SQLite treats database parameter as name
            }
            else
            {
                Assert.That(server.Name, Is.EqualTo("loco"));
            }

            //Oracle does not persist database in connection string
            Assert.That(server.GetCurrentDatabase()?.GetRuntimeName(), type == DatabaseType.Oracle ? Is.Null : Is.EqualTo("bob"));

            // SQLite doesn't support username/password authentication
            if (type == DatabaseType.Sqlite)
            {
                Assert.That(server.ExplicitUsernameIfAny, Is.Null);
                Assert.That(server.ExplicitPasswordIfAny, Is.Null);
            }
            else
            {
                Assert.That(server.ExplicitUsernameIfAny, Is.EqualTo("franko"));
                Assert.That(server.ExplicitPasswordIfAny, Is.EqualTo("wacky"));
            }
        });
    }


    [TestCaseSource(typeof(TestProjectDatabaseTypes), nameof(TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypesWithBoolFlags))]
    public void ServerHelper_GetConnectionStringBuilder_NoDatabase(DatabaseType type, bool useWhitespace)
    {
        var helper = ImplementationManager.GetImplementation(type).GetServerHelper();
        var builder = helper.GetConnectionStringBuilder("loco", useWhitespace ? "  " : null, "franko", "wacky");

        var server = new DiscoveredServer(builder);

        Assert.Multiple(() =>
        {
            // SQLite doesn't have traditional server names - it uses file paths
            // For SQLite, the "server" parameter becomes the database file path
            if (type == DatabaseType.Sqlite)
            {
                // When no database is specified for SQLite, server name should be used
                Assert.That(server.Name, Is.EqualTo("loco"));
            }
            else
            {
                Assert.That(server.Name, Is.EqualTo("loco"));
            }

            Assert.That(server.GetCurrentDatabase(), Is.Null);
        });

        Assert.Multiple(() =>
        {
            // SQLite doesn't support username/password authentication
            if (type == DatabaseType.Sqlite)
            {
                Assert.That(server.ExplicitUsernameIfAny, Is.Null);
                Assert.That(server.ExplicitPasswordIfAny, Is.Null);
            }
            else
            {
                Assert.That(server.ExplicitUsernameIfAny, Is.EqualTo("franko"));
                Assert.That(server.ExplicitPasswordIfAny, Is.EqualTo("wacky"));
            }
        });

        server = new DiscoveredServer("loco", useWhitespace ? "  " : null, type, "frank", "kangaro");
        Assert.Multiple(() =>
        {
            Assert.That(server.Name, Is.EqualTo("loco"));

            Assert.That(server.GetCurrentDatabase(), Is.Null);
        });


    }

    [TestCase(DatabaseType.MySql, false)]
    [TestCase(DatabaseType.MicrosoftSQLServer, false)]
    [TestCase(DatabaseType.Oracle, true)]
    [TestCase(DatabaseType.PostgreSql, false)]
    public void ServerHelper_ChangeDatabase(DatabaseType type, bool expectCaps)
    {
        var server = new DiscoveredServer("loco", "bob", type, "franko", "wacky");

        Assert.Multiple(() =>
        {
            Assert.That(server.Name, Is.EqualTo("loco"));

            //this failure is already exposed by Server_Helper_GetConnectionStringBuilder
            Assert.That(server.GetCurrentDatabase()?.GetRuntimeName(), Is.EqualTo(expectCaps ? "BOB" : "bob"));

            Assert.That(server.ExplicitUsernameIfAny, Is.EqualTo("franko"));
            Assert.That(server.ExplicitPasswordIfAny, Is.EqualTo("wacky"));
        });

        server.ChangeDatabase("omgggg");

        Assert.Multiple(() =>
        {
            Assert.That(server.Name, Is.EqualTo("loco"));

            Assert.That(server.GetCurrentDatabase()?.GetRuntimeName(), Is.EqualTo(expectCaps ? "OMGGGG" : "omgggg"));
            Assert.That(server.ExplicitUsernameIfAny, Is.EqualTo("franko"));
            Assert.That(server.ExplicitPasswordIfAny, Is.EqualTo("wacky"));
        });
    }


    /// <summary>
    /// Checks the API for <see cref="DiscoveredServer"/> respects both changes using the API and direct user changes made
    /// to <see cref="DiscoveredServer.Builder"/>
    /// </summary>
    /// <param name="type"></param>
    /// <param name="useApiFirst"></param>
    [TestCaseSource(typeof(TestProjectDatabaseTypes), nameof(TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypesWithBoolFlags))]
    public void ServerHelper_ChangeDatabase_AdHoc(DatabaseType type, bool useApiFirst)
    {
        if (type == DatabaseType.Oracle)
            Assert.Inconclusive("FAnsiSql understanding of Database cannot be encoded in DbConnectionStringBuilder sadly so we can end up with DiscoveredServer with no GetCurrentDatabase");

        //create initial server reference
        var helper = ImplementationManager.GetImplementation(type).GetServerHelper();
        var server = new DiscoveredServer(helper.GetConnectionStringBuilder("loco", "bob", "franko", "wacky"));
        Assert.Multiple(() =>
        {
            Assert.That(server.Name, Is.EqualTo("loco"));
            Assert.That(server.GetCurrentDatabase()?.GetRuntimeName(), Is.EqualTo("bob"));
        });

        //Use API to change databases
        if (useApiFirst)
        {
            server.ChangeDatabase("omgggg");
            Assert.Multiple(() =>
            {
                Assert.That(server.Name, Is.EqualTo("loco"));
                Assert.That(server.GetCurrentDatabase()?.GetRuntimeName(), Is.EqualTo("omgggg"));
            });
        }

        //adhoc changes to builder
        server.Builder["Database"] = "Fisss";
        Assert.Multiple(() =>
        {
            Assert.That(server.Name, Is.EqualTo("loco"));
            Assert.That(server.GetCurrentDatabase()?.GetRuntimeName(), Is.EqualTo("Fisss"));
        });

        server.Builder["Server"] = "Amagad";
        Assert.Multiple(() =>
        {
            Assert.That(server.Name, Is.EqualTo("Amagad"));
            Assert.That(server.GetCurrentDatabase()?.GetRuntimeName(), Is.EqualTo("Fisss"));
        });
    }

    [TestCase(DatabaseType.MicrosoftSQLServer, DatabaseType.MySql)]
    [TestCase(DatabaseType.MySql, DatabaseType.MicrosoftSQLServer)]
    [TestCase(DatabaseType.MicrosoftSQLServer, DatabaseType.PostgreSql)]
    [TestCase(DatabaseType.PostgreSql, DatabaseType.MicrosoftSQLServer)]

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

        using (var insert = toTable.BeginBulkInsert())
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

    [TestCaseSource(typeof(TestProjectDatabaseTypes), nameof(TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypes))]
    public void TestServer_GetVersion(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType, false);
        var ver = db.Server.GetVersion();

        TestContext.Out.WriteLine($"Version:{ver}");
        Assert.That(ver, Is.Not.Null);
        Assert.That(ver.Major, Is.GreaterThan(0));
    }
}
