using System;
using System.Data;
using System.Globalization;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementation;
using NUnit.Framework;

namespace FAnsiTests.Server;

/// <summary>
/// Base class for server-level tests across all database types.
/// To add a new test: Add a protected method taking DatabaseType parameter.
/// Then add [Test] methods to each ServerTests_{Database}.cs file that call it.
/// </summary>
internal abstract class ServerTestsBase : DatabaseTests
{
    protected void Server_Exists(DatabaseType type)
    {
        var server = GetTestServer(type);
        Assert.That(server.Exists(), "Server " + server + " did not exist");
    }

    protected void Server_Constructors(DatabaseType dbType)
    {
        var helper = ImplementationManager.GetImplementation(dbType).GetServerHelper();
        var server =
            new DiscoveredServer(
                helper.GetConnectionStringBuilder("localhost", null, "franko", "wacky").ConnectionString, dbType);

        Assert.That(server.Name, Is.EqualTo("localhost"));
    }

    protected void Server_RespondsWithinTime(DatabaseType type)
    {
        var server = GetTestServer(type);

        Assert.That(server.RespondsWithinTime(3, out _));
    }

    /// <summary>
    /// Tests systems ability to deal with missing information in the connection string
    /// </summary>
    protected void ServerHelper_GetCurrentDatabase_WhenNoneSpecified(DatabaseType type)
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

    protected void ServerHelper_GetConnectionStringBuilder(DatabaseType type)
    {
        if (type == DatabaseType.Sqlite)
            Assert.Inconclusive("SQLite is file-based and doesn't have separate server/database concepts or username/password authentication");

        var helper = ImplementationManager.GetImplementation(type).GetServerHelper();
        var builder = helper.GetConnectionStringBuilder("loco", "bob", "franko", "wacky");

        var server = new DiscoveredServer(builder);

        Assert.Multiple(() =>
        {
            Assert.That(server.Name, Is.EqualTo("loco"));

            //Oracle does not persist database in connection string
            Assert.That(server.GetCurrentDatabase()?.GetRuntimeName(), type == DatabaseType.Oracle ? Is.Null : Is.EqualTo("bob"));
            Assert.That(server.ExplicitUsernameIfAny, Is.EqualTo("franko"));
            Assert.That(server.ExplicitPasswordIfAny, Is.EqualTo("wacky"));
        });
    }

    protected void ServerHelper_GetConnectionStringBuilder_NoDatabase(DatabaseType type, bool useWhitespace)
    {
        if (type == DatabaseType.Sqlite)
            Assert.Inconclusive("SQLite is file-based and doesn't have separate server/database concepts or username/password authentication");

        var helper = ImplementationManager.GetImplementation(type).GetServerHelper();
        var builder = helper.GetConnectionStringBuilder("loco", useWhitespace ? "  " : null, "franko", "wacky");

        var server = new DiscoveredServer(builder);

        Assert.Multiple(() =>
        {
            Assert.That(server.Name, Is.EqualTo("loco"));

            Assert.That(server.GetCurrentDatabase(), Is.Null);
        });

        Assert.Multiple(() =>
        {
            Assert.That(server.ExplicitUsernameIfAny, Is.EqualTo("franko"));
            Assert.That(server.ExplicitPasswordIfAny, Is.EqualTo("wacky"));
        });

        server = new DiscoveredServer("loco", useWhitespace ? "  " : null, type, "frank", "kangaro");
        Assert.Multiple(() =>
        {
            Assert.That(server.Name, Is.EqualTo("loco"));

            Assert.That(server.GetCurrentDatabase(), Is.Null);
        });
    }

    protected void ServerHelper_ChangeDatabase(DatabaseType type)
    {
        // Oracle uppercases database names
        bool expectCaps = type == DatabaseType.Oracle;

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
    protected void ServerHelper_ChangeDatabase_AdHoc(DatabaseType type, bool useApiFirst)
    {
        if (type is DatabaseType.Oracle or DatabaseType.Sqlite)
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

    protected void TestServer_GetVersion(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType, false);
        var ver = db.Server.GetVersion();

        TestContext.Out.WriteLine($"Version:{ver}");
        Assert.That(ver, Is.Not.Null);
        Assert.That(ver.Major, Is.GreaterThan(0));
    }
}
