using System.Linq;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.TableCreation;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Parallel;

/// <summary>
/// PostgreSQL tests that run in parallel with other database type fixtures.
/// Tests within this fixture run sequentially on a single connection.
/// </summary>
[TestFixture]
[Parallelizable(ParallelScope.Self)]
public sealed class PostgreSqlParallelTests : SharedDatabaseTests
{
    protected override DatabaseType DatabaseType => DatabaseType.PostgreSql;

    [Test]
    public void CreateTable_Simple()
    {
        var db = GetDatabase();

        var table = db.CreateTable("TestTable",
        [
            new DatabaseColumnRequest("ID", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 50))
        ]);

        TrackForCleanup(table);

        Assert.That(table.Exists(), Is.True);
        Assert.That(table.DiscoverColumns().Count(), Is.EqualTo(2));
    }

    [Test]
    public void CreateTable_WithPrimaryKey()
    {
        var db = GetDatabase();

        var table = db.CreateTable("PKTable",
        [
            new DatabaseColumnRequest("ID", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Value", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        TrackForCleanup(table);

        Assert.That(table.Exists(), Is.True);

        var pk = table.DiscoverColumns().Single(c => c.GetRuntimeName() == "ID");
        Assert.That(pk.IsPrimaryKey, Is.True);
    }

    [Test]
    public void InsertAndSelect_Data()
    {
        var db = GetDatabase();

        var table = db.CreateTable("DataTable",
        [
            new DatabaseColumnRequest("ID", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 50))
        ]);

        TrackForCleanup(table);

        // Insert data
        using (var insert = table.Database.Server.GetCommand("INSERT INTO DataTable (ID, Name) VALUES (1, 'Test')", table.Database.Server.GetManagedConnection()))
        {
            insert.ExecuteNonQuery();
        }

        // Verify
        using (var select = table.Database.Server.GetCommand("SELECT COUNT(*) FROM DataTable", table.Database.Server.GetManagedConnection()))
        {
            var count = (int)(long)select.ExecuteScalar()!; // PostgreSQL returns long for COUNT(*)
            Assert.That(count, Is.EqualTo(1));
        }
    }

    [Test]
    public void DropTable_Works()
    {
        var db = GetDatabase();

        var table = db.CreateTable("DropMe",
        [
            new DatabaseColumnRequest("ID", new DatabaseTypeRequest(typeof(int)))
        ]);

        TrackForCleanup(table);

        Assert.That(table.Exists(), Is.True);

        table.Drop();

        Assert.That(table.Exists(), Is.False);
    }
}
