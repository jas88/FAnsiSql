using System.Data;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

internal sealed class BasicInsertTests : DatabaseTests
{
    public void CreateTableAndInsertAValue_ColumnOverload(DatabaseType type, object value)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("InsertTable",
        [
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(value.GetType(), 100, new DecimalSize(5, 5)))
        ]);

        var nameCol = tbl.DiscoverColumn("Name");

        tbl.Insert(new Dictionary<DiscoveredColumn, object>
        {
            { nameCol, value }
        });

        var result = tbl.GetDataTable();
        Assert.That(result.Rows, Has.Count.EqualTo(1));
        Assert.That(result.Rows[0][0], Is.EqualTo(value));

        tbl.Drop();
    }

    public void CreateTableAndInsertAValue_StringOverload(DatabaseType type, object value)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("InsertTable",
        [
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(value.GetType(), 100, new DecimalSize(5, 5)))
        ]);

        tbl.Insert(new Dictionary<string, object>
        {
            { "Name", value }
        });

        var result = tbl.GetDataTable();
        Assert.That(result.Rows, Has.Count.EqualTo(1));
        Assert.That(result.Rows[0][0], Is.EqualTo(value));

        tbl.Drop();
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void CreateTableAndInsertAValue_ReturnsIdentity(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("InsertTable",
        [
            new DatabaseColumnRequest("myidentity", new DatabaseTypeRequest(typeof(int)))
                { IsPrimaryKey = true, IsAutoIncrement = true },
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        var nameCol = tbl.DiscoverColumn("Name");

        var result = tbl.Insert(new Dictionary<DiscoveredColumn, object>
        {
            { nameCol, "fish" }
        });

        Assert.That(result, Is.EqualTo(1));


        result = tbl.Insert(new Dictionary<DiscoveredColumn, object>
        {
            { nameCol, "fish" }
        });

        Assert.That(result, Is.EqualTo(2));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Insert_DateTime_RoundTrips(DatabaseType type) =>
        AssertInsertedDateRoundTrips(type, new DateTime(2001, 1, 2, 3, 4, 5));

    /// <summary>
    ///     A 'timestamp' column has no time zone so the value read back must be the value that was written regardless of the
    ///     <see cref="DateTimeKind" /> and of the time zones of the client and server.
    /// </summary>
    [TestCase(DateTimeKind.Local)]
    [TestCase(DateTimeKind.Utc)]
    public void Insert_DateTime_PostgreSql_KindDoesNotChangeStoredValue(DateTimeKind kind) =>
        AssertInsertedDateRoundTrips(DatabaseType.PostgreSql, new DateTime(2001, 1, 2, 3, 4, 5, kind));

    [Test]
    public void Insert_DateTimeString_PostgreSql_RoundTrips() =>
        AssertInsertedDateRoundTrips(DatabaseType.PostgreSql, "2001-01-02 03:04:05");

    private void AssertInsertedDateRoundTrips(DatabaseType type, object value)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("InsertDateTable",
        [
            new DatabaseColumnRequest("MyDate", new DatabaseTypeRequest(typeof(DateTime)))
        ]);

        try
        {
            tbl.Insert(new Dictionary<string, object>
            {
                { "MyDate", value }
            });

            using var expected = new DataTable();
            expected.Columns.Add("MyDate", typeof(DateTime));
            expected.Rows.Add(new DateTime(2001, 1, 2, 3, 4, 5));

            using var actual = tbl.GetDataTable();
            AssertAreEqual(expected, actual);
        }
        finally
        {
            tbl.Drop();
        }
    }
}
