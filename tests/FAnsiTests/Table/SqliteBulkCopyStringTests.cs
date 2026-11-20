using System.Data;
using System.Globalization;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

/// <summary>
///     How SQLite bulk insert stores strings, and dates going into string columns
/// </summary>
internal sealed class SqliteBulkCopyStringTests : DatabaseTests
{
    /// <summary>
    ///     A string column must store a string that looks like a date or a time exactly as given.  SQLite bulk insert used to
    ///     parse every string bound for a TEXT column (what FAnsi creates for Unicode strings and strings of unlimited width)
    ///     and store any that parsed as a date as an ISO 8601 date and time: "12:30" became today's date at 12:30 and
    ///     "1/2/2003" became "2003-01-02 00:00:00".
    /// </summary>
    [Test]
    public void TestBulkInsert_Sqlite_DateLikeStringsInStringColumns_StoredUnchanged()
    {
        string[] columns = ["UnicodeString", "UnlimitedString", "DeclaredText", "SizedString"];
        string[] values = ["12:30", "1/2/2003", "2003-01-02", "13:11:10", "28/2/1993 5:36:27 AM", "Wed 22 May 2019"];

        var db = GetTestDatabase(DatabaseType.Sqlite);
        var tbl = db.CreateTable("DateLikeStrings",
        [
            new DatabaseColumnRequest(columns[0], new DatabaseTypeRequest(typeof(string), 100) { Unicode = true }),
            new DatabaseColumnRequest(columns[1], new DatabaseTypeRequest(typeof(string), int.MaxValue)),
            new DatabaseColumnRequest(columns[2], "TEXT"),
            new DatabaseColumnRequest(columns[3], new DatabaseTypeRequest(typeof(string), 100))
        ]);

        Assert.Multiple(() =>
        {
            Assert.That(tbl.DiscoverColumn(columns[0]).DataType?.SQLType, Is.EqualTo("TEXT"));
            Assert.That(tbl.DiscoverColumn(columns[1]).DataType?.SQLType, Is.EqualTo("TEXT"));
        });

        using (var dt = new DataTable())
        {
            foreach (var column in columns)
                dt.Columns.Add(column);

            foreach (var value in values)
                dt.Rows.Add(value, value, value, value);

            using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
            bulk.Upload(dt);
        }

        using var result = tbl.GetDataTable();
        Assert.That(result.Rows.Cast<DataRow>().Select(static r => r.ItemArray),
            Is.EquivalentTo(values.Select(static v => new object[] { v, v, v, v })));
    }

    /// <summary>
    ///     A <see cref="DateTime" /> is a date whatever column it goes into, so in a SQLite TEXT column (such as a date column
    ///     created by an earlier version of FAnsi, which declared dates as TEXT) it is stored as ISO 8601 text.
    /// </summary>
    [Test]
    public void TestBulkInsert_Sqlite_DateTimeIntoTextColumn_StoredAsIso8601()
    {
        var db = GetTestDatabase(DatabaseType.Sqlite);
        var tbl = db.CreateTable("DatesAsText", [new DatabaseColumnRequest("MyDate", "TEXT")]);

        using (var dt = new DataTable())
        {
            dt.Columns.Add("MyDate", typeof(DateTime));
            dt.Rows.Add(new DateTime(2003, 1, 2, 12, 30, 0));

            using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
            bulk.Upload(dt);
        }

        using var result = tbl.GetDataTable();
        Assert.That(result.Rows[0]["MyDate"], Is.EqualTo("2003-01-02 12:30:00"));
    }
}
