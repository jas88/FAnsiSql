using System.Data;
using System.Globalization;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;

namespace FAnsiTests.Table;

/// <summary>
///     MySqlConnector reads every bit column as a ulong.  A bit(n) column of more than one bit must be read, inserted and
///     bulk inserted as one, including values with the top bit set, while bit and bit(1) columns stay booleans.
/// </summary>
internal sealed class MySqlBitColumnTests : DatabaseTests
{
    private const DatabaseType DbType = DatabaseType.MySql;

    // One row with only the top bit set in each multi-bit column, and one with every bit set
    private static readonly object[][] Rows =
    [
        [true, (ulong)0x80, 1UL << 63],
        [false, (ulong)0xFF, ulong.MaxValue]
    ];

    [Test]
    public void DiscoverColumns_BitTypes_HaveTheTypeTheyAreReadAs()
    {
        var tbl = CreateBitTable();
        using var guesser = tbl.DiscoverColumn("MyBit64").GetGuesser();

        Assert.Multiple(() =>
        {
            Assert.That(tbl.DiscoverColumn("MyBit").DataType?.GetCSharpDataType(), Is.EqualTo(typeof(bool)));
            Assert.That(tbl.DiscoverColumn("MyBit8").DataType?.GetCSharpDataType(), Is.EqualTo(typeof(ulong)));
            Assert.That(tbl.DiscoverColumn("MyBit64").DataType?.GetCSharpDataType(), Is.EqualTo(typeof(ulong)));

            // TypeGuesser has no ulong, so a Guesser for a bit(n) column starts from long
            Assert.That(guesser.Guess.CSharpType, Is.EqualTo(typeof(long)));
        });
    }

    [Test]
    public void GetDataTable_BitValuesWithTopBitSet_AreReadExactly()
    {
        var tbl = CreateBitTable();

        // Bit literals, so that only reading the values is tested
        ExecuteNonQuery(tbl,
            $"INSERT INTO {tbl.GetFullyQualifiedName()} (MyBit, MyBit8, MyBit64) VALUES " +
            $"(b'1', b'10000000', b'1{new string('0', 63)}'), (b'0', b'11111111', b'{new string('1', 64)}')");

        AssertHasRows(tbl);
    }

    [Test]
    public void BulkInsert_BitValuesWithTopBitSet_RoundTrip()
    {
        var tbl = CreateBitTable();

        using (var dt = new DataTable())
        {
            dt.Columns.Add("MyBit", typeof(bool));
            dt.Columns.Add("MyBit8", typeof(ulong));
            dt.Columns.Add("MyBit64", typeof(ulong));
            foreach (var row in Rows)
                dt.Rows.Add(row);

            using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
            bulk.Upload(dt);
        }

        AssertHasRows(tbl);
    }

    [Test]
    public void Insert_BitValuesWithTopBitSet_RoundTrip()
    {
        var tbl = CreateBitTable();

        foreach (var row in Rows)
            tbl.Insert(new Dictionary<string, object>
            {
                ["MyBit"] = row[0],
                ["MyBit8"] = row[1],
                ["MyBit64"] = row[2]
            });

        AssertHasRows(tbl);
    }

    /// <summary>
    ///     Creating a table from a DataTable makes a Guesser for each explicitly typed column, and TypeGuesser throws for
    ///     a ulong, so the request for a bit(n) column must have a type it supports
    /// </summary>
    [Test]
    public void CreateTable_FromDataTableWithExplicitBitColumns_KeepsTheValues()
    {
        using var dt = new DataTable();
        dt.Columns.Add("MyBit8", typeof(ulong));
        dt.Columns.Add("MyBit64", typeof(ulong));
        dt.Rows.Add((ulong)0xFF, ulong.MaxValue);

        var tbl = GetTestDatabase(DbType).CreateTable("ExplicitBits", dt,
        [
            new DatabaseColumnRequest("MyBit8", "bit(8)"),
            new DatabaseColumnRequest("MyBit64", "bit(64)")
        ]);

        using var result = tbl.GetDataTable();

        Assert.Multiple(() =>
        {
            Assert.That(tbl.DiscoverColumn("MyBit64").DataType?.SQLType, Is.EqualTo("bit(64)"));
            Assert.That(result.Rows[0].ItemArray, Is.EqualTo(new object[] { (ulong)0xFF, ulong.MaxValue }));
        });
    }

    private DiscoveredTable CreateBitTable() =>
        GetTestDatabase(DbType).CreateTable("BitValues",
        [
            new DatabaseColumnRequest("MyBit", "bit"),
            new DatabaseColumnRequest("MyBit8", "bit(8)"),
            new DatabaseColumnRequest("MyBit64", "bit(64)")
        ]);

    private static void AssertHasRows(DiscoveredTable tbl)
    {
        using var dt = tbl.GetDataTable();

        Assert.Multiple(() =>
        {
            Assert.That(dt.Columns["MyBit"]?.DataType, Is.EqualTo(typeof(bool)));
            Assert.That(dt.Columns["MyBit8"]?.DataType, Is.EqualTo(typeof(ulong)));
            Assert.That(dt.Columns["MyBit64"]?.DataType, Is.EqualTo(typeof(ulong)));
            Assert.That(dt.Rows.Cast<DataRow>().Select(static r => r.ItemArray), Is.EquivalentTo(Rows));
        });
    }

    private static void ExecuteNonQuery(DiscoveredTable tbl, string sql)
    {
        var server = tbl.Database.Server;
        using var con = server.GetConnection();
        con.Open();
        using var cmd = server.GetCommand(sql, con);
        cmd.ExecuteNonQuery();
    }
}
