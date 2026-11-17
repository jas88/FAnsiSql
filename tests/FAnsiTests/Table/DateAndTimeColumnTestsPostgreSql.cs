using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;

namespace FAnsiTests.Table;

/// <summary>
///     PostgreSQL 'date', 'time', 'time with time zone' and 'interval' columns.  Npgsql 10 reads a 'date' as a
///     <see cref="DateOnly" />, a 'time' as a <see cref="TimeOnly" /> and a 'time with time zone' as a
///     <see cref="DateTimeOffset" />.  FAnsi gives them the C# types the other DBMS read their dates and times as: a
///     'date' is a <see cref="DateTime" /> and the others are a <see cref="TimeSpan" />.
/// </summary>
internal sealed class DateAndTimeColumnTestsPostgreSql : DatabaseTests
{
    private const DatabaseType DbType = DatabaseType.PostgreSql;

    private static readonly DateTime ExpectedDate = new(2001, 2, 3);
    private static readonly TimeSpan ExpectedTime = new(13, 11, 10);

    private static readonly object[] DateValues =
    [
        new DateTime(2001, 2, 3),
        new DateTime(2001, 2, 3, 4, 5, 6),
        new DateTime(2001, 2, 3, 23, 5, 6, DateTimeKind.Utc),
        new DateOnly(2001, 2, 3),
        "2001-02-03"
    ];

    private static readonly object[] TimeValues =
    [
        new TimeSpan(13, 11, 10),
        new TimeOnly(13, 11, 10),
        "13:11:10"
    ];

    private static readonly string[] TimeColumnTypes =
    [
        "time",
        "time(3)",
        "time with time zone",
        "timetz"
    ];

    /// <summary>
    ///     Whatever the DBMS, a 'date' column is read as a <see cref="DateTime" /> and a 'time' column as a
    ///     <see cref="TimeSpan" />, by every way FAnsi has of reading a table into a <see cref="DataTable" />
    /// </summary>
    [TestCase(DatabaseType.MicrosoftSQLServer)]
    [TestCase(DatabaseType.MySql)]
    [TestCase(DatabaseType.PostgreSql)]
    public void DateAndTimeColumns_AreReadAsTheSameTypesOnEveryDbms(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("DateAndTimeTypes",
        [
            new DatabaseColumnRequest("MyDate", "date"),
            new DatabaseColumnRequest("MyTime", "time")
        ]);

        var syntax = tbl.GetQuerySyntaxHelper();
        var columns = $"{syntax.EnsureWrapped("MyDate")}, {syntax.EnsureWrapped("MyTime")}";

        using (var con = db.Server.GetConnection())
        {
            con.Open();
            using var cmd = db.Server.GetCommand(
                $"INSERT INTO {tbl.GetFullyQualifiedName()} ({columns}) VALUES ('2001-02-03', '13:11:10')", con);
            cmd.ExecuteNonQuery();
        }

        Assert.Multiple(() =>
        {
            Assert.That(tbl.DiscoverColumn("MyDate").DataType?.GetCSharpDataType(), Is.EqualTo(typeof(DateTime)));
            Assert.That(tbl.DiscoverColumn("MyTime").DataType?.GetCSharpDataType(), Is.EqualTo(typeof(TimeSpan)));
        });

        using (var typed = tbl.GetDataTable())
            AssertDateAndTime(typed, "GetDataTable()");

        using (var untyped = tbl.GetDataTable(enforceTypesAndNullness: false))
            AssertDateAndTime(untyped, "GetDataTable(enforceTypesAndNullness: false)");

        using (var con = db.Server.GetConnection())
        {
            con.Open();
            using var cmd = db.Server.GetCommand($"SELECT {columns} FROM {tbl.GetFullyQualifiedName()}", con);
            using var da = db.Server.GetDataAdapter(cmd);
            using var adapted = new DataTable();
            da.Fill(adapted);
            AssertDateAndTime(adapted, "GetDataAdapter");
        }

        tbl.Drop();
    }

    private static void AssertDateAndTime(DataTable dt, string how)
    {
        Assert.Multiple(() =>
        {
            Assert.That(dt.Columns["MyDate"]?.DataType, Is.EqualTo(typeof(DateTime)), how);
            Assert.That(dt.Columns["MyTime"]?.DataType, Is.EqualTo(typeof(TimeSpan)), how);
            Assert.That(dt.Rows[0]["MyDate"], Is.EqualTo(ExpectedDate), how);
            Assert.That(dt.Rows[0]["MyTime"], Is.EqualTo(ExpectedTime), how);
        });
    }

    [Test]
    public void DateColumn_IsDiscovered()
    {
        var db = GetTestDatabase(DbType);
        var tbl = db.CreateTable("DateColumn", [new DatabaseColumnRequest("MyDate", "date")]);

        var col = tbl.DiscoverColumn("MyDate");

        Assert.Multiple(() =>
        {
            Assert.That(col.DataType?.SQLType, Is.EqualTo("date"));
            Assert.That(col.DataType?.GetCSharpDataType(), Is.EqualTo(typeof(DateTime)));
            Assert.That(col.GetGuesser().Guess.CSharpType, Is.EqualTo(typeof(DateTime)));
        });

        tbl.Drop();
    }

    [TestCase("time", "time without time zone")]
    [TestCase("time(3)", "time without time zone")]
    [TestCase("time with time zone", "time with time zone")]
    [TestCase("timetz", "time with time zone")]
    public void TimeColumn_IsDiscovered(string sqlType, string expectedSqlType)
    {
        var db = GetTestDatabase(DbType);
        var tbl = db.CreateTable("TimeColumn", [new DatabaseColumnRequest("MyTime", sqlType)]);

        var col = tbl.DiscoverColumn("MyTime");

        Assert.Multiple(() =>
        {
            Assert.That(col.DataType?.SQLType, Is.EqualTo(expectedSqlType));
            Assert.That(col.DataType?.GetCSharpDataType(), Is.EqualTo(typeof(TimeSpan)));
            Assert.That(col.GetGuesser().Guess.CSharpType, Is.EqualTo(typeof(TimeSpan)));
        });

        tbl.Drop();
    }

    [TestCaseSource(nameof(DateValues))]
    public void DateColumn_InsertAndBulkInsert(object value)
    {
        var db = GetTestDatabase(DbType);
        var tbl = db.CreateTable("DateColumn", [new DatabaseColumnRequest("MyValue", "date")]);

        InsertAndBulkInsert(tbl, value);

        AssertReadAs(tbl, ExpectedDate);
        tbl.Drop();
    }

    [Test]
    public void TimeColumn_InsertAndBulkInsert([ValueSource(nameof(TimeColumnTypes))] string columnType,
        [ValueSource(nameof(TimeValues))] object value)
    {
        var db = GetTestDatabase(DbType);
        var tbl = db.CreateTable("TimeColumn", [new DatabaseColumnRequest("MyValue", columnType)]);

        InsertAndBulkInsert(tbl, value);

        AssertReadAs(tbl, ExpectedTime);
        tbl.Drop();
    }

    /// <summary>
    ///     A time keeps its fraction of a second, to the microsecond PostgreSQL stores
    /// </summary>
    [Test]
    public void TimeColumn_KeepsMicroseconds()
    {
        var value = new TimeSpan(0, 13, 11, 10, 123, 456);

        var db = GetTestDatabase(DbType);
        var tbl = db.CreateTable("TimeColumn", [new DatabaseColumnRequest("MyValue", "time")]);

        InsertAndBulkInsert(tbl, value);

        AssertReadAs(tbl, value);
        tbl.Drop();
    }

    /// <summary>
    ///     An 'interval' is read as a <see cref="TimeSpan" />, which need not be less than a day or positive
    /// </summary>
    [TestCase(1, 2, 3, 4)]
    [TestCase(0, 13, 11, 10)]
    [TestCase(-1, -2, -3, -4)]
    public void IntervalColumn_InsertAndBulkInsert(int days, int hours, int minutes, int seconds)
    {
        var value = new TimeSpan(days, hours, minutes, seconds);

        var db = GetTestDatabase(DbType);
        var tbl = db.CreateTable("IntervalColumn", [new DatabaseColumnRequest("MyValue", "interval")]);

        Assert.Multiple(() =>
        {
            Assert.That(tbl.DiscoverColumn("MyValue").DataType?.SQLType, Is.EqualTo("interval"));
            Assert.That(tbl.DiscoverColumn("MyValue").DataType?.GetCSharpDataType(), Is.EqualTo(typeof(TimeSpan)));
        });

        InsertAndBulkInsert(tbl, value);

        AssertReadAs(tbl, value);
        tbl.Drop();
    }

    /// <summary>
    ///     A <see cref="DataTable" /> read from one table can be bulk inserted into another table like it, and into a table
    ///     created from it, without changing any value
    /// </summary>
    [Test]
    public void DateAndTime_RoundTrip()
    {
        var db = GetTestDatabase(DbType);

        DatabaseColumnRequest[] columns =
        [
            new("MyDate", "date"),
            new("MyTime", "time"),
            new("MyTimeTz", "time with time zone"),
            new("MyInterval", "interval"),
            new("MyTimestamp", "timestamp"),
            new("MyTimestampTz", "timestamp with time zone")
        ];

        var source = db.CreateTable("DateAndTimeSource", columns);
        var copy = db.CreateTable("DateAndTimeCopy", columns);

        using (var dt = new DataTable())
        {
            dt.Columns.Add("MyDate", typeof(DateOnly));
            dt.Columns.Add("MyTime", typeof(TimeOnly));
            dt.Columns.Add("MyTimeTz", typeof(TimeSpan));
            dt.Columns.Add("MyInterval", typeof(TimeSpan));
            dt.Columns.Add("MyTimestamp", typeof(DateTime));
            dt.Columns.Add("MyTimestampTz", typeof(DateTime));
            dt.Rows.Add(new DateOnly(2001, 2, 3), new TimeOnly(13, 11, 10), ExpectedTime, new TimeSpan(1, 2, 3, 4),
                new DateTime(2001, 2, 3, 4, 5, 6), new DateTime(2001, 2, 3, 4, 5, 6));
            dt.Rows.Add(DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);

            using var bulk = source.BeginBulkInsert(CultureInfo.InvariantCulture);
            bulk.Upload(dt);
        }

        using var read = source.GetDataTable();

        Assert.Multiple(() =>
        {
            Assert.That(read.Columns["MyDate"]?.DataType, Is.EqualTo(typeof(DateTime)));
            Assert.That(read.Columns["MyTime"]?.DataType, Is.EqualTo(typeof(TimeSpan)));
            Assert.That(read.Columns["MyTimeTz"]?.DataType, Is.EqualTo(typeof(TimeSpan)));
            Assert.That(read.Columns["MyInterval"]?.DataType, Is.EqualTo(typeof(TimeSpan)));

            var row = read.Rows.Cast<DataRow>().Single(static r => r["MyDate"] != DBNull.Value);
            Assert.That(row["MyDate"], Is.EqualTo(ExpectedDate));
            Assert.That(row["MyTime"], Is.EqualTo(ExpectedTime));
            Assert.That(row["MyTimeTz"], Is.EqualTo(ExpectedTime));
            Assert.That(row["MyInterval"], Is.EqualTo(new TimeSpan(1, 2, 3, 4)));
            Assert.That(row["MyTimestamp"], Is.EqualTo(new DateTime(2001, 2, 3, 4, 5, 6)));
            Assert.That(row["MyTimestampTz"], Is.EqualTo(new DateTime(2001, 2, 3, 4, 5, 6)));
        });

        using (var bulk = copy.BeginBulkInsert(CultureInfo.InvariantCulture))
        {
            bulk.Upload(read);
        }

        Assert.That(GetStoredText(copy), Is.EquivalentTo(GetStoredText(source)));

        //and into a table of the types FAnsi creates for the C# types of the DataTable.  That is a 'time' for a
        //TimeSpan, which cannot hold an interval of a day or more
        read.Columns.Remove("MyInterval");
        var created = db.CreateTable("DateAndTimeCreated", read);
        using var readCreated = created.GetDataTable();
        AssertAreEqual(read, readCreated);

        created.Drop();
        copy.Drop();
        source.Drop();
    }

    /// <summary>
    ///     Reading a query into a <see cref="DataSet" />, or with the schema of the result, gives the same types
    /// </summary>
    [Test]
    [RequiresUnreferencedCode("Calls DbDataAdapter.FillSchema, whose schema table types cannot be statically analyzed")]
    public void DataAdapter_DataSetAndSchema()
    {
        var db = GetTestDatabase(DbType);

        const string sql =
            "SELECT CAST('2001-02-03' AS date) AS d, CAST('13:11:10' AS time) AS t, CAST('13:11:10+02' AS timetz) AS tz, CAST('1 day 02:03:04' AS interval) AS i";

        using var con = db.Server.GetConnection();
        con.Open();

        using (var da = db.Server.GetDataAdapter(sql, con))
        using (var ds = new DataSet())
        {
            da.Fill(ds);
            AssertTypes(ds.Tables[0], "Fill(DataSet)");
        }

        using (var da = db.Server.GetDataAdapter(sql, con))
        using (var schema = new DataTable())
        {
            da.FillSchema(schema, SchemaType.Source);
            Assert.That(schema.Columns.Cast<DataColumn>().Select(static c => c.DataType),
                Is.EqualTo(new[] { typeof(DateTime), typeof(TimeSpan), typeof(TimeSpan), typeof(TimeSpan) }),
                "FillSchema");
        }

        using (var da = db.Server.GetDataAdapter(sql, con))
        using (var withKey = new DataTable())
        {
            da.MissingSchemaAction = MissingSchemaAction.AddWithKey;
            da.Fill(withKey);
            AssertTypes(withKey, "MissingSchemaAction.AddWithKey");
        }

        static void AssertTypes(DataTable dt, string how)
        {
            Assert.Multiple(() =>
            {
                Assert.That(dt.Columns.Cast<DataColumn>().Select(static c => c.DataType),
                    Is.EqualTo(new[] { typeof(DateTime), typeof(TimeSpan), typeof(TimeSpan), typeof(TimeSpan) }), how);
                Assert.That(dt.Rows[0]["d"], Is.EqualTo(ExpectedDate), how);
                Assert.That(dt.Rows[0]["t"], Is.EqualTo(ExpectedTime), how);
                //the time of day as written, in its own offset
                Assert.That(dt.Rows[0]["tz"], Is.EqualTo(ExpectedTime), how);
                Assert.That(dt.Rows[0]["i"], Is.EqualTo(new TimeSpan(1, 2, 3, 4)), how);
            });
        }
    }

    /// <summary>
    ///     Inserts <paramref name="value" /> into the 'MyValue' column of <paramref name="tbl" /> twice, once as a parameter
    ///     and once by bulk insert from a <see cref="DataTable" /> column of the type of the value
    /// </summary>
    private static void InsertAndBulkInsert(DiscoveredTable tbl, object value)
    {
        tbl.Insert(new Dictionary<string, object> { { "MyValue", value } }, CultureInfo.InvariantCulture);

        using var dt = new DataTable();
        dt.Columns.Add(value switch
        {
            //by default a DataTable makes every DateTime it is given Unspecified, this mode keeps a Utc one Utc
            DateTime { Kind: DateTimeKind.Utc } => new DataColumn("MyValue", typeof(DateTime))
            {
                DateTimeMode = DataSetDateTime.Utc
            },
            DateTime => new DataColumn("MyValue", typeof(DateTime)),
            DateOnly => new DataColumn("MyValue", typeof(DateOnly)),
            TimeSpan => new DataColumn("MyValue", typeof(TimeSpan)),
            TimeOnly => new DataColumn("MyValue", typeof(TimeOnly)),
            string => new DataColumn("MyValue", typeof(string)),
            _ => throw new ArgumentOutOfRangeException(nameof(value))
        });
        dt.Rows.Add(value);

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        Assert.That(bulk.Upload(dt), Is.EqualTo(1));
    }

    /// <summary>
    ///     Asserts that every value in the 'MyValue' column of <paramref name="tbl" /> is <paramref name="expected" />
    ///     and of its type, whether or not <see cref="DiscoveredTable.GetDataTable(int,bool,FAnsi.Connections.IManagedTransaction)" />
    ///     types the <see cref="DataTable" /> from the table first
    /// </summary>
    private static void AssertReadAs(DiscoveredTable tbl, object expected)
    {
        foreach (var enforceTypes in new[] { true, false })
        {
            using var dt = tbl.GetDataTable(enforceTypesAndNullness: enforceTypes);

            Assert.Multiple(() =>
            {
                Assert.That(dt.Columns["MyValue"]?.DataType, Is.EqualTo(expected.GetType()),
                    $"enforceTypesAndNullness: {enforceTypes}");
                Assert.That(dt.Rows.Cast<DataRow>().Select(static r => r["MyValue"]),
                    Is.EqualTo(new[] { expected, expected }), $"enforceTypesAndNullness: {enforceTypes}");
            });
        }
    }

    /// <summary>
    ///     Returns every row of <paramref name="tbl" /> as the server formats it, which does not depend on any client side
    ///     conversion
    /// </summary>
    private static List<string> GetStoredText(DiscoveredTable tbl)
    {
        var rows = new List<string>();

        using var con = tbl.Database.Server.GetConnection();
        con.Open();
        using var cmd = tbl.Database.Server.GetCommand($"SELECT CAST(t AS text) FROM {tbl.GetFullyQualifiedName()} t",
            con);
        using var r = cmd.ExecuteReader();
        while (r.Read())
            rows.Add(r.GetString(0));

        return rows;
    }
}
