using System.Data;
using System.Globalization;
using System.Numerics;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

/// <summary>
///     Bulk insert rejects a value for a decimal(p,s) column whose absolute value is 10^(p-s) or more
/// </summary>
internal sealed class BulkInsertDecimalLimitTests : DatabaseTests
{
    /// <summary>
    ///     decimal(29,1) holds 28 digits before the decimal point. Its largest value, 9999999999999999999999999999.9,
    ///     is too long for a C# decimal and was rounded to 10^28, which was then allowed although it has 29 digits.
    /// </summary>
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypesExceptSqlite))]
    public void TestBulkInsert_Decimal29Scale1_RejectsTenToThe28(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var tbl = db.CreateTable("MyBulkInsertTest",
        [
            new DatabaseColumnRequest("Score", new DatabaseTypeRequest(typeof(decimal), null, new DecimalSize(28, 1)))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Score", typeof(decimal));
        dt.Rows.Add(9999999999999999999999999999m); // 28 nines

        using (var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture))
            bulk.Upload(dt);

        Assert.That(tbl.GetRowCount(), Is.EqualTo(1));

        dt.Rows.Clear();
        dt.Rows.Add(10000000000000000000000000000m); // 10^28

        using var tooLarge = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        var ex = Assert.Catch<Exception>(() => tooLarge.Upload(dt), "Expected 10^28 to be rejected");

        Assert.That(ex?.Message,
            Does.Contain("Value 10000000000000000000000000000 in column 'Score' (row 1) exceeds the maximum allowed " +
                         "for decimal(29,1)"));
        Assert.That(ex?.Message, Does.Contain("Maximum value is 9999999999999999999999999999.9."));
    }

    /// <summary>
    ///     PostgreSQL 15 and later and Oracle allow a scale larger than the precision: numeric(3,5) holds 3 digits
    ///     from the third place after the decimal point, so up to 0.00999. Its DecimalSize has 5 digits after the point
    ///     and none before, which allowed values up to 0.99999.
    /// </summary>
    [TestCase(DatabaseType.PostgreSql, "numeric(3,5)")]
    [TestCase(DatabaseType.Oracle, "NUMBER(3,5)")]
    public void TestBulkInsert_ScaleLargerThanPrecision_RejectsHalf(DatabaseType type, string sqlType)
    {
        var db = GetTestDatabase(type);
        if (type == DatabaseType.PostgreSql && db.Server.GetVersion()?.Major < 15)
            Assert.Ignore("PostgreSQL allows a scale larger than the precision from version 15");

        var tbl = db.CreateTable("MyBulkInsertTest", [new DatabaseColumnRequest("Score", sqlType)]);

        using var dt = new DataTable();
        dt.Columns.Add("Score", typeof(decimal));
        dt.Rows.Add(0.00999m);

        using (var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture))
            bulk.Upload(dt);

        Assert.That(tbl.GetDataTable().Rows[0]["Score"], Is.EqualTo(0.00999m));

        dt.Rows.Clear();
        dt.Rows.Add(0.5m);

        using var tooLarge = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        var ex = Assert.Catch<Exception>(() => tooLarge.Upload(dt), "Expected 0.5 to be rejected");

        Assert.That(ex?.Message,
            Does.Contain("Value 0.5 in column 'Score' (row 1) exceeds the maximum allowed for decimal(3,5)"));
        Assert.That(ex?.Message, Does.Contain("Maximum value is 0.00999."));
    }

    /// <summary>
    ///     The range of a decimal is symmetric, so the absolute value of decimal.MinValue is decimal.MaxValue
    /// </summary>
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypesExceptSqlite))]
    public void TestBulkInsert_DecimalMinAndMaxValue_InsertedWithNoLimit(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("MyBulkInsertTest", [new DatabaseColumnRequest("Score", "decimal(38,0)")]);

        using (var dt = new DataTable())
        {
            dt.Columns.Add("Score", typeof(decimal));
            dt.Rows.Add(decimal.MinValue);
            dt.Rows.Add(decimal.MaxValue);

            using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
            bulk.Upload(dt);
        }

        Assert.That(tbl.GetRowCount(), Is.EqualTo(2));
    }

    /// <summary>
    ///     A float or double at or over 10^(p-s) is rejected, however many digits the column has before the decimal
    ///     point: 1e30 is too large for decimal(29,0) although every decimal fits in it. 1e29 is the double nearest
    ///     10^29, and just below it, but is too large as its text is 1E+29. SQL Server does not take float columns at
    ///     all.
    /// </summary>
    [TestCaseSource(nameof(AtOrOverTheLimit))]
    public void TestBulkInsert_FloatingPointAtOrOverTheLimit_Rejected(DatabaseType type, bool isFloat, int precision,
        double value)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("MyBulkInsertTest", [new DatabaseColumnRequest("Score", $"decimal({precision},0)")]);

        using var dt = CreateFloatingPointTable(isFloat, value);
        var text = Convert.ToString(dt.Rows[0][0], CultureInfo.InvariantCulture);

        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        var ex = Assert.Throws<InvalidOperationException>(() => bulk.Upload(dt));

        Assert.That(ex?.Message, Is.EqualTo(
            $"Value {text} in column 'Score' (row 1) exceeds the maximum allowed for decimal({precision},0). " +
            $"Maximum value is {new string('9', precision)}."));
    }

    private static IEnumerable<TestCaseData> AtOrOverTheLimit()
    {
        foreach (var type in All.DatabaseTypesExceptSqlite)
        {
            bool[] floats = type == DatabaseType.MicrosoftSQLServer ? [false] : [false, true];
            foreach (var isFloat in floats)
            {
                yield return new TestCaseData(type, isFloat, 10, 1e30);
                yield return new TestCaseData(type, isFloat, 29, 1e30);
                yield return new TestCaseData(type, isFloat, 29, 1e29);
            }
        }
    }

    /// <summary>
    ///     A float or double under 10^(p-s) goes to the database even when it is too large for a decimal, which
    ///     converting it to would throw OverflowException: 1e30 into decimal(38,0), and the double and float just below
    ///     10^29 into decimal(29,0). Each DBMS stores the number of its shortest text: ODP.NET would round the double
    ///     just below 10^29 up to 10^29 itself, too large for NUMBER(29,0). SqlBulkCopy converts a double to decimal
    ///     itself, so it cannot write these to SQL Server.
    /// </summary>
    [TestCaseSource(nameof(UnderTheLimit))]
    public void TestBulkInsert_FloatingPointUnderTheLimit_Inserted(DatabaseType type, bool isFloat, int precision,
        double value)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("MyBulkInsertTest", [new DatabaseColumnRequest("Score", $"decimal({precision},0)")]);

        string text;
        using (var dt = CreateFloatingPointTable(isFloat, value))
        {
            text = Convert.ToString(dt.Rows[0][0], CultureInfo.InvariantCulture) ?? "";

            using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
            bulk.Upload(dt);
        }

        Assert.That(tbl.GetRowCount(), Is.EqualTo(1));

        // MySQL is sent the shortest text of the value, PostgreSQL the whole number of that text and Oracle its
        // NUMBER, which is what the value is checked as
        Assert.That(CountRowsWhere(tbl, "Score",
            $"= {BigInteger.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture)}"), Is.EqualTo(1));
    }

    private static IEnumerable<TestCaseData> UnderTheLimit()
    {
        foreach (var type in new[] { DatabaseType.MySql, DatabaseType.Oracle, DatabaseType.PostgreSql })
        {
            yield return new TestCaseData(type, false, 38, 1e30);
            yield return new TestCaseData(type, true, 38, 1e30);
            yield return new TestCaseData(type, false, 29, Math.BitDecrement(1e29));
            yield return new TestCaseData(type, true, 29, (double)MathF.BitDecrement(1e29f));
        }
    }

    /// <summary>
    ///     NaN and infinity do not fit in any decimal column, even one with no limit
    /// </summary>
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypesExceptSqlite))]
    public void TestBulkInsert_DoubleNotFinite_Rejected(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("MyBulkInsertTest", [new DatabaseColumnRequest("Score", "decimal(38,0)")]);
        var maxValue = new string('9', 38);

        Assert.Multiple(() =>
        {
            Assert.That(UploadAndCatch(tbl, double.NaN),
                Is.EqualTo("Value NaN in column 'Score' (row 1) is not a number, so it cannot be inserted into " +
                           "decimal(38,0)."));
            Assert.That(UploadAndCatch(tbl, double.PositiveInfinity),
                Is.EqualTo("Value Infinity in column 'Score' (row 1) exceeds the maximum allowed for decimal(38,0). " +
                           $"Maximum value is {maxValue}."));
            Assert.That(UploadAndCatch(tbl, double.NegativeInfinity),
                Is.EqualTo("Value -Infinity in column 'Score' (row 1) exceeds the maximum allowed for decimal(38,0). " +
                           $"Maximum value is {maxValue}."));
        });
    }

    /// <summary>
    ///     A PostgreSQL numeric column without a precision is not checked, and Npgsql cannot write NaN or infinity to
    ///     it but would otherwise throw OverflowException
    /// </summary>
    [Test]
    public void TestBulkInsert_PostgreSqlNumericWithoutPrecision_FloatingPoint()
    {
        var db = GetTestDatabase(DatabaseType.PostgreSql);
        var tbl = db.CreateTable("MyBulkInsertTest", [new DatabaseColumnRequest("Score", "numeric")]);

        using (var dt = CreateFloatingPointTable(false, 1e30))
        using (var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture))
            bulk.Upload(dt);

        Assert.Multiple(() =>
        {
            Assert.That(CountRowsWhere(tbl, "Score", "= 1000000000000000000000000000000"), Is.EqualTo(1));
            Assert.That(UploadAndCatch(tbl, double.NaN),
                Is.EqualTo("Value NaN cannot be bulk inserted into a numeric column: Npgsql writes numeric values " +
                           "from decimals"));
        });
    }

    private static DataTable CreateFloatingPointTable(bool isFloat, double value)
    {
        var dt = new DataTable();
        dt.Columns.Add("Score", isFloat ? typeof(float) : typeof(double));
        dt.Rows.Add(isFloat ? (object)(float)value : value);
        return dt;
    }

    /// <summary>
    ///     Uploads <paramref name="value" /> to <paramref name="tbl" /> and returns the message of the
    ///     InvalidOperationException that this throws
    /// </summary>
    private static string UploadAndCatch(DiscoveredTable tbl, double value)
    {
        using var dt = CreateFloatingPointTable(false, value);
        using var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture);
        return Assert.Throws<InvalidOperationException>(() => bulk.Upload(dt))?.Message ?? "";
    }

    private static int CountRowsWhere(DiscoveredTable tbl, string column, string condition)
    {
        using var con = tbl.Database.Server.GetConnection();
        con.Open();
        var sql =
            $"SELECT COUNT(*) FROM {tbl.GetFullyQualifiedName()} WHERE {tbl.DiscoverColumn(column).GetWrappedName()}";
        using var cmd = tbl.Database.Server.GetCommand($"{sql} {condition}", con);
        return Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
    }
}
