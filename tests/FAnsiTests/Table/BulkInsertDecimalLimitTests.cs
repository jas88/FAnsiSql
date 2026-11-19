using System.Data;
using System.Globalization;
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
}
