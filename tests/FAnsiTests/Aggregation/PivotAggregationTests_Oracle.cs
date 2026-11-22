using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class PivotAggregationTests_Oracle : PivotAggregationTestsBase
{
    private const DatabaseType DbType = DatabaseType.Oracle;

    [Test]
    public void Test_PivotOnlyCount() => Test_PivotOnlyCount(DbType);

    [Test]
    public void Test_Pivot_SameConnectionAfterFailedPivot() => Test_Pivot_SameConnectionAfterFailedPivot(DbType);

    [TestCase(false)]
    [TestCase(true)]
    public void Test_Pivot_OnlyNullOrEmptyValues(bool withAxis) => Test_Pivot_OnlyNullOrEmptyValues(DbType, withAxis);

    /// <summary>
    ///     A name cannot hold a double quote: a value made only of them gets its position as its name, and one that
    ///     loses its double quote or is cut to the most bytes a name can have gets its position as a suffix if an earlier
    ///     column has its name.  The names expected are those of a server whose names can have 128 bytes: Oracle 12.2 or
    ///     later with COMPATIBLE 12.2 or higher.
    /// </summary>
    [Test]
    public void Test_Pivot_ColumnNamesAreValidAndUnique() =>
        Test_Pivot_ColumnNamesAreValidAndUnique(DbType,
            ["T", "_2", "_3", "ab", "ab_5", "a`b]c'd", new string('x', 128), $"{new string('x', 126)}_8"]);

    [TestCase("DATE",
        new[] { "TIMESTAMP '1901-01-01 00:00:00'", "TIMESTAMP '2001-01-01 00:00:00'", "TIMESTAMP '2001-01-01 12:00:00'" },
        new[] { "1901-01-01 00:00:00", "2001-01-01 00:00:00", "2001-01-01 12:00:00" })]
    [TestCase("TIMESTAMP",
        new[] { "TIMESTAMP '1901-01-01 00:00:00'", "TIMESTAMP '2001-01-01 12:00:00'", "TIMESTAMP '2001-01-01 12:00:00.5'" },
        new[] { "1901-01-01 00:00:00", "2001-01-01 12:00:00", "2001-01-01 12:00:00.5" })]
    [TestCase("TIMESTAMP WITH TIME ZONE",
        new[]
        {
            "TIMESTAMP '1901-01-01 00:00:00 +00:00'", "TIMESTAMP '2001-01-01 01:00:00 +01:00'",
            "TIMESTAMP '2001-01-01 12:00:00.5 +00:00'"
        },
        new[] { "1901-01-01 00:00:00 UTC", "2001-01-01 00:00:00 UTC", "2001-01-01 12:00:00.5 UTC" })]
    public void Test_Pivot_DateColumnNames(string sqlType, string[] values, string[] expectedNames) =>
        Test_Pivot_DateColumnNames(DbType, sqlType, values, expectedNames);
}
