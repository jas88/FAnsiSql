using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class PivotAggregationTests_PostgreSql : PivotAggregationTestsBase
{
    private const DatabaseType DbType = DatabaseType.PostgreSql;

    [Test]
    public void Test_PivotOnlyCount() => Test_PivotOnlyCount(DbType);

    [Test]
    public void Test_Pivot_SameConnectionAfterFailedPivot() => Test_Pivot_SameConnectionAfterFailedPivot(DbType);

    [Test]
    public void Test_Pivot_QueriesThatDifferBeforeSelectHaveCursorsOfTheirOwn() =>
        Test_Pivot_QueriesThatDifferBeforeSelectHaveCursorsOfTheirOwn(DbType);

    [TestCase(false)]
    [TestCase(true)]
    public void Test_Pivot_OnlyNullOrEmptyValues(bool withAxis) => Test_Pivot_OnlyNullOrEmptyValues(DbType, withAxis);

    /// <summary>
    ///     A name can hold a double quote, but is cut to 63 bytes (max_identifier_length): a value cut to the name of an
    ///     earlier column gets its position as a suffix
    /// </summary>
    [Test]
    public void Test_Pivot_ColumnNamesAreValidAndUnique() =>
        Test_Pivot_ColumnNamesAreValidAndUnique(DbType,
            ["T", "\"", "\"\"", "a\"b", "ab", "a`b]c'd", new string('x', 63), $"{new string('x', 61)}_8"]);

    [TestCase("timestamp",
        new[]
        {
            "TIMESTAMP '1901-01-01 00:00:00'", "TIMESTAMP '2001-01-01 00:00:00'", "TIMESTAMP '2001-01-01 12:00:00'",
            "TIMESTAMP '2001-01-01 12:00:00.5'"
        },
        new[] { "1901-01-01 00:00:00", "2001-01-01 00:00:00", "2001-01-01 12:00:00", "2001-01-01 12:00:00.5" })]
    [TestCase("date", new[] { "DATE '1901-01-01'", "DATE '2001-01-01'" }, new[] { "1901-01-01", "2001-01-01" })]
    public void Test_Pivot_DateColumnNames(string sqlType, string[] values, string[] expectedNames) =>
        Test_Pivot_DateColumnNames(DbType, sqlType, values, expectedNames);
}
