using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class PivotAggregationTests_MySql : PivotAggregationTestsBase
{
    private const DatabaseType DbType = DatabaseType.MySql;

    [Test]
    public void Test_PivotOnlyCount() => Test_PivotOnlyCount(DbType);

    [Test]
    public void Test_Pivot_SameConnectionAfterFailedPivot() => Test_Pivot_SameConnectionAfterFailedPivot(DbType);

    [TestCase(false)]
    [TestCase(true)]
    public void Test_Pivot_OnlyNullOrEmptyValues(bool withAxis) => Test_Pivot_OnlyNullOrEmptyValues(DbType, withAxis);

    /// <summary>
    ///     A backtick in a name is doubled: a value longer than a MySQL column name is cut to 64 characters, and gets its
    ///     position as a suffix if an earlier column has that name
    /// </summary>
    [Test]
    public void Test_Pivot_ColumnNamesAreValidAndUnique() =>
        Test_Pivot_ColumnNamesAreValidAndUnique(DbType,
            ["T", "\"", "\"\"", "a\"b", "ab", "a`b]c'd", new string('x', 64), $"{new string('x', 62)}_8"]);
}
