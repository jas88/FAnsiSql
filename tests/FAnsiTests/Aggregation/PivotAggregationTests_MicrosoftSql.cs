using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class PivotAggregationTests_MicrosoftSql : PivotAggregationTestsBase
{
    private const DatabaseType DbType = DatabaseType.MicrosoftSQLServer;

    [Test]
    public void Test_PivotOnlyCount() => Test_PivotOnlyCount(DbType);

    [Test]
    public void Test_Pivot_SameConnectionAfterFailedPivot() => Test_Pivot_SameConnectionAfterFailedPivot(DbType);

    /// <summary>
    ///     QUOTENAME quotes any name of up to 128 characters: a longer value is cut to 128 characters, and gets its
    ///     position as a suffix if an earlier column has that name
    /// </summary>
    [Test]
    public void Test_Pivot_ColumnNamesAreValidAndUnique() =>
        Test_Pivot_ColumnNamesAreValidAndUnique(DbType,
            ["T", "\"", "\"\"", "a\"b", "ab", "a`b]c'd", new string('x', 128), $"{new string('x', 126)}_8"]);
}
