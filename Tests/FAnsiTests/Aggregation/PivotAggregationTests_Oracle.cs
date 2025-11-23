using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class PivotAggregationTests_Oracle : PivotAggregationTestsBase
{
    private const DatabaseType DbType = DatabaseType.Oracle;

    [Test]
    [Ignore("Pivot operations not supported on Oracle")]
    public void Test_PivotOnlyCount() => Test_PivotOnlyCount(DbType);
}
