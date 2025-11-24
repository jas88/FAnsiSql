using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class PivotAggregationTests_MySql : PivotAggregationTestsBase
{
    private const DatabaseType DbType = DatabaseType.MySql;

    [Test]
    public void Test_PivotOnlyCount() => Test_PivotOnlyCount(DbType);
}
