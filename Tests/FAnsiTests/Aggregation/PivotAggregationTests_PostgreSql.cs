using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class PivotAggregationTests_PostgreSql : PivotAggregationTestsBase
{
    private const DatabaseType DbType = DatabaseType.PostgreSql;

    [Test]
    [Ignore("Pivot operations not supported on PostgreSQL")]
    public void Test_PivotOnlyCount() => Test_PivotOnlyCount(DbType);
}
