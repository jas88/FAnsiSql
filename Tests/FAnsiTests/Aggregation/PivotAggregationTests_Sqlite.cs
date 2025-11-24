using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class PivotAggregationTests_Sqlite : PivotAggregationTestsBase
{
    private const DatabaseType DbType = DatabaseType.Sqlite;

    [Test]
    [Ignore("Pivot operations not supported on SQLite")]
    public void Test_PivotOnlyCount() => Test_PivotOnlyCount(DbType);
}
