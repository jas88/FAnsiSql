using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class PivotAggregationTests_MicrosoftSql : PivotAggregationTestsBase
{
    private const DatabaseType DbType = DatabaseType.MicrosoftSQLServer;

    [Test]
    public void Test_PivotOnlyCount() => Test_PivotOnlyCount(DbType);
}
