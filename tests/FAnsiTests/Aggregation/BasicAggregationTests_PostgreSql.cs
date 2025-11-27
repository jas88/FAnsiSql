using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class BasicAggregationTests_PostgreSql : BasicAggregationTestsBase
{
    private const DatabaseType DbType = DatabaseType.PostgreSql;

    [Test]
    public void Test_BasicCount() => Test_BasicCount(DbType);

    [Test]
    public void Test_GroupByCount() => Test_GroupByCount(DbType);
}
