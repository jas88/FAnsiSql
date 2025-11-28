using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class AggregateHelperCoreTests_Oracle : AggregateHelperCoreTestsBase
{
    private const DatabaseType DbType = DatabaseType.Oracle;

    [Test]
    public void Test_GetDatePartOfColumn_Day() => Test_GetDatePartOfColumn_Day(DbType);

    [Test]
    public void Test_GetDatePartOfColumn_Month() => Test_GetDatePartOfColumn_Month(DbType);

    [Test]
    public void Test_GetDatePartOfColumn_Year() => Test_GetDatePartOfColumn_Year(DbType);

    [Test]
    public void Test_GetDatePartOfColumn_Quarter() => Test_GetDatePartOfColumn_Quarter(DbType);

    [Test]
    public void Test_SumAggregate() => Test_SumAggregate(DbType);

    [Test]
    public void Test_AvgAggregate() => Test_AvgAggregate(DbType);

    [Test]
    public void Test_MinAggregate() => Test_MinAggregate(DbType);

    [Test]
    public void Test_MaxAggregate() => Test_MaxAggregate(DbType);

    [Test]
    public void Test_GroupByWithSum() => Test_GroupByWithSum(DbType);

    [Test]
    public void Test_GroupByWithAvg() => Test_GroupByWithAvg(DbType);

    [Test]
    public void Test_CountDistinct() => Test_CountDistinct(DbType);

    [Test]
    public void Test_HavingClause_GreaterThan() => Test_HavingClause_GreaterThan(DbType);

    [Test]
    public void Test_HavingClause_Count() => Test_HavingClause_Count(DbType);

    [Test]
    public void Test_CustomAggregateAlias() => Test_CustomAggregateAlias(DbType);

    [Test]
    public void Test_MultipleGroupByColumns() => Test_MultipleGroupByColumns(DbType);

    [Test]
    public void Test_WhereClauseWithAggregate() => Test_WhereClauseWithAggregate(DbType);

    [Test]
    public void Test_WhereAndGroupBy() => Test_WhereAndGroupBy(DbType);

}
