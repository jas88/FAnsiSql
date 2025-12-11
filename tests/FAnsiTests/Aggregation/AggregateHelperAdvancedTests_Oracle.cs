using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class AggregateHelperAdvancedTests_Oracle : AggregateHelperAdvancedTestsBase
{
    private const DatabaseType DbType = DatabaseType.Oracle;

    [Test]
    public void Test_BuildBasicAggregate_NoAxisNoPivot() => Test_BuildBasicAggregate_NoAxisNoPivot(DbType);

    [Test]
    public void Test_BuildAxisAggregate_DayIncrement() => Test_BuildAxisAggregate_DayIncrement(DbType);

    [Test]
    public void Test_BuildAxisAggregate_MonthIncrement() => Test_BuildAxisAggregate_MonthIncrement(DbType);

    [Test]
    public void Test_AxisAggregate_VerifyWrappedColumn() => Test_AxisAggregate_VerifyWrappedColumn(DbType);

    [Test]
    public void Test_GroupByWithOrderByDesc() => Test_GroupByWithOrderByDesc(DbType);

    [Test]
    public void Test_MultipleAggregatesInSameQuery() => Test_MultipleAggregatesInSameQuery(DbType);

    [Test]
    public void Test_HavingWithMultipleConditions() => Test_HavingWithMultipleConditions(DbType);

    [Test]
    public void Test_EmptyResult_WithAxis() => Test_EmptyResult_WithAxis(DbType);

    [Test]
    public void Test_AggregateWithComplexExpression() => Test_AggregateWithComplexExpression(DbType);

    [Test]
    public void Test_GroupByWithNullHandling() => Test_GroupByWithNullHandling(DbType);

    [Test]
    public void Test_AggregateWithCaseExpression() => Test_AggregateWithCaseExpression(DbType);

    [Test]
    public void Test_GetDatePartOfColumn_AllIncrements_Consistency() =>
        Test_GetDatePartOfColumn_AllIncrements_Consistency(DbType);

    [Test]
    public void Test_AggregateWithJoin() => Test_AggregateWithJoin(DbType);
}
