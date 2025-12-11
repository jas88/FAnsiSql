using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class CalendarAggregationTests_PostgreSql : CalendarAggregationTestsBase
{
    private const DatabaseType DbType = DatabaseType.PostgreSql;

    [Test]
    public void Test_Calendar_Year() => Test_Calendar_Year(DbType);

    [Test]
    public void Test_Calendar_Quarter() => Test_Calendar_Quarter(DbType);

    [Test]
    public void Test_Calendar_Month() => Test_Calendar_Month(DbType);

    [Test]
    public void Test_Calendar_Day() => Test_Calendar_Day(DbType);

    [Test]
    public void Test_Calendar_ToToday() => Test_Calendar_ToToday(DbType);

    [Test]
    public void Test_Calendar_SELECTColumnOrder_CountAfterAxisColumn() =>
        Test_Calendar_SELECTColumnOrder_CountAfterAxisColumn(DbType);
}
