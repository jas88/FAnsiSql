using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class CalendarWithPivotAggregationTests_MicrosoftSql : CalendarWithPivotAggregationTestsBase
{
    private const DatabaseType DbType = DatabaseType.MicrosoftSQLServer;

    [Test]
    public void Test_Calendar_WithPivot_Easy() => Test_Calendar_WithPivot(DbType, true);

    [Test]
    public void Test_Calendar_WithPivot_Hard() => Test_Calendar_WithPivot(DbType, false);
}
