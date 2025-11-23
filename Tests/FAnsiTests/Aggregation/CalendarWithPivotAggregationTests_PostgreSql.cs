using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class CalendarWithPivotAggregationTests_PostgreSql : CalendarWithPivotAggregationTestsBase
{
    private const DatabaseType DbType = DatabaseType.PostgreSql;

    [Test]
    [Ignore("Pivot with calendar not supported on this database")]
    public void Test_Calendar_WithPivot_Easy() => Test_Calendar_WithPivot(DbType, true);

    [Test]
    [Ignore("Pivot with calendar not supported on this database")]
    public void Test_Calendar_WithPivot_Hard() => Test_Calendar_WithPivot(DbType, false);
}
