using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class CalendarWithPivotAggregationTests_Sqlite : CalendarWithPivotAggregationTestsBase
{
    private const DatabaseType DbType = DatabaseType.Sqlite;

    [Test]
    [Ignore("Pivot with calendar not supported on this database")]
    public void Test_Calendar_WithPivot_Easy() => Test_Calendar_WithPivot(DbType, true);

    [Test]
    [Ignore("Pivot with calendar not supported on this database")]
    public void Test_Calendar_WithPivot_Hard() => Test_Calendar_WithPivot(DbType, false);
}
