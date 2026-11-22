using FAnsi;
using FAnsiTests.TestGeneration;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class CalendarWithPivotAggregationTests_Sqlite : CalendarWithPivotAggregationTestsBase
{
    private const DatabaseType DbType = DatabaseType.Sqlite;

    [TestCase(DbType)]
    [SkipDatabase(DbType, SqliteCannotPivot)]
    public void Test_Calendar_WithPivot_Easy(DatabaseType type) => Test_Calendar_WithPivot(type, true);

    [TestCase(DbType)]
    [SkipDatabase(DbType, SqliteCannotPivot)]
    public void Test_Calendar_WithPivot_Hard(DatabaseType type) => Test_Calendar_WithPivot(type, false);
}
