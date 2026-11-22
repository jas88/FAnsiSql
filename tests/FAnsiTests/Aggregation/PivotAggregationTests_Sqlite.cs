using FAnsi;
using FAnsiTests.TestGeneration;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class PivotAggregationTests_Sqlite : PivotAggregationTestsBase
{
    private const DatabaseType DbType = DatabaseType.Sqlite;

    [TestCase(DbType)]
    [SkipDatabase(DbType, SqliteCannotPivot)]
    public new void Test_PivotOnlyCount(DatabaseType type) => base.Test_PivotOnlyCount(type);

    [TestCase(DbType)]
    [SkipDatabase(DbType, SqliteCannotPivot)]
    public new void Test_Pivot_SameConnectionAfterFailedPivot(DatabaseType type) =>
        base.Test_Pivot_SameConnectionAfterFailedPivot(type);

    [TestCase(DbType, false)]
    [TestCase(DbType, true)]
    [SkipDatabase(DbType, SqliteCannotPivot)]
    public new void Test_Pivot_OnlyNullOrEmptyValues(DatabaseType type, bool withAxis) =>
        base.Test_Pivot_OnlyNullOrEmptyValues(type, withAxis);
}
