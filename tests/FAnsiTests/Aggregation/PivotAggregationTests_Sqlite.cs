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
}
