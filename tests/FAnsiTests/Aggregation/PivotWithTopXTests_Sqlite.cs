using FAnsi;
using FAnsiTests.TestGeneration;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class PivotWithTopXTests_Sqlite : PivotWithTopXTestsBase
{
    private const DatabaseType DbType = DatabaseType.Sqlite;

    [TestCase(DbType)]
    [SkipDatabase(DbType, SqliteCannotPivot)]
    public new void Test_PivotWithTop2_OrderByCountDesc(DatabaseType type) =>
        base.Test_PivotWithTop2_OrderByCountDesc(type);

    [TestCase(DbType)]
    [SkipDatabase(DbType, SqliteCannotPivot)]
    public new void Test_PivotWithTop2_WithHaving(DatabaseType type) => base.Test_PivotWithTop2_WithHaving(type);

    [TestCase(DbType)]
    [SkipDatabase(DbType, SqliteCannotPivot)]
    public new void Test_PivotWithTop2_WithWhere(DatabaseType type) => base.Test_PivotWithTop2_WithWhere(type);

    [TestCase(DbType)]
    [SkipDatabase(DbType, SqliteCannotPivot)]
    public new void Test_PivotWithTop2_OrderByAlphabetical(DatabaseType type) =>
        base.Test_PivotWithTop2_OrderByAlphabetical(type);

    [TestCase(DbType)]
    [SkipDatabase(DbType, SqliteCannotPivot)]
    public new void Test_PivotWithTop2_WhereAndHaving(DatabaseType type) =>
        base.Test_PivotWithTop2_WhereAndHaving(type);

    [TestCase(DbType)]
    [SkipDatabase(DbType, SqliteCannotPivot)]
    public new void Test_PivotWithTop1_SingleColumn(DatabaseType type) => base.Test_PivotWithTop1_SingleColumn(type);
}
