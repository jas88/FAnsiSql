using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class PivotWithTopXTests_Sqlite : PivotWithTopXTestsBase
{
    private const DatabaseType DbType = DatabaseType.Sqlite;

    [Test]
    [Ignore("Pivot with TOP X not supported on this database")]
    public void Test_PivotWithTop2_OrderByCountDesc() => Test_PivotWithTop2_OrderByCountDesc(DbType);

    [Test]
    [Ignore("Pivot with TOP X not supported on this database")]
    public void Test_PivotWithTop2_WithHaving() => Test_PivotWithTop2_WithHaving(DbType);

    [Test]
    [Ignore("Pivot with TOP X not supported on this database")]
    public void Test_PivotWithTop2_WithWhere() => Test_PivotWithTop2_WithWhere(DbType);

    [Test]
    [Ignore("Pivot with TOP X not supported on this database")]
    public void Test_PivotWithTop2_OrderByAlphabetical() => Test_PivotWithTop2_OrderByAlphabetical(DbType);

    [Test]
    [Ignore("Pivot with TOP X not supported on this database")]
    public void Test_PivotWithTop2_WhereAndHaving() => Test_PivotWithTop2_WhereAndHaving(DbType);

    [Test]
    [Ignore("Pivot with TOP X not supported on this database")]
    public void Test_PivotWithTop1_SingleColumn() => Test_PivotWithTop1_SingleColumn(DbType);
}
