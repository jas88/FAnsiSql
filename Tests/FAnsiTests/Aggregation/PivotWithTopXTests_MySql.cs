using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal sealed class PivotWithTopXTests_MySql : PivotWithTopXTestsBase
{
    private const DatabaseType DbType = DatabaseType.MySql;

    [Test]
    public void Test_PivotWithTop2_OrderByCountDesc() => Test_PivotWithTop2_OrderByCountDesc(DbType);

    [Test]
    public void Test_PivotWithTop2_WithHaving() => Test_PivotWithTop2_WithHaving(DbType);

    [Test]
    public void Test_PivotWithTop2_WithWhere() => Test_PivotWithTop2_WithWhere(DbType);

    [Test]
    public void Test_PivotWithTop2_OrderByAlphabetical() => Test_PivotWithTop2_OrderByAlphabetical(DbType);

    [Test]
    public void Test_PivotWithTop2_WhereAndHaving() => Test_PivotWithTop2_WhereAndHaving(DbType);

    [Test]
    public void Test_PivotWithTop1_SingleColumn() => Test_PivotWithTop1_SingleColumn(DbType);

}
