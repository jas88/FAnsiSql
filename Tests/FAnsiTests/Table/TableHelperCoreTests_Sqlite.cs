using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Table;

internal sealed class TableHelperCoreTests_Sqlite : TableHelperCoreTestsBase
{
    private const DatabaseType DbType = DatabaseType.Sqlite;

    [Test]
    public void Exists_TableExists_ReturnsTrue() => Exists_TableExists_ReturnsTrue(DbType);

    [Test]
    public void Exists_TableDoesNotExist_ReturnsFalse() => Exists_TableDoesNotExist_ReturnsFalse(DbType);

    [Test]
    public void Exists_AfterDrop_ReturnsFalse() => Exists_AfterDrop_ReturnsFalse(DbType);

    [Test]
    public void Exists_CaseInsensitive_ReturnsTrue() => Exists_CaseInsensitive_ReturnsTrue(DbType);

    [Test]
    public void Exists_WithSchema_ReturnsTrue() => Exists_WithSchema_ReturnsTrue(DbType);

    [Test]
    public void Drop_TableExists_SuccessfullyDrops() => Drop_TableExists_SuccessfullyDrops(DbType);

    [Test]
    public void Drop_TableWithData_SuccessfullyDrops() => Drop_TableWithData_SuccessfullyDrops(DbType);

    [Test]
    public void Drop_View_SuccessfullyDrops() => Drop_View_SuccessfullyDrops(DbType);

    [Test]
    public void Truncate_TableWithData_RemovesAllRows() => Truncate_TableWithData_RemovesAllRows(DbType);

    [Test]
    public void Truncate_EmptyTable_NoError() => Truncate_EmptyTable_NoError(DbType);

    [Test]
    public void Truncate_PreservesTableStructure() => Truncate_PreservesTableStructure(DbType);

    [Test]
    public void GetRowCount_EmptyTable_ReturnsZero() => GetRowCount_EmptyTable_ReturnsZero(DbType);

    [Test]
    public void GetRowCount_TableWithRows_ReturnsCorrectCount() => GetRowCount_TableWithRows_ReturnsCorrectCount(DbType);

    [Test]
    public void GetRowCount_AfterInsert_ReturnsUpdatedCount() => GetRowCount_AfterInsert_ReturnsUpdatedCount(DbType);

    [Test]
    public void IsEmpty_EmptyTable_ReturnsTrue() => IsEmpty_EmptyTable_ReturnsTrue(DbType);

    [Test]
    public void IsEmpty_TableWithData_ReturnsFalse() => IsEmpty_TableWithData_ReturnsFalse(DbType);

    [Test]
    public void IsEmpty_AfterTruncate_ReturnsTrue() => IsEmpty_AfterTruncate_ReturnsTrue(DbType);

    [Test]
    public void GetTopXSql_ReturnsValidSql() => GetTopXSql_ReturnsValidSql(DbType);

    [Test]
    public void FillDataTableWithTopX_ReturnsCorrectRowCount() => FillDataTableWithTopX_ReturnsCorrectRowCount(DbType);

    [Test]
    public void FillDataTableWithTopX_PreservesColumnTypes() => FillDataTableWithTopX_PreservesColumnTypes(DbType);

}
