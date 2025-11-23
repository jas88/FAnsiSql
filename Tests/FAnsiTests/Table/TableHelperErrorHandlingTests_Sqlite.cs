using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Table;

internal sealed class TableHelperErrorHandlingTests_Sqlite : TableHelperErrorHandlingTestsBase
{
    private const DatabaseType DbType = DatabaseType.Sqlite;

    [Test]
    public void Drop_NonExistentTable_ThrowsException() => Drop_NonExistentTable_ThrowsException(DbType);

    [Test]
    public void AddColumn_DuplicateColumnName_ThrowsException() => AddColumn_DuplicateColumnName_ThrowsException(DbType);

    [Test]
    public void DropColumn_NonExistentColumn_ThrowsException() => DropColumn_NonExistentColumn_ThrowsException(DbType);

    [Test]
    public void Truncate_NonExistentTable_ThrowsException() => Truncate_NonExistentTable_ThrowsException(DbType);

    [Test]
    public void CreateIndex_DuplicateIndexName_ThrowsException() => CreateIndex_DuplicateIndexName_ThrowsException(DbType);

    [Test]
    public void DropIndex_NonExistentIndex_ThrowsException() => DropIndex_NonExistentIndex_ThrowsException(DbType);

    [Test]
    public void Rename_ToExistingTableName_ThrowsException() => Rename_ToExistingTableName_ThrowsException(DbType);

    [Test]
    public void AddForeignKey_NoPrimaryKey_ThrowsException() => AddForeignKey_NoPrimaryKey_ThrowsException(DbType);

    [Test]
    public void AddForeignKey_TypeMismatch_ThrowsException() => AddForeignKey_TypeMismatch_ThrowsException(DbType);

    [Test]
    public void AddColumn_EmptyColumnName_ThrowsException() => AddColumn_EmptyColumnName_ThrowsException(DbType);

    [Test]
    public void Insert_WithAutoIncrement_Concurrent_NoCollisions() => Insert_WithAutoIncrement_Concurrent_NoCollisions(DbType);

    [Test]
    public void Insert_NullIntoNotNullColumn_ThrowsException() => Insert_NullIntoNotNullColumn_ThrowsException(DbType);

    [Test]
    public void Insert_NullIntoNullableColumn_Success() => Insert_NullIntoNullableColumn_Success(DbType);

    [Test]
    public void Insert_RollbackTransaction_NoDataPersisted() => Insert_RollbackTransaction_NoDataPersisted(DbType);

    [Test]
    public void Insert_CommitTransaction_DataPersisted() => Insert_CommitTransaction_DataPersisted(DbType);

    [Test]
    public void GetRowCount_LargeTable_ReturnsCorrectCount() => GetRowCount_LargeTable_ReturnsCorrectCount(DbType);

    [Test]
    public void Insert_SpecialCharactersInString_Success() => Insert_SpecialCharactersInString_Success(DbType);

}
