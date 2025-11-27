using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Table;

internal sealed class TableHelperColumnOperationTests_MicrosoftSql : TableHelperColumnOperationTestsBase
{
    private const DatabaseType DbType = DatabaseType.MicrosoftSQLServer;

    [Test]
    public void AddColumn_IntColumn_Success() => AddColumn_IntColumn_Success(DbType);

    [Test]
    public void AddColumn_StringColumn_Success() => AddColumn_StringColumn_Success(DbType);

    [Test]
    public void AddColumn_NotNullColumn_Success() => AddColumn_NotNullColumn_Success(DbType);

    [Test]
    public void AddColumn_DateTimeColumn_Success() => AddColumn_DateTimeColumn_Success(DbType);

    [Test]
    public void AddColumn_DecimalColumn_Success() => AddColumn_DecimalColumn_Success(DbType);

    [Test]
    public void AddColumn_MultipleColumns_Success() => AddColumn_MultipleColumns_Success(DbType);

    [Test]
    public void DropColumn_ExistingColumn_Success() => DropColumn_ExistingColumn_Success(DbType);

    [Test]
    public void DropColumn_WithData_Success() => DropColumn_WithData_Success(DbType);

    [Test]
    public void DropColumn_NonPrimaryKeyColumn_Success() => DropColumn_NonPrimaryKeyColumn_Success(DbType);

    [Test]
    public void DropColumn_LastNonKeyColumn_Success() => DropColumn_LastNonKeyColumn_Success(DbType);

    [Test]
    public void DiscoverColumn_AfterAddColumn_FindsNewColumn() => DiscoverColumn_AfterAddColumn_FindsNewColumn(DbType);

    [Test]
    public void DiscoverColumn_CaseInsensitive_FindsColumn() => DiscoverColumn_CaseInsensitive_FindsColumn(DbType);

    [Test]
    public void AddAndDropColumn_Sequence_Success() => AddAndDropColumn_Sequence_Success(DbType);

    [Test]
    public void AddColumn_InsertData_DropColumn_DataPreserved() => AddColumn_InsertData_DropColumn_DataPreserved(DbType);

    [Test]
    public void AddColumn_AllBasicTypes_Success() => AddColumn_AllBasicTypes_Success(DbType);

}
