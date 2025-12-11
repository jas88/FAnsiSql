using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Table;

internal sealed class ColumnHelperTests_MicrosoftSql : ColumnHelperTestsBase
{
    private const DatabaseType DbType = DatabaseType.MicrosoftSQLServer;

    #region GetTopXSqlForColumn Tests

    [Test]
    public void GetTopXSqlForColumn_WithoutDiscardNulls_ReturnsAllRows() =>
        GetTopXSqlForColumn_WithoutDiscardNulls_ReturnsAllRows(DbType);

    [Test]
    public void GetTopXSqlForColumn_WithDiscardNulls_ExcludesNulls() =>
        GetTopXSqlForColumn_WithDiscardNulls_ExcludesNulls(DbType);

    [Test]
    public void GetTopXSqlForColumn_IntegerColumn_Success() => GetTopXSqlForColumn_IntegerColumn_Success(DbType);

    [Test]
    public void GetTopXSqlForColumn_DateTimeColumn_Success() => GetTopXSqlForColumn_DateTimeColumn_Success(DbType);

    [Test]
    public void GetTopXSqlForColumn_SingleRow_Success() => GetTopXSqlForColumn_SingleRow_Success(DbType);

    [Test]
    public void GetTopXSqlForColumn_AllNulls_WithDiscardNulls_ReturnsEmpty() =>
        GetTopXSqlForColumn_AllNulls_WithDiscardNulls_ReturnsEmpty(DbType);

    [Test]
    public void GetTopXSqlForColumn_LargeTopX_ReturnsAllAvailable() =>
        GetTopXSqlForColumn_LargeTopX_ReturnsAllAvailable(DbType);

    [Test]
    public void GetTopXSqlForColumn_SpecialCharactersInColumnName_Success() =>
        GetTopXSqlForColumn_SpecialCharactersInColumnName_Success(DbType);

    #endregion

    #region GetAlterColumnToSql Tests

    [Test]
    public void GetAlterColumnToSql_IncreaseStringLength_Success() =>
        GetAlterColumnToSql_IncreaseStringLength_Success(DbType);

    [Test]
    public void GetAlterColumnToSql_ChangeNullability_AllowNullsToNotNull() =>
        GetAlterColumnToSql_ChangeNullability_AllowNullsToNotNull(DbType);

    [Test]
    public void GetAlterColumnToSql_ChangeNullability_NotNullToAllowNulls() =>
        GetAlterColumnToSql_ChangeNullability_NotNullToAllowNulls(DbType);

    [Test]
    public void GetAlterColumnToSql_IntToVarchar_Success() => GetAlterColumnToSql_IntToVarchar_Success(DbType);

    [Test]
    public void GetAlterColumnToSql_MicrosoftSQL_BitToOtherType_UsesStringIntermediate() =>
        GetAlterColumnToSql_MicrosoftSQL_BitToOtherType_UsesStringIntermediate(DbType);

    [Test]
    public void GetAlterColumnToSql_DecreaseStringLength_Success() =>
        GetAlterColumnToSql_DecreaseStringLength_Success(DbType);

    [Test]
    public void GetAlterColumnToSql_DateTimeColumn_Success() => GetAlterColumnToSql_DateTimeColumn_Success(DbType);

    [Test]
    public void GetAlterColumnToSql_PreservesData_AfterTypeChange() =>
        GetAlterColumnToSql_PreservesData_AfterTypeChange(DbType);

    #endregion

    #region Edge Cases and SQL Syntax Verification

    [Test]
    public void GetTopXSqlForColumn_ContainsDatabaseSpecificSyntax() =>
        GetTopXSqlForColumn_ContainsDatabaseSpecificSyntax(DbType);

    [Test]
    public void GetTopXSqlForColumn_UsesInvariantCulture() => GetTopXSqlForColumn_UsesInvariantCulture(DbType);

    #endregion
}
