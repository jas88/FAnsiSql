using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Table;

internal sealed class ColumnHelperTests_Sqlite : ColumnHelperTestsBase
{
    private const DatabaseType DbType = DatabaseType.Sqlite;

    #region GetAlterColumnToSql Tests

    // Note: Most GetAlterColumnToSql tests omitted for SQLite as it does not support ALTER COLUMN operations

    [Test]
    public void GetAlterColumnToSql_Sqlite_ThrowsNotSupportedException() =>
        GetAlterColumnToSql_Sqlite_ThrowsNotSupportedException(DbType);

    #endregion

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

    #region Edge Cases and SQL Syntax Verification

    [Test]
    public void GetTopXSqlForColumn_ContainsDatabaseSpecificSyntax() =>
        GetTopXSqlForColumn_ContainsDatabaseSpecificSyntax(DbType);

    [Test]
    public void GetTopXSqlForColumn_UsesInvariantCulture() => GetTopXSqlForColumn_UsesInvariantCulture(DbType);

    #endregion
}
