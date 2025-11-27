using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Table;

internal sealed class BulkCopyTests_Sqlite : BulkCopyTestsBase
{
    private const DatabaseType DbType = DatabaseType.Sqlite;

    #region Basic Upload Operations

    [Test]
    public void Upload_EmptyDataTable_ReturnsZero() => Upload_EmptyDataTable_ReturnsZero(DbType);

    [Test]
    public void Upload_SingleRow_Success() => Upload_SingleRow_Success(DbType);

    [Test]
    public void Upload_MultipleRows_AllInserted() => Upload_MultipleRows_AllInserted(DbType);

    [Test]
    public void Upload_MultipleBatches_Success() => Upload_MultipleBatches_Success(DbType);

    #endregion

    #region NULL Value Handling

    [Test]
    public void Upload_NullValues_InsertedCorrectly() => Upload_NullValues_InsertedCorrectly(DbType);

    [Test]
    public void Upload_EmptyStrings_ConvertedToNull() => Upload_EmptyStrings_ConvertedToNull(DbType);

    #endregion

    #region Error Handling - Data Type Violations

    [Test]
    public void Upload_StringTooLong_ThrowsException() => Upload_StringTooLong_ThrowsException(DbType);

    [Test]
    public void Upload_DecimalOutOfRange_ThrowsException() => Upload_DecimalOutOfRange_ThrowsException(DbType);

    [Test]
    public void Upload_InvalidDecimalFormat_ThrowsFormatException() => Upload_InvalidDecimalFormat_ThrowsFormatException(DbType);

    [Test]
    public void Upload_IntegerOverflow_ThrowsException() => Upload_IntegerOverflow_ThrowsException(DbType);

    #endregion

    #region Error Handling - Constraint Violations

    [Test]
    public void Upload_ViolateNotNullConstraint_ThrowsException() => Upload_ViolateNotNullConstraint_ThrowsException(DbType);

    [Test]
    public void Upload_DuplicatePrimaryKey_ThrowsException() => Upload_DuplicatePrimaryKey_ThrowsException(DbType);

    #endregion

    #region Column Mapping

    // Skipped: Upload_ReorderedColumns_MapsCorrectly - SQLite DateTime limitation

    [Test]
    public void Upload_SubsetOfColumns_UsesDefaults() => Upload_SubsetOfColumns_UsesDefaults(DbType);

    [Test]
    public void Upload_ExtraColumnsInDataTable_ThrowsException() => Upload_ExtraColumnsInDataTable_ThrowsException(DbType);

    [Test]
    public void Upload_CaseMismatchedColumns_MapsCorrectly() => Upload_CaseMismatchedColumns_MapsCorrectly(DbType);

    #endregion

    #region Transaction Behavior

    [Test]
    public void Upload_WithTransaction_CommitsProperly() => Upload_WithTransaction_CommitsProperly(DbType);

    [Test]
    public void Upload_WithTransaction_RollbackWorks() => Upload_WithTransaction_RollbackWorks(DbType);

    [Test]
    public void Upload_TransactionError_RollsBackAutomatically() => Upload_TransactionError_RollsBackAutomatically(DbType);

    #endregion

    #region Special Data Types

    [Test]
    public void Upload_UnicodeStrings_PreservedCorrectly() => Upload_UnicodeStrings_PreservedCorrectly(DbType);

    // Skipped: Upload_DateTimeValues_PreservedCorrectly - SQLite DateTime limitation
    // Skipped: Upload_DateTimeStrings_ParsedCorrectly - SQLite DateTime limitation

    [Test]
    public void Upload_BinaryData_PreservedCorrectly() => Upload_BinaryData_PreservedCorrectly(DbType);

    [Test]
    public void Upload_BooleanValues_ConvertedCorrectly() => Upload_BooleanValues_ConvertedCorrectly(DbType);

    [Test]
    public void Upload_DecimalPrecision_PreservedCorrectly() => Upload_DecimalPrecision_PreservedCorrectly(DbType);

    #endregion

    #region Performance and Large Datasets

    [Test]
    public void Upload_LargeDataset_CompletesSuccessfully() => Upload_LargeDataset_CompletesSuccessfully(DbType);

    [Test]
    public void Upload_WideTable_AllColumnsInserted() => Upload_WideTable_AllColumnsInserted(DbType);

    #endregion

    #region Timeout and Cancellation

    [Test]
    public void Upload_WithTimeout_RespectsSetting() => Upload_WithTimeout_RespectsSetting(DbType);

    #endregion

    #region Auto-Increment and Identity Columns

    [Test]
    public void Upload_WithAutoIncrementColumn_GeneratesValues() => Upload_WithAutoIncrementColumn_GeneratesValues(DbType);

    #endregion

    #region Decimal Precision and Scale Validation

    [Test]
    public void Upload_DecimalExceedsPrecision_ThrowsException() => Upload_DecimalExceedsPrecision_ThrowsException(DbType);

    [Test]
    public void Upload_DecimalExceedsScale_ThrowsException() => Upload_DecimalExceedsScale_ThrowsException(DbType);

    [Test]
    public void Upload_DecimalMaxPrecisionAndScale_Success() => Upload_DecimalMaxPrecisionAndScale_Success(DbType);

    [Test]
    public void Upload_DecimalLargePrecisionViolation_ThrowsException() => Upload_DecimalLargePrecisionViolation_ThrowsException(DbType);

    [Test]
    public void Upload_DecimalZeroScale_Success() => Upload_DecimalZeroScale_Success(DbType);

    [Test]
    public void Upload_DecimalZeroScale_WithDecimalPlaces_ThrowsException() => Upload_DecimalZeroScale_WithDecimalPlaces_ThrowsException(DbType);

    [Test]
    public void Upload_DecimalNullValues_IgnoredInValidation() => Upload_DecimalNullValues_IgnoredInValidation(DbType);

    #endregion

    #region Disposal and Resource Management

    [Test]
    public void Upload_AfterDispose_ThrowsException() => Upload_AfterDispose_ThrowsException(DbType);

    #endregion
}
