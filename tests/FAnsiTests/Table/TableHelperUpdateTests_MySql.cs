using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Table;

internal sealed class TableHelperUpdateTests_MySql : TableHelperUpdateTestsBase
{
    private const DatabaseType DbType = DatabaseType.MySql;

    [Test]
    public void UpdateWithJoin_SingleColumn_UpdatesCorrectly() => UpdateWithJoin_SingleColumn_UpdatesCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_MultipleColumns_UpdatesCorrectly() =>
        UpdateWithJoin_MultipleColumns_UpdatesCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_WithWhereClause_FiltersCorrectly() =>
        UpdateWithJoin_WithWhereClause_FiltersCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_MultipleWhereConditions_AppliesAllConditions() =>
        UpdateWithJoin_MultipleWhereConditions_AppliesAllConditions(DbType);

    [Test]
    public void UpdateWithJoin_SetToNull_UpdatesCorrectly() => UpdateWithJoin_SetToNull_UpdatesCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_WhereIsNull_FiltersCorrectly() => UpdateWithJoin_WhereIsNull_FiltersCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_SelfJoin_UpdatesCorrectly() => UpdateWithJoin_SelfJoin_UpdatesCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_MultipleJoinConditions_JoinsCorrectly() =>
        UpdateWithJoin_MultipleJoinConditions_JoinsCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_DateTimeValues_UpdatesCorrectly() =>
        UpdateWithJoin_DateTimeValues_UpdatesCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_DecimalValues_UpdatesCorrectly() =>
        UpdateWithJoin_DecimalValues_UpdatesCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_SpecialCharactersInData_UpdatesCorrectly() =>
        UpdateWithJoin_SpecialCharactersInData_UpdatesCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_NoMatchingRows_UpdatesNothing() => UpdateWithJoin_NoMatchingRows_UpdatesNothing(DbType);

    [Test]
    public void UpdateWithJoin_EmptyUpdateTable_UpdatesNothing() =>
        UpdateWithJoin_EmptyUpdateTable_UpdatesNothing(DbType);

    [Test]
    public void UpdateWithJoin_LargeDataSet_UpdatesCorrectly() => UpdateWithJoin_LargeDataSet_UpdatesCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_InvalidLocationToInsert_ThrowsException() =>
        UpdateWithJoin_InvalidLocationToInsert_ThrowsException(DbType);

    [Test]
    public void UpdateWithJoin_CalculatedExpression_UpdatesCorrectly() =>
        UpdateWithJoin_CalculatedExpression_UpdatesCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_CaseWhenExpression_UpdatesCorrectly() =>
        UpdateWithJoin_CaseWhenExpression_UpdatesCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_StringConcatenation_UpdatesCorrectly() =>
        UpdateWithJoin_StringConcatenation_UpdatesCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_OrConditionInWhere_UpdatesCorrectly() =>
        UpdateWithJoin_OrConditionInWhere_UpdatesCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_LessThanComparison_UpdatesCorrectly() =>
        UpdateWithJoin_LessThanComparison_UpdatesCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_BooleanValues_UpdatesCorrectly() =>
        UpdateWithJoin_BooleanValues_UpdatesCorrectly(DbType);

    [Test]
    public void UpdateWithJoin_InOperator_UpdatesCorrectly() => UpdateWithJoin_InOperator_UpdatesCorrectly(DbType);

    [Test]
    public void Test_UpdateTableFromJoin_OriginalTest() => Test_UpdateTableFromJoin_OriginalTest(DbType);
}
