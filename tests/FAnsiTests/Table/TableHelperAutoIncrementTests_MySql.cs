using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Table;

internal sealed class TableHelperAutoIncrementTests_MySql : TableHelperAutoIncrementTestsBase
{
    private const DatabaseType DbType = DatabaseType.MySql;

    [Test]
    public void ExecuteInsertReturningIdentity_SingleInsert_ReturnsIdentity() =>
        ExecuteInsertReturningIdentity_SingleInsert_ReturnsIdentity(DbType);

    [Test]
    public void ExecuteInsertReturningIdentity_MultipleInserts_ReturnsIncrementingValues() =>
        ExecuteInsertReturningIdentity_MultipleInserts_ReturnsIncrementingValues(DbType);

    [Test]
    public void ExecuteInsertReturningIdentity_WithTransaction_ReturnsIdentity() =>
        ExecuteInsertReturningIdentity_WithTransaction_ReturnsIdentity(DbType);

    [Test]
    public void DiscoverColumns_IdentifiesAutoIncrement_Correctly() =>
        DiscoverColumns_IdentifiesAutoIncrement_Correctly(DbType);

    [Test]
    public void Insert_AutoIncrementTable_GeneratesIdentity() => Insert_AutoIncrementTable_GeneratesIdentity(DbType);

    [Test]
    public void Insert_MultipleRows_GeneratesSequentialIdentities() =>
        Insert_MultipleRows_GeneratesSequentialIdentities(DbType);

    [Test]
    public void BulkInsert_AutoIncrementTable_GeneratesIdentities() =>
        BulkInsert_AutoIncrementTable_GeneratesIdentities(DbType);

    [Test]
    public void AutoIncrement_Column_IsPrimaryKey() => AutoIncrement_Column_IsPrimaryKey(DbType);

    [Test]
    public void ScriptTableCreation_WithAutoIncrement_IncludesIdentity() =>
        ScriptTableCreation_WithAutoIncrement_IncludesIdentity(DbType);

    [Test]
    public void ScriptTableCreation_ConvertIdentityToInt_RemovesAutoIncrement() =>
        ScriptTableCreation_ConvertIdentityToInt_RemovesAutoIncrement(DbType);
}
