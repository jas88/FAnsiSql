using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Table;

internal sealed class TableHelperMetadataTests_Oracle : TableHelperMetadataTestsBase
{
    private const DatabaseType DbType = DatabaseType.Oracle;

    [Test]
    public void HasPrimaryKey_NoPrimaryKey_ReturnsFalse() => HasPrimaryKey_NoPrimaryKey_ReturnsFalse(DbType);

    [Test]
    public void HasPrimaryKey_WithPrimaryKey_ReturnsTrue() => HasPrimaryKey_WithPrimaryKey_ReturnsTrue(DbType);

    [Test]
    public void HasPrimaryKey_CompositePrimaryKey_ReturnsTrue() => HasPrimaryKey_CompositePrimaryKey_ReturnsTrue(DbType);

    [Test]
    public void HasPrimaryKey_AfterAddingPrimaryKey_ReturnsTrue() => HasPrimaryKey_AfterAddingPrimaryKey_ReturnsTrue(DbType);

    [Test]
    public void DiscoverColumns_EmptyTable_ReturnsColumns() => DiscoverColumns_EmptyTable_ReturnsColumns(DbType);

    [Test]
    public void DiscoverColumns_IdentifiesPrimaryKey() => DiscoverColumns_IdentifiesPrimaryKey(DbType);

    [Test]
    public void DiscoverColumns_IdentifiesAutoIncrement() => DiscoverColumns_IdentifiesAutoIncrement(DbType);

    [Test]
    public void DiscoverColumns_IdentifiesNullability() => DiscoverColumns_IdentifiesNullability(DbType);

    [Test]
    public void DiscoverColumns_CorrectDataTypes() => DiscoverColumns_CorrectDataTypes(DbType);

    [Test]
    public void DiscoverRelationships_NoRelationships_ReturnsEmpty() => DiscoverRelationships_NoRelationships_ReturnsEmpty(DbType);

    [Test]
    public void DiscoverRelationships_WithForeignKey_ReturnsRelationship() => DiscoverRelationships_WithForeignKey_ReturnsRelationship(DbType);

    [Test]
    public void ScriptTableCreation_ReturnsValidSql() => ScriptTableCreation_ReturnsValidSql(DbType);

    [Test]
    public void ScriptTableCreation_DropPrimaryKeys_OmitsPrimaryKey() => ScriptTableCreation_DropPrimaryKeys_OmitsPrimaryKey(DbType);

    [Test]
    public void MakeDistinct_TableWithDuplicates_RemovesDuplicates() => MakeDistinct_TableWithDuplicates_RemovesDuplicates(DbType);

    [Test]
    public void MakeDistinct_TableWithPrimaryKey_NoChange() => MakeDistinct_TableWithPrimaryKey_NoChange(DbType);

}
