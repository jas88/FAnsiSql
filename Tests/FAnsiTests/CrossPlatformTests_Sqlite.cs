using FAnsi;
using NUnit.Framework;

namespace FAnsiTests;

#if SQLITE_TESTS
internal sealed class CrossPlatformTests_Sqlite : CrossPlatformTestsBase
{
    protected override DatabaseType DatabaseType => DatabaseType.Sqlite;

    [Test]
    public void TestTableCreation_NullTableName() => TestTableCreation_NullTableName();

    [Test]
    public void ForeignKeyCreationTest() => ForeignKeyCreationTest();

    [TestCase(false)]
    [TestCase(true)]
    public void ForeignKeyCreationTest_TwoColumns(bool cascadeDelete) =>
        ForeignKeyCreationTest_TwoColumns(cascadeDelete);

    [Test]
    public void CreateMaxVarcharColumns() => CreateMaxVarcharColumns();

    [Test]
    public void CreateMaxVarcharColumnFromDataTable() => CreateMaxVarcharColumnFromDataTable();

    [Test]
    public void CreateDateColumnFromDataTable() => CreateDateColumnFromDataTable();

    [Test]
    public void CreateTable_EmptyDataTable_ExplicitTypes() => CreateTable_EmptyDataTable_ExplicitTypes();

    [TestCase(false)]
    [TestCase(true)]
    public void AddColumnTest(bool useTransaction) => AddColumnTest(useTransaction);

    [Test]
    public void ChangeDatabaseShouldNotAffectOriginalConnectionString_Test() =>
        ChangeDatabaseShouldNotAffectOriginalConnectionString_Test();

    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public void TestDistincting(bool useTransaction, bool dodgyNames) =>
        TestDistincting(useTransaction, dodgyNames);

    [Test]
    public void TestIntDataTypes() => TestIntDataTypes();

    [Test]
    public void TestFloatDataTypes() => TestFloatDataTypes();

    [Test]
    public void CreateTable_AutoIncrementColumnTest() => CreateTable_AutoIncrementColumnTest();

    [Test]
    public void CreateTable_DefaultTest_Date() => CreateTable_DefaultTest_Date();

    [Test]
    public void CreateTable_DefaultTest_Guid() => CreateTable_DefaultTest_Guid();

    [Test]
    public void Test_BulkInserting_LotsOfDates() => Test_BulkInserting_LotsOfDates();
}
#endif
