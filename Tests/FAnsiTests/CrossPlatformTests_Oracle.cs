using FAnsi;
using NUnit.Framework;

namespace FAnsiTests;

#if ORACLE_TESTS
internal sealed class CrossPlatformTests_Oracle : CrossPlatformTestsBase
{
    protected override DatabaseType DatabaseType => DatabaseType.Oracle;

    [Test]
    public void TestTableCreation_NullTableName() => TestTableCreation_NullTableName();

    [TestCase("01/01/2007 00:00:00")]
    [TestCase("2007-01-01 00:00:00")]
    public void DateColumnTests_NoTime(object input) => DateColumnTests_NoTime(input);

    [TestCase("2/28/1993 5:36:27 AM", "en-US")]
    [TestCase("28/2/1993 5:36:27 AM", "en-GB")]
    public void DateColumnTests_UkUsFormat_Explicit(object input, string culture) =>
        DateColumnTests_UkUsFormat_Explicit(input, culture);

    [TestCase("2/28/1993 5:36:27 AM", "en-US")]
    public void DateColumnTests_PrimaryKeyColumn(object input, string culture) =>
        DateColumnTests_PrimaryKeyColumn(input, culture);

    [TestCase("00:00:00")]
    [TestCase("00:00")]
    public void DateColumnTests_TimeOnly_Midnight(object input) => DateColumnTests_TimeOnly_Midnight(input);

    [TestCase("13:11:10")]
    [TestCase("13:11")]
    public void DateColumnTests_TimeOnly_Afternoon(object input) => DateColumnTests_TimeOnly_Afternoon(input);

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

    [TestCase("my (database)", "my (table)", "my (col)")]
    [TestCase("my.database", "my.table", "my.col")]
    public void UnsupportedEntityNames(string horribleDatabaseName, string horribleTableName, string columnName) =>
        UnsupportedEntityNames(horribleDatabaseName, horribleTableName, columnName);

    [TestCase("_-o-_", ":>0<:", "-_")]
    [TestCase("Comment", "Comment", "Comment")]  // reserved keyword in Oracle
    [TestCase("Comment", "SSSS", "Space Out")]
    public void HorribleColumnNames(string horribleDatabaseName, string horribleTableName, string columnName) =>
        HorribleColumnNames(horribleDatabaseName, horribleTableName, columnName);

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
