using FAnsi;
using NUnit.Framework;

namespace FAnsiTests;

internal sealed class CrossPlatformTests_Sqlite : CrossPlatformTestsBase
{
    /// <summary>
    ///     SQLite has no date or time type, FAnsi creates these columns as TEXT.  <c>Insert</c> stores the string it is
    ///     given, bulk insert stores any string that parses as a date as an ISO 8601 date and time (a time on its own gets
    ///     today's date) and <c>GetDataTable</c> returns the strings rather than DateTime or TimeSpan values
    /// </summary>
    private const string DatesAreText =
        "SQLite stores dates and times as TEXT, Insert keeps the string as given, bulk insert turns any date-like string (even a bare time) into a date and time, and they come back as strings";

    protected override DatabaseType DatabaseType => DatabaseType.Sqlite;

    [Test]
    public new void TestTableCreation_NullTableName() => base.TestTableCreation_NullTableName();

    [TestCase("01/01/2007 00:00:00")]
    [TestCase("2007-01-01 00:00:00")]
    [Ignore(DatesAreText)]
    public new void DateColumnTests_NoTime(object input) => base.DateColumnTests_NoTime(input);

    [TestCase("2/28/1993 5:36:27 AM", "en-US")]
    [TestCase("28/2/1993 5:36:27 AM", "en-GB")]
    [Ignore(DatesAreText)]
    public new void DateColumnTests_UkUsFormat_Explicit(object input, string culture) =>
        base.DateColumnTests_UkUsFormat_Explicit(input, culture);

    [TestCase("2/28/1993 5:36:27 AM", "en-US")]
    [Ignore(DatesAreText)]
    public new void DateColumnTests_PrimaryKeyColumn(object input, string culture) =>
        base.DateColumnTests_PrimaryKeyColumn(input, culture);

    [TestCase("00:00:00")]
    [TestCase("00:00")]
    [Ignore(DatesAreText)]
    public new void DateColumnTests_TimeOnly_Midnight(object input) => base.DateColumnTests_TimeOnly_Midnight(input);

    [TestCase("13:11:10")]
    [TestCase("13:11")]
    [Ignore(DatesAreText)]
    public new void DateColumnTests_TimeOnly_Afternoon(object input) => base.DateColumnTests_TimeOnly_Afternoon(input);

    [TestCase("INTEGER", "-23.00")]
    [TestCase("INTEGER", "23.0")]
    [TestCase("INTEGER", "0")]
    [TestCase("INTEGER", "00.0")]
    [TestCase("INTEGER", "-24")]
    public new void TypeConsensusBetweenGuesserAndDiscoveredTableTest(string datatType, string insertValue) =>
        base.TypeConsensusBetweenGuesserAndDiscoveredTableTest(datatType, insertValue);

    [Test]
    public new void ForeignKeyCreationTest() => base.ForeignKeyCreationTest();

    [TestCase(false)]
    [TestCase(true)]
    public new void ForeignKeyCreationTest_TwoColumns(bool cascadeDelete) =>
        base.ForeignKeyCreationTest_TwoColumns(cascadeDelete);

    [Test]
    public new void CreateMaxVarcharColumns() => base.CreateMaxVarcharColumns();

    [Test]
    public new void CreateMaxVarcharColumnFromDataTable() => base.CreateMaxVarcharColumnFromDataTable();

    [Test]
    public new void CreateDateColumnFromDataTable() => base.CreateDateColumnFromDataTable();

    [Test]
    public new void CreateTable_EmptyDataTable_ExplicitTypes() => base.CreateTable_EmptyDataTable_ExplicitTypes();

    [TestCase(false)]
    [TestCase(true)]
    public new void AddColumnTest(bool useTransaction) => base.AddColumnTest(useTransaction);

    [Test]
    public new void ChangeDatabaseShouldNotAffectOriginalConnectionString_Test() =>
        base.ChangeDatabaseShouldNotAffectOriginalConnectionString_Test();

    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public new void TestDistincting(bool useTransaction, bool dodgyNames) =>
        base.TestDistincting(useTransaction, dodgyNames);

    [Test]
    public new void TestIntDataTypes() => base.TestIntDataTypes();

    [Test]
    public new void TestFloatDataTypes() => base.TestFloatDataTypes();

    [TestCase("_-o-_", ":>0<:")]
    [Ignore("A SQLite database is a file: creating one by name fails here (CreateDatabase finds it does not exist afterwards) and SQLite cannot add a primary key to an existing table")]
    public new void HorribleDatabaseAndTableNames(string horribleDatabaseName, string horribleTableName) =>
        base.HorribleDatabaseAndTableNames(horribleDatabaseName, horribleTableName);

    [TestCase("_-o-_", ":>0<:", "-_")]
    [TestCase("Comment", "SSSS", "Space Out")]
    public new void HorribleColumnNames(string horribleDatabaseName, string horribleTableName, string columnName) =>
        base.HorribleColumnNames(horribleDatabaseName, horribleTableName, columnName);

    [Test]
    public new void CreateTable_AutoIncrementColumnTest() => base.CreateTable_AutoIncrementColumnTest();

    [Test]
    public new void CreateTable_DefaultTest_Date() => base.CreateTable_DefaultTest_Date();

    [Test]
    public new void CreateTable_DefaultTest_Guid() => base.CreateTable_DefaultTest_Guid();

    [Test]
    public new void Test_BulkInserting_LotsOfDates() => base.Test_BulkInserting_LotsOfDates();
}
