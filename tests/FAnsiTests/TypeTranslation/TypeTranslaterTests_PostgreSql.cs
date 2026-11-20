using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.TypeTranslation;

internal sealed class TypeTranslaterTests_PostgreSql : TypeTranslaterTests
{
    private const DatabaseType DbType = DatabaseType.PostgreSql;

    [Test]
    public void Test_CSharpToDbType_String10() => Test_CSharpToDbType_String10(DbType, "varchar(10)");

    [Test]
    public void Test_CSharpToDbType_StringWidthNotSupplied() =>
        Test_CSharpToDbType_StringWidthNotSupplied(DbType, "varchar(4000)", "varchar(4000)");

    [Test]
    public void Test_CSharpToDbType_StringMax() => Test_CSharpToDbType_StringMax(DbType, "text");

    [TestCase("text", false)]
    public void Test_GetLengthIfString_VarcharMaxCols(string datatype, bool expectUnicode) =>
        Test_GetLengthIfString_VarcharMaxCols(DbType, datatype, expectUnicode);

    /// <summary>
    ///     The column created as each type is discovered with the name information_schema gives it (e.g. 'time' becomes
    ///     'time without time zone'), which must map to the same C# type
    /// </summary>
    [TestCase("date", typeof(DateTime))]
    [TestCase("time", typeof(TimeSpan))]
    [TestCase("time(3)", typeof(TimeSpan))]
    [TestCase("time without time zone", typeof(TimeSpan))]
    [TestCase("time with time zone", typeof(TimeSpan))]
    [TestCase("timetz", typeof(TimeSpan))]
    [TestCase("interval", typeof(TimeSpan))]
    [TestCase("timestamp", typeof(DateTime))]
    [TestCase("timestamp(3)", typeof(DateTime))]
    [TestCase("timestamp with time zone", typeof(DateTime))]
    [TestCase("timestamptz", typeof(DateTime))]
    public void TestIsKnownType(string sqlType, Type expectedType) =>
        TestIsKnownType(DbType, sqlType, expectedType);

    /// <summary>
    ///     Types that are named like a date or a time but are not one
    /// </summary>
    [TestCase("daterange")]
    [TestCase("datemultirange")]
    [TestCase("tsrange")]
    [TestCase("tstzrange")]
    public void TestNotSupportedTypes(string sqlType) =>
        TestNotSupportedTypes(DbType, sqlType);
}
