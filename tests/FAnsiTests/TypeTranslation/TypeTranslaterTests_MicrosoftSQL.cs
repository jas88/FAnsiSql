using System;
using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.TypeTranslation;

internal sealed class TypeTranslaterTests_MicrosoftSQL : TypeTranslaterTests
{
    private const DatabaseType DbType = DatabaseType.MicrosoftSQLServer;

    [Test]
    public void Test_CSharpToDbType_String10() => Test_CSharpToDbType_String10(DbType, "varchar(10)");

    [Test]
    public void Test_CSharpToDbType_StringMax() => Test_CSharpToDbType_StringMax(DbType, "varchar(max)");

    [TestCase("varchar(max)", false)]
    [TestCase("nvarchar(max)", true)]
    [TestCase("text", false)]
    [TestCase("ntext", true)]
    public void Test_GetLengthIfString_VarcharMaxCols(string datatype, bool expectUnicode) =>
        Test_GetLengthIfString_VarcharMaxCols(DbType, datatype, expectUnicode);

    [TestCase("bigint", typeof(long))]
    [TestCase("binary", typeof(byte[]))]
    [TestCase("bit", typeof(bool))]
    [TestCase("char", typeof(string))]
    [TestCase("date", typeof(DateTime))]
    [TestCase("datetime", typeof(DateTime))]
    [TestCase("datetime2", typeof(DateTime))]
    [TestCase("datetimeoffset", typeof(DateTime))]
    [TestCase("decimal", typeof(decimal))]
    [TestCase("varbinary(max)", typeof(byte[]))]
    [TestCase("float", typeof(decimal))]
    [TestCase("image", typeof(byte[]))]
    [TestCase("int", typeof(int))]
    [TestCase("money", typeof(decimal))]
    [TestCase("nchar", typeof(string))]
    [TestCase("ntext", typeof(string))]
    [TestCase("numeric", typeof(decimal))]
    [TestCase("nvarchar", typeof(string))]
    [TestCase("real", typeof(decimal))]
    [TestCase("rowversion", typeof(byte[]))]
    [TestCase("smalldatetime", typeof(DateTime))]
    [TestCase("smallint", typeof(short))]
    [TestCase("smallmoney", typeof(decimal))]
    [TestCase("text", typeof(string))]
    [TestCase("time", typeof(TimeSpan))]
    [TestCase("timestamp", typeof(byte[]))]
    [TestCase("tinyint", typeof(byte))]
    [TestCase("uniqueidentifier", typeof(Guid))]
    [TestCase("varbinary", typeof(byte[]))]
    [TestCase("varchar", typeof(string))]
    [TestCase("xml", typeof(string))]
    public void TestIsKnownType(string sqlType, Type expectedType) =>
        TestIsKnownType(DbType, sqlType, expectedType);

    [TestCase("sql_variant")]
    public void TestNotSupportedTypes(string sqlType) =>
        TestNotSupportedTypes(DbType, sqlType);
}
