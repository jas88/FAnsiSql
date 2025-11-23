using System;
using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.TypeTranslation;

internal sealed class TypeTranslaterTests_Oracle : TypeTranslaterTests
{
    private const DatabaseType DbType = DatabaseType.Oracle;

    [Test]
    public void Test_CSharpToDbType_String10() => Test_CSharpToDbType_String10(DbType, "varchar2(10)");

    [Test]
    public void Test_CSharpToDbType_StringMax() => Test_CSharpToDbType_StringMax(DbType, "CLOB");

    [TestCase("CLOB", false)]
    public void Test_GetLengthIfString_VarcharMaxCols(string datatype, bool expectUnicode) =>
        Test_GetLengthIfString_VarcharMaxCols(DbType, datatype, expectUnicode);

    [TestCase("varchar2(10)", typeof(string))]
    [TestCase("CHAR(10)", typeof(string))]
    [TestCase("CHAR", typeof(string))]
    [TestCase("nchar", typeof(string))]
    [TestCase("nvarchar2(1)", typeof(string))]
    [TestCase("clob", typeof(string))]
    [TestCase("nclob", typeof(string))]
    [TestCase("long", typeof(string))]
    [TestCase("NUMBER", typeof(decimal))]
    [TestCase("date", typeof(DateTime))]
    [TestCase("BLOB", typeof(byte[]))]
    [TestCase("BFILE", typeof(byte[]))]
    [TestCase("RAW(100)", typeof(byte[]))]
    [TestCase("LONG RAW", typeof(byte[]))]
    [TestCase("ROWID", typeof(byte[]))]
    [TestCase("CHARACTER", typeof(string))]
    [TestCase("FLOAT", typeof(decimal))]
    [TestCase("FLOAT(5)", typeof(decimal))]
    [TestCase("REAL", typeof(decimal))]
    [TestCase("DOUBLE PRECISION", typeof(decimal))]
    [TestCase("CHARACTER VARYING(10)", typeof(string))]
    [TestCase("CHAR VARYING(10)", typeof(string))]
    [TestCase("LONG VARCHAR", typeof(string))]
    [TestCase("DEC(3,2)", typeof(decimal))]
    [TestCase("DEC(*,3)", typeof(decimal))]
    [TestCase("INTEGER", typeof(int))]
    [TestCase("INT", typeof(int))]
    [TestCase("SMALLINT", typeof(int))]
    public void TestIsKnownType(string sqlType, Type expectedType) =>
        TestIsKnownType(DbType, sqlType, expectedType);

    [TestCase("MLSLABEL")]
    public void TestNotSupportedTypes(string sqlType) =>
        TestNotSupportedTypes(DbType, sqlType);
}
