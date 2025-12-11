using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.TypeTranslation;

internal sealed class TypeTranslaterTests_MySql : TypeTranslaterTests
{
    private const DatabaseType DbType = DatabaseType.MySql;

    [Test]
    public void Test_CSharpToDbType_String10() => Test_CSharpToDbType_String10(DbType, "varchar(10)");

    [Test]
    public void Test_CSharpToDbType_StringMax() => Test_CSharpToDbType_StringMax(DbType, "longtext");

    [TestCase("longtext", false)]
    public void Test_GetLengthIfString_VarcharMaxCols(string datatype, bool expectUnicode) =>
        Test_GetLengthIfString_VarcharMaxCols(DbType, datatype, expectUnicode);

    [TestCase("BOOL", typeof(bool))]
    [TestCase("BOOLEAN", typeof(bool))]
    [TestCase("TINYINT", typeof(byte))]
    [TestCase("CHARACTER VARYING(10)", typeof(string))]
    [TestCase("FIXED", typeof(decimal))]
    [TestCase("DEC", typeof(decimal))]
    [TestCase("VARCHAR(10)", typeof(string))]
    [TestCase("DECIMAL", typeof(decimal))]
    [TestCase("FLOAT4", typeof(decimal))]
    [TestCase("FLOAT", typeof(decimal))]
    [TestCase("FLOAT8", typeof(decimal))]
    [TestCase("DOUBLE", typeof(decimal))]
    [TestCase("INT1", typeof(byte))]
    [TestCase("INT2", typeof(short))]
    [TestCase("INT3", typeof(int))]
    [TestCase("INT4", typeof(int))]
    [TestCase("INT8", typeof(long))]
    [TestCase("INT(1)", typeof(int))]
    [TestCase("INT(2)", typeof(int))]
    [TestCase("INT(3)", typeof(int))]
    [TestCase("INT(4)", typeof(int))]
    [TestCase("INT(8)", typeof(int))]
    [TestCase("SMALLINT", typeof(short))]
    [TestCase("MEDIUMINT", typeof(int))]
    [TestCase("INT", typeof(int))]
    [TestCase("BIGINT", typeof(long))]
    [TestCase("LONG VARBINARY", typeof(byte[]))]
    [TestCase("MEDIUMBLOB", typeof(byte[]))]
    [TestCase("LONG VARCHAR", typeof(string))]
    [TestCase("MEDIUMTEXT", typeof(string))]
    [TestCase("LONG", typeof(string))]
    [TestCase("MIDDLEINT", typeof(int))]
    [TestCase("NUMERIC", typeof(decimal))]
    [TestCase("INTEGER", typeof(int))]
    [TestCase("BIT", typeof(bool))]
    [TestCase("SMALLINT(3)", typeof(short))]
    [TestCase("INT UNSIGNED", typeof(int))]
    [TestCase("INT UNSIGNED ZEROFILL", typeof(int))]
    [TestCase("SMALLINT UNSIGNED", typeof(short))]
    [TestCase("SMALLINT ZEROFILL UNSIGNED", typeof(short))]
    [TestCase("LONGTEXT", typeof(string))]
    [TestCase("CHAR(10)", typeof(string))]
    [TestCase("TEXT", typeof(string))]
    [TestCase("BLOB", typeof(byte[]))]
    [TestCase("ENUM('fish','carrot')", typeof(string))]
    [TestCase("SET('fish','carrot')", typeof(string))]
    [TestCase("VARBINARY(10)", typeof(byte[]))]
    [TestCase("date", typeof(DateTime))]
    [TestCase("datetime", typeof(DateTime))]
    [TestCase("TIMESTAMP", typeof(DateTime))]
    [TestCase("TIME", typeof(TimeSpan))]
    [TestCase("nchar", typeof(string))]
    [TestCase("nvarchar(10)", typeof(string))]
    [TestCase("real", typeof(decimal))]
    public void TestIsKnownType(string sqlType, Type expectedType) =>
        TestIsKnownType(DbType, sqlType, expectedType);

    [TestCase("GEOMETRY")]
    [TestCase("POINT")]
    [TestCase("LINESTRING")]
    [TestCase("POLYGON")]
    [TestCase("MULTIPOINT")]
    [TestCase("MULTILINESTRING")]
    [TestCase("MULTIPOLYGON")]
    [TestCase("GEOMETRYCOLLECTION")]
    public void TestNotSupportedTypes(string sqlType) =>
        TestNotSupportedTypes(DbType, sqlType);
}
