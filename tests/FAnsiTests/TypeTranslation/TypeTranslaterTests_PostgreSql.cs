using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.TypeTranslation;

internal sealed class TypeTranslaterTests_PostgreSql : TypeTranslaterTests
{
    private const DatabaseType DbType = DatabaseType.PostgreSql;

    [Test]
    public void Test_CSharpToDbType_String10() => Test_CSharpToDbType_String10(DbType, "varchar(10)");

    [Test]
    public void Test_CSharpToDbType_StringMax() => Test_CSharpToDbType_StringMax(DbType, "text");

    [TestCase("text", false)]
    public void Test_GetLengthIfString_VarcharMaxCols(string datatype, bool expectUnicode) =>
        Test_GetLengthIfString_VarcharMaxCols(DbType, datatype, expectUnicode);
}
