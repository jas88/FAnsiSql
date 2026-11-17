using FAnsi;
using FAnsi.Implementations.Sqlite;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.TypeTranslation;

internal sealed class TypeTranslaterTests_SQLite : TypeTranslaterTests
{
    private const DatabaseType DbType = DatabaseType.Sqlite;

    [TestCase("TEXT", typeof(string))]
    [TestCase("INTEGER", typeof(int))]
    [TestCase("REAL", typeof(decimal))]
    [TestCase("BLOB", typeof(byte[]))]
    [TestCase("BOOLEAN", typeof(bool))]
    [TestCase("DATETIME", typeof(DateTime))]
    [TestCase("TIME", typeof(TimeSpan))]
    public void TestIsKnownType(string sqlType, Type expectedType) => TestIsKnownType(DbType, sqlType, expectedType);

    /// <summary>
    ///     SQLite has no boolean, date or time types.  FAnsi declares these columns BOOLEAN, DATETIME and TIME so that they
    ///     are discovered as the type they were created for.  It used to declare them INTEGER and TEXT, which are
    ///     discovered as int and string.
    /// </summary>
    [TestCase(typeof(bool), "BOOLEAN", typeof(bool))]
    [TestCase(typeof(DateTime), "DATETIME", typeof(DateTime))]
    [TestCase(typeof(DateOnly), "DATETIME", typeof(DateTime))]
    [TestCase(typeof(TimeSpan), "TIME", typeof(TimeSpan))]
    [TestCase(typeof(TimeOnly), "TIME", typeof(TimeSpan))]
    public void Test_CSharpToDbType_DiscoveredAsSameType(Type cSharpType, string expectedSqlType, Type expectedType)
    {
        var sqlType = SqliteTypeTranslater.Instance.GetSQLDBTypeForCSharpType(new DatabaseTypeRequest(cSharpType));

        Assert.That(sqlType, Is.EqualTo(expectedSqlType));
        TestIsKnownType(DbType, sqlType, expectedType);
    }
}
