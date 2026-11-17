using System.Data;
using FAnsi;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using FAnsi.Implementations.PostgreSql;
using FAnsi.Implementations.Sqlite;
using NUnit.Framework;

namespace FAnsiTests.TypeTranslation;

internal sealed class TypeTranslaterUnitTests
{
    /// <summary>
    ///     IsSupportedType is a support check for FAnsi not the DBMS.  This test shows that FAnsi's view of 'what is a string'
    ///     is pretty
    ///     broad.  We don't want to bind to DBMS / API since that would be too brittle.
    /// </summary>
    /// <param name="dbType"></param>
    /// <param name="sqlDbType"></param>
    /// <param name="expectedOutcome"></param>
    public void Test_IsSupportedType(DatabaseType dbType, string sqlDbType, bool expectedOutcome)
    {
        var tt = ImplementationManager.GetImplementation(dbType).GetQuerySyntaxHelper().TypeTranslater;
        Assert.That(tt.IsSupportedSQLDBType(sqlDbType), Is.EqualTo(expectedOutcome),
            $"Unexpected result for IsSupportedSQLDBType with {dbType}.  Input was '{sqlDbType}' expected {expectedOutcome}");
    }

    /// <summary>
    ///     Each ADO.NET provider has its own idea of what a <see cref="DbType" /> means.  Npgsql sends
    ///     <see cref="DbType.DateTime" /> as 'timestamp with time zone' and <see cref="DbType.DateTime2" /> as 'timestamp
    ///     without time zone', the other providers expect <see cref="DbType.DateTime" /> for all of their date types.
    /// </summary>
    [TestCase(DatabaseType.PostgreSql, "timestamp without time zone", DbType.DateTime2)]
    [TestCase(DatabaseType.PostgreSql, "timestamp", DbType.DateTime2)]
    [TestCase(DatabaseType.PostgreSql, "timestamp with time zone", DbType.DateTime)]
    [TestCase(DatabaseType.PostgreSql, "time without time zone", DbType.Time)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "datetime2", DbType.DateTime)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "datetime", DbType.DateTime)]
    [TestCase(DatabaseType.MySql, "datetime", DbType.DateTime)]
    [TestCase(DatabaseType.Oracle, "DATE", DbType.DateTime)]
    [TestCase(DatabaseType.Oracle, "TIMESTAMP(6)", DbType.DateTime)]
    [TestCase(DatabaseType.Sqlite, "datetime", DbType.DateTime)]
    public void Test_GetDbTypeForSQLDBType_Dates(DatabaseType dbType, string sqlDbType, DbType expected) =>
        Assert.That(GetTypeTranslater(dbType).GetDbTypeForSQLDBType(sqlDbType), Is.EqualTo(expected));

    private static ITypeTranslater GetTypeTranslater(DatabaseType dbType) =>
        dbType switch
        {
            DatabaseType.MicrosoftSQLServer => MicrosoftSQLTypeTranslater.Instance,
            DatabaseType.MySql => MySqlTypeTranslater.Instance,
            DatabaseType.Oracle => OracleTypeTranslater.Instance,
            DatabaseType.PostgreSql => PostgreSqlTypeTranslater.Instance,
            DatabaseType.Sqlite => SqliteTypeTranslater.Instance,
            _ => throw new ArgumentOutOfRangeException(nameof(dbType))
        };
}
