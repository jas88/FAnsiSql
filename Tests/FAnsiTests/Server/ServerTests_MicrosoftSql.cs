using FAnsi;
using NUnit.Framework;

namespace FAnsiTests.Server;

internal sealed class ServerTests_MicrosoftSql : ServerTestsBase
{
    private const DatabaseType DbType = DatabaseType.MicrosoftSQLServer;

    [Test]
    public void Server_Exists() => Server_Exists(DbType);

    [Test]
    public void Server_Constructors() => Server_Constructors(DbType);

    [Test]
    public void Server_RespondsWithinTime() => Server_RespondsWithinTime(DbType);

    [Test]
    public void ServerHelper_GetCurrentDatabase_WhenNoneSpecified() => ServerHelper_GetCurrentDatabase_WhenNoneSpecified(DbType);

    [Test]
    public void ServerHelper_GetConnectionStringBuilder() => ServerHelper_GetConnectionStringBuilder(DbType);

    [TestCase(true)]
    [TestCase(false)]
    public void ServerHelper_GetConnectionStringBuilder_NoDatabase(bool useWhitespace) => ServerHelper_GetConnectionStringBuilder_NoDatabase(DbType, useWhitespace);

    [Test]
    public void ServerHelper_ChangeDatabase() => ServerHelper_ChangeDatabase(DbType);

    [TestCase(true)]
    [TestCase(false)]
    public void ServerHelper_ChangeDatabase_AdHoc(bool useApiFirst) => ServerHelper_ChangeDatabase_AdHoc(DbType, useApiFirst);

    [Test]
    public void TestServer_GetVersion() => TestServer_GetVersion(DbType);
}
