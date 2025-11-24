using FAnsi;
using NUnit.Framework;

namespace FAnsiTests;

#if SQLITE_TESTS
internal sealed class ManagedConnectionTests_Sqlite : ManagedConnectionTestsBase
{
    protected override DatabaseType DatabaseType => DatabaseType.Sqlite;

    [Test]
    public void Test_GetConnection_NotOpenAtStart() => Test_GetConnection_NotOpenAtStart();

    [Test]
    public void Test_GetManagedConnection_AutoOpenClose() => Test_GetManagedConnection_AutoOpenClose();

    [Test]
    public void Test_BeginNewTransactedConnection_AutoOpenClose() => Test_BeginNewTransactedConnection_AutoOpenClose();

    [Test]
    public void Test_GetManagedConnection_OngoingTransaction() => Test_GetManagedConnection_OngoingTransaction();

    [TestCase(false)]
    [TestCase(true)]
    public void Test_GetManagedConnection_OngoingTransaction_WithCommitRollback(bool commit) =>
        Test_GetManagedConnection_OngoingTransaction_WithCommitRollback(commit);

    [Test]
    public void Test_ManagedTransaction_MultipleCancel() => Test_ManagedTransaction_MultipleCancel();

    [Test]
    public void Test_Clone_AutoOpenClose() => Test_Clone_AutoOpenClose();
}
#endif
