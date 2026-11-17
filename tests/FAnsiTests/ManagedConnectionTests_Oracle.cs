using FAnsi;
using NUnit.Framework;

namespace FAnsiTests;

internal sealed class ManagedConnectionTests_Oracle : ManagedConnectionTestsBase
{
    protected override DatabaseType DatabaseType => DatabaseType.Oracle;

    [Test]
    public new void Test_GetConnection_NotOpenAtStart() => base.Test_GetConnection_NotOpenAtStart();

    [Test]
    public new void Test_GetManagedConnection_AutoOpenClose() => base.Test_GetManagedConnection_AutoOpenClose();

    [Test]
    public new void Test_BeginNewTransactedConnection_AutoOpenClose() => base.Test_BeginNewTransactedConnection_AutoOpenClose();

    [Test]
    public new void Test_GetManagedConnection_OngoingTransaction() => base.Test_GetManagedConnection_OngoingTransaction();

    [TestCase(false)]
    [TestCase(true)]
    public new void Test_GetManagedConnection_OngoingTransaction_WithCommitRollback(bool commit) =>
        base.Test_GetManagedConnection_OngoingTransaction_WithCommitRollback(commit);

    [Test]
    public new void Test_ManagedTransaction_MultipleCancel() => base.Test_ManagedTransaction_MultipleCancel();

    [Test]
    public new void Test_Clone_AutoOpenClose() => base.Test_Clone_AutoOpenClose();
}
