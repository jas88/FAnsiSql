using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using FAnsi.Implementations.PostgreSql;
using FAnsi.Implementations.Sqlite;
using NUnit.Framework;
using System.Linq;

#pragma warning disable NUnit2022 // IEnumerable<IImplementation> Count analyzer false positive

namespace FAnsiTests;

internal sealed class ImplementationManagerLoadTests
{
    [OneTimeSetUp]
    public void Setup()
    {
        // Explicit loading for tests (ModuleInitializer timing is unreliable in test runners)
#pragma warning disable CS0618 // Type or member is obsolete
        ImplementationManager.Load<MicrosoftSQLImplementation>();
        ImplementationManager.Load<MySqlImplementation>();
        ImplementationManager.Load<OracleImplementation>();
        ImplementationManager.Load<PostgreSqlImplementation>();
        ImplementationManager.Load<SqliteImplementation>();
#pragma warning restore CS0618 // Type or member is obsolete
    }

    [Test]
    public void Test_LoadAssemblies_FromDirectory()
    {
        Assert.That(ImplementationManager.GetImplementations().Count(), Is.GreaterThanOrEqualTo(3));
    }
}

#pragma warning restore NUnit2022 // IEnumerable<IImplementation> Count analyzer false positive
