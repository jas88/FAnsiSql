using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using FAnsi.Implementations.PostgreSql;
using FAnsi.Implementations.Sqlite;
using Microsoft.Data.SqlClient;
using NUnit.Framework;

namespace FAnsiTests.Server;

internal sealed class ServerLevelUnitTests
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
    public void ConstructionStringBuilderTest()
    {
        var b = new SqlConnectionStringBuilder("Server=localhost;Database=RDMP_Catalogue;User ID=SA;Password=blah;Trust Server Certificate=true;Encrypt=True")
        {
            InitialCatalog = "master"
        };

        Assert.That(b.ConnectionString, Is.EqualTo("Data Source=localhost;Initial Catalog=master;User ID=SA;Password=blah;Encrypt=True;Trust Server Certificate=True"));
    }
}
