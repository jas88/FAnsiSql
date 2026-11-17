using System.Data.Common;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.ConnectionStringDefaults;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using FAnsi.Implementations.PostgreSql;
using FAnsi.Implementations.Sqlite;
using NUnit.Framework;
using Oracle.ManagedDataAccess.Client;

namespace FAnsiTests.Server;

public sealed class ConnectionStringKeywordAccumulatorTests
{
    private readonly Dictionary<DatabaseType, IDiscoveredServerHelper> _helpers = new()
    {
        { DatabaseType.MicrosoftSQLServer, MicrosoftSQLServerHelper.Instance },
        { DatabaseType.MySql, MySqlServerHelper.Instance },
        { DatabaseType.Oracle, OracleServerHelper.Instance },
        { DatabaseType.PostgreSql, PostgreSqlServerHelper.Instance },
        { DatabaseType.Sqlite, SqliteServerHelper.Instance }
    };

    [Test]
    public void TestKeywords()
    {
        var acc = new ConnectionStringKeywordAccumulator(DatabaseType.MySql);
        acc.AddOrUpdateKeyword("Auto Enlist", "false", ConnectionStringKeywordPriority.SystemDefaultLow);

        var connectionStringBuilder = _helpers[DatabaseType.MySql]
            .GetConnectionStringBuilder("localhost", "mydb", "frank", "kangaro");

        Assert.That(connectionStringBuilder.ConnectionString, Does.Not.Contain("auto enlist"));

        acc.EnforceOptions(connectionStringBuilder);

        Assert.That(connectionStringBuilder.ConnectionString.Contains("Auto Enlist=false",
            StringComparison.InvariantCultureIgnoreCase));
    }


    [Test]
    public void TestKeywords_OverrideWithLowerPriority_Ignored()
    {
        var acc = new ConnectionStringKeywordAccumulator(DatabaseType.MicrosoftSQLServer);
        acc.AddOrUpdateKeyword("Pooling", "false", ConnectionStringKeywordPriority.SystemDefaultHigh);

        var connectionStringBuilder = _helpers[DatabaseType.MicrosoftSQLServer]
            .GetConnectionStringBuilder("localhost", "mydb", "frank", "kangaro");

        Assert.That(connectionStringBuilder.ConnectionString, Does.Not.Contain("pooling"));

        acc.EnforceOptions(connectionStringBuilder);

        Assert.That(connectionStringBuilder.ConnectionString, Does.Contain("Pooling=False"));

        //attempt override with low priority setting it to true (note we flipped case of P just to be a curve ball)
        acc.AddOrUpdateKeyword("pooling", "true", ConnectionStringKeywordPriority.SystemDefaultLow);

        acc.EnforceOptions(connectionStringBuilder);

        Assert.That(connectionStringBuilder.ConnectionString, Does.Contain("Pooling=False"));
    }

    /// <summary>
    ///     The second keyword is an alias of the first, not the same keyword in another case, so the accumulator can only
    ///     tell they are the same by asking the connection string builder of the DBMS
    /// </summary>
    [TestCase(DatabaseType.MicrosoftSQLServer, "AttachDbFilename", @"c:\temp\db", "Initial File Name", @"x:\omg.mdf")]
    [TestCase(DatabaseType.MySql, "Database", "db1", "Initial Catalog", "db2")]
    [TestCase(DatabaseType.Oracle, "CONNECTION TIMEOUT", "10", "Connect Timeout", "20")]
    [TestCase(DatabaseType.PostgreSql, "Database", "db1", "DB", "db2")]
    [TestCase(DatabaseType.Sqlite, "Default Timeout", "10", "Command Timeout", "20")]
    public void TestKeywords_OverrideWithNovelButEquivalentKeyword_Ignored(DatabaseType databaseType, string key1,
        string value1, string equivalentKey, string value2)
    {
        //the connection string builder of the DBMS must take the two keywords to be the same one
        var aliasCheck = _helpers[databaseType].GetConnectionStringBuilder("localhost", "mydb", "frank", "kangaro");
        aliasCheck[key1] = value1;
        var keyCount = aliasCheck.Keys.Count;
        aliasCheck[equivalentKey] = value2;
        Assert.That(aliasCheck.Keys, Has.Count.EqualTo(keyCount),
            $"'{equivalentKey}' is not an alias of '{key1}' for {databaseType}");

        var acc = new ConnectionStringKeywordAccumulator(databaseType);
        acc.AddOrUpdateKeyword(key1, value1, ConnectionStringKeywordPriority.SystemDefaultHigh);

        var connectionStringBuilder =
            _helpers[databaseType].GetConnectionStringBuilder("localhost", "mydb", "frank", "kangaro");

        acc.EnforceOptions(connectionStringBuilder);

        Assert.That(connectionStringBuilder.ConnectionString, Does.Contain($"{key1}={value1}"));

        //attempt override with low priority setting it to true but also use the alias
        acc.AddOrUpdateKeyword(equivalentKey, value2, ConnectionStringKeywordPriority.SystemDefaultLow);

        acc.EnforceOptions(connectionStringBuilder);

        Assert.That(connectionStringBuilder.ConnectionString, Does.Contain($"{key1}={value1}"),
            "ConnectionStringKeywordAccumulator did not realise that keywords are equivalent");

        //and it must not keep the alias as a keyword of its own, which EnforceOptions could apply after the original
        var keywordsKept = new DbConnectionStringBuilder();
        acc.EnforceOptions(keywordsKept);
        Assert.That(keywordsKept.ContainsKey(equivalentKey), Is.False,
            "ConnectionStringKeywordAccumulator kept the alias as a keyword of its own");
    }

    [TestCase(ConnectionStringKeywordPriority.SystemDefaultHigh)] //same as current (still results in override)
    [TestCase(ConnectionStringKeywordPriority.ApiRule)]
    public void TestKeywords_OverrideWithHigherPriority_Respected(ConnectionStringKeywordPriority newPriority)
    {
        var acc = new ConnectionStringKeywordAccumulator(DatabaseType.MicrosoftSQLServer);
        acc.AddOrUpdateKeyword("Pooling", "false", ConnectionStringKeywordPriority.SystemDefaultHigh);

        var connectionStringBuilder = _helpers[DatabaseType.MicrosoftSQLServer]
            .GetConnectionStringBuilder("localhost", "mydb", "frank", "kangaro");

        Assert.That(connectionStringBuilder.ConnectionString, Does.Not.Contain("pooling"));

        acc.EnforceOptions(connectionStringBuilder);

        Assert.That(connectionStringBuilder.ConnectionString, Does.Contain("Pooling=False"));

        //attempt override with low priority setting it to true (note we flipped case of P just to be a curve ball)
        acc.AddOrUpdateKeyword("pooling", "true", newPriority);

        acc.EnforceOptions(connectionStringBuilder);

        Assert.That(connectionStringBuilder.ConnectionString, Does.Contain("Pooling=True"));
    }


    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void TestKeywords_Invalid(DatabaseType databaseType)
    {
        var acc = new ConnectionStringKeywordAccumulator(databaseType);

        // Oracle driver throws OracleException instead of ArgumentException for invalid keywords
        // Accept either exception type since the behavior is inconsistent across drivers
        var ex = Assert.Catch<Exception>(() =>
            acc.AddOrUpdateKeyword("FLIBBLE", "false", ConnectionStringKeywordPriority.SystemDefaultLow));

        Assert.Multiple(() =>
        {
            Assert.That(ex, Is.InstanceOf<ArgumentException>().Or.InstanceOf<OracleException>(),
                "Expected ArgumentException or OracleException for invalid connection string keyword");
            Assert.That(ex?.Message, Does.Contain("FLIBBLE"),
                "Exception message should contain the invalid keyword name");
        });
    }
}
