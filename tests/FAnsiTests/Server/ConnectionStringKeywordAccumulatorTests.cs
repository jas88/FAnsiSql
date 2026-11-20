using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
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
    /// <summary>How long a test waits for another thread before it gives up</summary>
    private static readonly TimeSpan WaitLimit = TimeSpan.FromSeconds(30);

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

    /// <summary>
    ///     Adding a keyword checks whether it is an alias of one already added, with the one connection string builder of
    ///     the accumulator, so a keyword added from another thread at the same time has to wait.  The test holds Alpha
    ///     part way through that check, once it is in the builder, and adds Beta meanwhile.  When Beta did not wait, it
    ///     cleared the builder and put itself there, so Alpha was taken for an alias of Beta: Beta got Alpha's value and
    ///     Alpha was lost.
    /// </summary>
    [Test]
    public void TestKeywords_AddedFromTwoThreadsAtOnce_BothKept()
    {
        var builder = new PausingConnectionStringBuilder("Alpha");
        var acc = new ConnectionStringKeywordAccumulator(DatabaseType.MicrosoftSQLServer, builder);
        var testThread = Thread.CurrentThread;

        var first = Task.Run(() => acc.AddOrUpdateKeyword("Alpha", "1", ConnectionStringKeywordPriority.ApiRule));
        try
        {
            Assert.That(builder.Paused.Task.Wait(WaitLimit), "Adding Alpha never reached the connection string builder");

            // lets Alpha go on once this thread waits for it, as it should, to add Beta
            var release = Task.Run(() =>
            {
                SpinWait.SpinUntil(
                    () => builder.Release.Task.IsCompleted ||
                          (testThread.ThreadState & ThreadState.WaitSleepJoin) != 0, WaitLimit);
                builder.Release.TrySetResult();
            });

            acc.AddOrUpdateKeyword("Beta", "2", ConnectionStringKeywordPriority.ApiRule);
            builder.Release.TrySetResult();
            release.Wait();
        }
        finally
        {
            builder.Release.TrySetResult();
            first.Wait();
        }

        Assert.That(GetEnforcedKeywords(acc), Is.EqualTo("Alpha=1;Beta=2"));
    }

    /// <summary>
    ///     Returns the keywords that <paramref name="acc" /> enforces as keyword=value pairs in alphabetical order
    /// </summary>
    private static string GetEnforcedKeywords(ConnectionStringKeywordAccumulator acc)
    {
        var enforced = new DbConnectionStringBuilder();
        acc.EnforceOptions(enforced);

        return string.Join(';',
            enforced.Keys.Cast<string>().Order(StringComparer.OrdinalIgnoreCase).Select(k => $"{k}={enforced[k]}"));
    }

    /// <summary>
    ///     Holds the first thread that sets <paramref name="pauseOn" /> until <see cref="Release" /> is set, so that a
    ///     test can do something else while a keyword is being added
    /// </summary>
    private sealed class PausingConnectionStringBuilder(string pauseOn) : DbConnectionStringBuilder
    {
        private int _pauses;

        /// <summary>Set once a thread is held</summary>
        public TaskCompletionSource Paused { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Set to let the thread go on</summary>
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        [AllowNull]
        public override object this[string keyword]
        {
            get => base[keyword];
            set
            {
                base[keyword] = value;

                if (keyword != pauseOn || Interlocked.Increment(ref _pauses) != 1)
                    return;

                Paused.SetResult();
                Release.Task.Wait(WaitLimit);
            }
        }
    }
}
