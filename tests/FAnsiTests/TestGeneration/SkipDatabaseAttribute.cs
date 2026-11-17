using FAnsi;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal.Commands;

namespace FAnsiTests.TestGeneration;

/// <summary>
///     Marks a test method to skip specific database types.
///     <para>
///         On a protected method of a T4 template base class (see Server/ServerTests.tt) the template does not generate the
///         test for these databases at all.  On a test method, a test case with one of these databases as an argument is
///         reported as ignored, with <see cref="Reason" />, instead of being run.
///     </para>
///     Use this instead of Assert.Inconclusive() so that the reason a database is not tested is declared on the test.
/// </summary>
/// <example>
///     [SkipDatabase(DatabaseType.Sqlite, "SQLite doesn't support feature X")]
///     protected void TestFeatureX(DatabaseType type) { }
/// </example>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
internal sealed class SkipDatabaseAttribute : Attribute, IWrapSetUpTearDown
{
    public SkipDatabaseAttribute(DatabaseType database, string? reason = null)
    {
        ExcludedDatabases = [database];
        Reason = reason;
    }

    public SkipDatabaseAttribute(DatabaseType database1, DatabaseType database2, string? reason = null)
    {
        ExcludedDatabases = [database1, database2];
        Reason = reason;
    }

    public SkipDatabaseAttribute(DatabaseType database1, DatabaseType database2, DatabaseType database3,
        string? reason = null)
    {
        ExcludedDatabases = [database1, database2, database3];
        Reason = reason;
    }

    public DatabaseType[] ExcludedDatabases { get; }
    public string? Reason { get; }

    /// <inheritdoc />
    public TestCommand Wrap(TestCommand command) =>
        new DatabaseTypeFilterCommand(command, type => ExcludedDatabases.Contains(type), Reason);
}
