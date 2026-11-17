using FAnsi;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal.Commands;

namespace FAnsiTests.TestGeneration;

/// <summary>
///     Marks a test method to run ONLY on specific database types.
///     Use this for database-specific feature tests.
///     <para>
///         A test case with any other database as an argument is reported as ignored, with <see cref="Reason" />, instead
///         of being run.  The T4 templates do not read this attribute, use <see cref="SkipDatabaseAttribute" /> there.
///     </para>
/// </summary>
/// <example>
///     [OnlyDatabase(DatabaseType.MySql, DatabaseType.PostgreSql, "Only MySQL/PostgreSQL support this")]
///     protected void TestSpecificFeature(DatabaseType type) { }
/// </example>
[AttributeUsage(AttributeTargets.Method)]
internal sealed class OnlyDatabaseAttribute : Attribute, IWrapSetUpTearDown
{
    public OnlyDatabaseAttribute(DatabaseType database, string? reason = null)
    {
        IncludedDatabases = [database];
        Reason = reason;
    }

    public OnlyDatabaseAttribute(DatabaseType database1, DatabaseType database2, string? reason = null)
    {
        IncludedDatabases = [database1, database2];
        Reason = reason;
    }

    public OnlyDatabaseAttribute(DatabaseType database1, DatabaseType database2, DatabaseType database3,
        string? reason = null)
    {
        IncludedDatabases = [database1, database2, database3];
        Reason = reason;
    }

    public OnlyDatabaseAttribute(DatabaseType database1, DatabaseType database2, DatabaseType database3,
        DatabaseType database4, string? reason = null)
    {
        IncludedDatabases = [database1, database2, database3, database4];
        Reason = reason;
    }

    public DatabaseType[] IncludedDatabases { get; }
    public string? Reason { get; }

    /// <inheritdoc />
    public TestCommand Wrap(TestCommand command) =>
        new DatabaseTypeFilterCommand(command, type => !IncludedDatabases.Contains(type), Reason);
}
