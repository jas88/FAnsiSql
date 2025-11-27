using System;
using FAnsi;

namespace FAnsiTests.TestGeneration;

/// <summary>
/// Marks a test method to skip specific database types during test generation.
/// Use this instead of Assert.Inconclusive() to prevent tests from being generated at all.
/// </summary>
/// <example>
/// [SkipDatabase(DatabaseType.Sqlite, "SQLite doesn't support feature X")]
/// protected void TestFeatureX(DatabaseType type) { }
/// </example>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
internal sealed class SkipDatabaseAttribute : Attribute
{
    public DatabaseType[] ExcludedDatabases { get; }
    public string? Reason { get; }

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

    public SkipDatabaseAttribute(DatabaseType database1, DatabaseType database2, DatabaseType database3, string? reason = null)
    {
        ExcludedDatabases = [database1, database2, database3];
        Reason = reason;
    }
}
