using System;
using FAnsi;

namespace FAnsiTests.TestGeneration;

/// <summary>
/// Marks a test method to run ONLY on specific database types.
/// Use this for database-specific feature tests.
/// </summary>
/// <example>
/// [OnlyDatabase(DatabaseType.MySql, DatabaseType.PostgreSql, "Only MySQL/PostgreSQL support this")]
/// protected void TestSpecificFeature(DatabaseType type) { }
/// </example>
[AttributeUsage(AttributeTargets.Method)]
internal sealed class OnlyDatabaseAttribute : Attribute
{
    public DatabaseType[] IncludedDatabases { get; }
    public string? Reason { get; }

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

    public OnlyDatabaseAttribute(DatabaseType database1, DatabaseType database2, DatabaseType database3, string? reason = null)
    {
        IncludedDatabases = [database1, database2, database3];
        Reason = reason;
    }

    public OnlyDatabaseAttribute(DatabaseType database1, DatabaseType database2, DatabaseType database3, DatabaseType database4, string? reason = null)
    {
        IncludedDatabases = [database1, database2, database3, database4];
        Reason = reason;
    }
}
