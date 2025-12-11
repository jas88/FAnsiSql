namespace FAnsiTests.TestGeneration;

/// <summary>
///     Marks a test base class for automatic per-database test generation.
///     The source generator will create a concrete test class for each database type
///     (MySql, MicrosoftSQLServer, Oracle, PostgreSql, Sqlite) with [Test] methods
///     that delegate to the protected methods in this base class.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
internal sealed class GeneratePerDatabaseTestsAttribute : Attribute
{
    /// <summary>
    ///     Optional: Exclude specific database types from generation.
    /// </summary>
    public string[]? ExcludeDatabaseTypes { get; set; }
}
