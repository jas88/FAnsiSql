using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;

namespace FAnsiTests.Database;

/// <summary>
/// Tests for DiscoveredDatabase.Exists() method across all database types.
/// Verifies that checking for non-existent databases returns false instead of throwing exceptions.
/// </summary>
internal sealed class DatabaseExistsTests : DatabaseTests
{
    /// <summary>
    /// Test that DiscoveredDatabase.Exists() returns false for non-existent databases
    /// without throwing exceptions. This is critical for PostgreSQL where connecting
    /// to a non-existent database throws an exception.
    /// </summary>
    /// <remarks>
    /// Upstream issue: HicServices/FAnsiSql#335
    /// PostgreSQL requires connecting to a system database (postgres) first to query pg_database.
    /// This fork's implementation correctly handles this by using DatabaseExists() which connects
    /// to the postgres system database before checking.
    /// </remarks>
    [TestCaseSource(typeof(TestProjectDatabaseTypes), nameof(TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypes))]
    public void DiscoveredDatabase_Exists_NonExistentDatabase_ReturnsFalse(DatabaseType dbType)
    {
        var server = GetTestServer(dbType);
        var db = server.ExpectDatabase("NonExistentDatabase_12345");

        // Should return false, not throw an exception
        Assert.That(db.Exists(), Is.False,
            $"Exists() should return false for non-existent database on {dbType}, not throw an exception");
    }

    /// <summary>
    /// Test that DiscoveredDatabase.Exists() returns true for existing databases
    /// </summary>
    [TestCaseSource(typeof(TestProjectDatabaseTypes), nameof(TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypes))]
    public void DiscoveredDatabase_Exists_ExistingDatabase_ReturnsTrue(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        Assert.That(db.Exists(), Is.True,
            $"Exists() should return true for existing database on {dbType}");
    }

    /// <summary>
    /// Test that creating and then checking a database works correctly
    /// </summary>
    [TestCaseSource(typeof(TestProjectDatabaseTypes), nameof(TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypes))]
    public void DiscoveredDatabase_Exists_AfterCreate_ReturnsTrue(DatabaseType dbType)
    {
        var server = GetTestServer(dbType);
        var dbName = $"TestExistsDB_{dbType}_{System.Guid.NewGuid():N}".Substring(0, 30);
        var db = server.ExpectDatabase(dbName);

        try
        {
            // Should not exist initially
            Assert.That(db.Exists(), Is.False, "Database should not exist before creation");

            // Create it
            db.Create();

            // Should exist now
            Assert.That(db.Exists(), Is.True, "Database should exist after creation");
        }
        finally
        {
            // Clean up
            if (db.Exists())
                db.Drop();
        }
    }
}
