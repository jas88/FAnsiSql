using System.Linq;
using FAnsi;
using NUnit.Framework;

namespace FAnsiTests;

/// <summary>
/// Tests to validate that the test filtering is working correctly.
/// These tests verify that each test project only runs tests for its intended database type.
/// </summary>
public sealed class TestFilteringValidationTests
{
    [Test]
    public void TestGetCurrentProjectDatabaseTypes_ReturnsCorrectDatabaseType()
    {
        var databaseTypes = TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypes();

#if FANSI_CORE_TESTS
        // Core tests should return all database types
        Assert.That(databaseTypes, Has.Length.EqualTo(5),
            $"Expected exactly 5 database types for core tests, got {databaseTypes.Length}");

        // Verify all expected database types are present
        var expectedTypes = new[] {
            DatabaseType.MicrosoftSQLServer,
            DatabaseType.MySql,
            DatabaseType.Oracle,
            DatabaseType.PostgreSql,
            DatabaseType.Sqlite
        };

        foreach (var expectedType in expectedTypes)
        {
            Assert.That(databaseTypes, Contains.Item(expectedType),
                $"Expected to find {expectedType} in core tests database types");
        }
#else
        // Should return exactly one database type for specific database test projects
        Assert.That(databaseTypes, Has.Length.EqualTo(1),
            $"Expected exactly 1 database type, got {databaseTypes.Length}");

        // The database type should match the current project's compile-time constant
        var expectedType = GetExpectedDatabaseTypeForCurrentProject();
        Assert.That(databaseTypes[0], Is.EqualTo(expectedType),
            $"Expected {expectedType} but got {databaseTypes[0]}");
#endif
    }

    [Test]
    public void TestGetCurrentProjectDatabaseTypesWithBoolFlags_ReturnsCorrectCount()
    {
        var databaseTypesWithFlags = TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypesWithBoolFlags();

#if FANSI_CORE_TESTS
        // Core tests should return all combinations for all 5 database types (5 * 2 = 10)
        Assert.That(databaseTypesWithFlags, Has.Length.EqualTo(10),
            $"Expected exactly 10 database type combinations for core tests (5 types * 2 flags), got {databaseTypesWithFlags.Length}");

        // Verify all expected database types are present with both true and false
        var expectedTypes = new[] {
            DatabaseType.MicrosoftSQLServer,
            DatabaseType.MySql,
            DatabaseType.Oracle,
            DatabaseType.PostgreSql,
            DatabaseType.Sqlite
        };

        foreach (var expectedType in expectedTypes)
        {
            Assert.That(databaseTypesWithFlags.Any(objArr =>
            {
                var arr = (object[])objArr;
                return arr.Length == 2 && arr[0] is DatabaseType dt && dt == expectedType && arr[1] is bool flag && flag == true;
            }), $"Expected to find true combination for {expectedType}");
            Assert.That(databaseTypesWithFlags.Any(objArr =>
            {
                var arr = (object[])objArr;
                return arr.Length == 2 && arr[0] is DatabaseType dt && dt == expectedType && arr[1] is bool flag && flag == false;
            }), $"Expected to find false combination for {expectedType}");
        }
#else
        // Should return exactly 2 entries (true and false) for the single database type
        Assert.That(databaseTypesWithFlags, Has.Length.EqualTo(2),
            $"Expected exactly 2 database type combinations, got {databaseTypesWithFlags.Length}");

        // Verify both true and false values are present
        var databaseType = GetExpectedDatabaseTypeForCurrentProject();
        Assert.That(databaseTypesWithFlags.Any(objArr =>
        {
            var arr = (object[])objArr;
            return arr.Length == 2 && arr[0] is DatabaseType dt && dt == databaseType && arr[1] is bool flag && flag == true;
        }), "Expected to find true combination");
        Assert.That(databaseTypesWithFlags.Any(objArr =>
        {
            var arr = (object[])objArr;
            return arr.Length == 2 && arr[0] is DatabaseType dt2 && dt2 == databaseType && arr[1] is bool flag2 && flag2 == false;
        }), "Expected to find false combination");
#endif
    }

    [Test]
    public void TestGetCurrentProjectDatabaseTypesWithTwoBoolFlags_ReturnsCorrectCount()
    {
        var databaseTypesWithFlags = TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypesWithTwoBoolFlags();

#if FANSI_CORE_TESTS
        // Core tests should return all combinations for all 5 database types (5 * 4 = 20)
        Assert.That(databaseTypesWithFlags, Has.Length.EqualTo(20),
            $"Expected exactly 20 database type combinations for core tests (5 types * 4 flag combinations), got {databaseTypesWithFlags.Length}");

        // Verify all expected database types are present with all four flag combinations
        var expectedTypes = new[] {
            DatabaseType.MicrosoftSQLServer,
            DatabaseType.MySql,
            DatabaseType.Oracle,
            DatabaseType.PostgreSql,
            DatabaseType.Sqlite
        };

        var expectedCombinations = new[]
        {
            new object[] { true, true },
            new object[] { true, false },
            new object[] { false, true },
            new object[] { false, false }
        };

        foreach (var expectedType in expectedTypes)
        {
            foreach (var expectedCombo in expectedCombinations)
            {
                Assert.That(databaseTypesWithFlags.Any(objArr =>
                {
                    var arr = (object[])objArr;
                    return arr.Length == 3 &&
                           arr[0] is DatabaseType dt && dt == expectedType &&
                           arr[1] is bool flag1 && flag1 == (bool)expectedCombo[0] &&
                           arr[2] is bool flag2 && flag2 == (bool)expectedCombo[1];
                }), $"Expected to find combination {expectedCombo[0]}, {expectedCombo[1]} for {expectedType}");
            }
        }
#else
        // Should return exactly 4 entries (all combinations of true/false) for the single database type
        Assert.That(databaseTypesWithFlags, Has.Length.EqualTo(4),
            $"Expected exactly 4 database type combinations, got {databaseTypesWithFlags.Length}");

        // Verify all combinations are present
        var databaseType = GetExpectedDatabaseTypeForCurrentProject();
        var expectedCombinations = new[]
        {
            new object[] { databaseType, true, true },
            new object[] { databaseType, true, false },
            new object[] { databaseType, false, true },
            new object[] { databaseType, false, false }
        };

        foreach (var expectedCombo in expectedCombinations)
        {
            Assert.That(databaseTypesWithFlags.Any(objArr =>
            {
                var arr = (object[])objArr;
                return arr.Length == 3 &&
                       arr[0] is DatabaseType dt && dt == databaseType &&
                       arr[1] is bool flag1 && flag1 == (bool)expectedCombo[1] &&
                       arr[2] is bool flag2 && flag2 == (bool)expectedCombo[2];
            }), $"Expected to find combination {expectedCombo[1]}, {expectedCombo[2]}");
        }
#endif
    }

    [Test]
    public void TestGetCurrentProjectDatabaseTypesExceptSqlite_ForNonSqliteProjects()
    {
        var databaseTypes = TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypesExceptSqlite();

#if FANSI_CORE_TESTS
        // Core tests should return all database types except SQLite (4 total)
        Assert.That(databaseTypes, Has.Length.EqualTo(4),
            $"Expected exactly 4 database types for core tests (all except SQLite), got {databaseTypes.Length}");

        // Verify all expected database types are present
        var expectedTypes = new[] {
            DatabaseType.MicrosoftSQLServer,
            DatabaseType.MySql,
            DatabaseType.Oracle,
            DatabaseType.PostgreSql
        };

        foreach (var expectedType in expectedTypes)
        {
            Assert.That(databaseTypes, Contains.Item(expectedType),
                $"Expected to find {expectedType} in core tests database types (except SQLite)");
        }
#else
        var expectedType = GetExpectedDatabaseTypeForCurrentProject();

        if (expectedType == DatabaseType.Sqlite)
        {
            // SQLite project should return empty array for "except SQLite" tests
            Assert.That(databaseTypes, Is.Empty, "SQLite project should return empty array for DatabaseTypesExceptSqlite");
        }
        else
        {
            // Non-SQLite projects should return their single database type
            Assert.That(databaseTypes, Has.Length.EqualTo(1),
                $"Non-SQLite project should return 1 database type, got {databaseTypes.Length}");
            Assert.That(databaseTypes[0], Is.EqualTo(expectedType),
                $"Expected {expectedType} but got {databaseTypes[0]}");
        }
#endif
    }

    private static DatabaseType GetExpectedDatabaseTypeForCurrentProject()
    {
#if FANSI_CORE_TESTS
        // Core tests should return Microsoft SQL Server as the default for validation
        return DatabaseType.MicrosoftSQLServer;
#elif FANSI_MICROSOFTSQL_TESTS
        return DatabaseType.MicrosoftSQLServer;
#elif FANSI_MYSQL_TESTS
        return DatabaseType.MySql;
#elif FANSI_ORACLE_TESTS
        return DatabaseType.Oracle;
#elif FANSI_POSTGRESQL_TESTS
        return DatabaseType.PostgreSql;
#elif FANSI_SQLITE_TESTS
        return DatabaseType.Sqlite;
#else
        throw new System.InvalidOperationException("No test project constant defined - this should not happen in a proper test project");
#endif
    }
}
