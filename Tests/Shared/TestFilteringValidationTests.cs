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

        // Should return exactly one database type
        Assert.That(databaseTypes, Has.Length.EqualTo(1),
            $"Expected exactly 1 database type, got {databaseTypes.Length}");

        // The database type should match the current project's compile-time constant
        var expectedType = GetExpectedDatabaseTypeForCurrentProject();
        Assert.That(databaseTypes[0], Is.EqualTo(expectedType),
            $"Expected {expectedType} but got {databaseTypes[0]}");
    }

    [Test]
    public void TestGetCurrentProjectDatabaseTypesWithBoolFlags_ReturnsCorrectCount()
    {
        var databaseTypesWithFlags = TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypesWithBoolFlags();

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
    }

    [Test]
    public void TestGetCurrentProjectDatabaseTypesWithTwoBoolFlags_ReturnsCorrectCount()
    {
        var databaseTypesWithFlags = TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypesWithTwoBoolFlags();

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
    }

    [Test]
    public void TestGetCurrentProjectDatabaseTypesExceptSqlite_ForNonSqliteProjects()
    {
        var databaseTypes = TestProjectDatabaseTypes.GetCurrentProjectDatabaseTypesExceptSqlite();
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
    }

    private static DatabaseType GetExpectedDatabaseTypeForCurrentProject()
    {
#if FANSI_MICROSOFTSQL_TESTS
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