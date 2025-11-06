using System;
using System.Collections.Generic;
using System.Linq;
using FAnsi;
using NUnit.Framework;

namespace FAnsiTests;

/// <summary>
/// Provides database-specific TestCaseSource arrays for each test project.
/// Each test project should only run tests for its specific database type.
/// </summary>
public sealed class TestProjectDatabaseTypes
{
    /// <summary>
    /// Returns only Microsoft SQL Server for the Microsoft SQL Server test project.
    /// </summary>
    public static readonly DatabaseType[] MicrosoftSqlServerOnly = [DatabaseType.MicrosoftSQLServer];

    /// <summary>
    /// Returns only MySQL for the MySQL test project.
    /// </summary>
    public static readonly DatabaseType[] MySqlOnly = [DatabaseType.MySql];

    /// <summary>
    /// Returns only Oracle for the Oracle test project.
    /// </summary>
    public static readonly DatabaseType[] OracleOnly = [DatabaseType.Oracle];

    /// <summary>
    /// Returns only PostgreSQL for the PostgreSQL test project.
    /// </summary>
    public static readonly DatabaseType[] PostgreSqlOnly = [DatabaseType.PostgreSql];

    /// <summary>
    /// Returns only SQLite for the SQLite test project.
    /// </summary>
    public static readonly DatabaseType[] SqliteOnly = [DatabaseType.Sqlite];

    /// <summary>
    /// Returns the appropriate database type array based on the current test project.
    /// This uses compile-time constants defined in each test project's .csproj file.
    /// </summary>
    public static DatabaseType[] GetCurrentProjectDatabaseTypes()
    {
#if FANSI_CORE_TESTS
        return All.DatabaseTypes;
#elif FANSI_MICROSOFTSQL_TESTS
        return MicrosoftSqlServerOnly;
#elif FANSI_MYSQL_TESTS
        return MySqlOnly;
#elif FANSI_ORACLE_TESTS
        return OracleOnly;
#elif FANSI_POSTGRESQL_TESTS
        return PostgreSqlOnly;
#elif FANSI_SQLITE_TESTS
        return SqliteOnly;
#else
        throw new InvalidOperationException(
            "No FANSI_*_TESTS compile constant defined. Please add the appropriate DefineConstants to the test project's .csproj file. " +
            "Valid constants are: FANSI_CORE_TESTS, FANSI_MICROSOFTSQL_TESTS, FANSI_MYSQL_TESTS, FANSI_ORACLE_TESTS, FANSI_POSTGRESQL_TESTS, FANSI_SQLITE_TESTS");
#endif
    }

    /// <summary>
    /// Returns the appropriate database type array for tests that should run on all databases except SQLite.
    /// This is filtered based on the current test project.
    /// </summary>
    public static DatabaseType[] GetCurrentProjectDatabaseTypesExceptSqlite()
    {
#if FANSI_SQLITE_TESTS
        // SQLite project should not run tests that exclude SQLite
        return [];
#else
        return GetCurrentProjectDatabaseTypes().Where(dt => dt != DatabaseType.Sqlite).ToArray();
#endif
    }

    /// <summary>
    /// Returns the appropriate database type array with boolean flags for the current test project.
    /// This matches the All.DatabaseTypesWithBoolFlags format but filtered for the current project.
    /// </summary>
    public static object[] GetCurrentProjectDatabaseTypesWithBoolFlags()
    {
        var databaseTypes = GetCurrentProjectDatabaseTypes();
        var result = new List<object>();

        foreach (var dbType in databaseTypes)
        {
            result.Add(new object[] { dbType, true });
            result.Add(new object[] { dbType, false });
        }

        return result.ToArray();
    }

    /// <summary>
    /// Returns the appropriate database type array with two boolean flags for the current test project.
    /// This matches the All.DatabaseTypesWithTwoBoolFlags format but filtered for the current project.
    /// </summary>
    public static object[] GetCurrentProjectDatabaseTypesWithTwoBoolFlags()
    {
        var databaseTypes = GetCurrentProjectDatabaseTypes();
        var result = new List<object>();

        foreach (var dbType in databaseTypes)
        {
            result.Add(new object[] { dbType, true, true });
            result.Add(new object[] { dbType, true, false });
            result.Add(new object[] { dbType, false, true });
            result.Add(new object[] { dbType, false, false });
        }

        return result.ToArray();
    }
}
