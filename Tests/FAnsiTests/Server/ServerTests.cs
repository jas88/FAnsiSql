using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using FAnsi;
using FAnsiTests.TestGeneration;
using NUnit.Framework;

namespace FAnsiTests.Server;

/// <summary>
/// Auto-generates test methods for each database type by reflecting over ServerTestsBase.
/// To add a new test: Just add a protected method to ServerTestsBase - it will automatically
/// run on all 5 databases with zero additional code needed!
/// </summary>
internal sealed class ServerTests : ServerTestsBase
{
    private static readonly DatabaseType[] AllDatabases =
    [
        DatabaseType.MicrosoftSQLServer,
        DatabaseType.MySql,
        DatabaseType.Oracle,
        DatabaseType.PostgreSql,
        DatabaseType.Sqlite
    ];

    /// <summary>
    /// Dynamically generates test cases by discovering all protected methods in the base class
    /// that take DatabaseType as the first parameter. Respects [SkipDatabase] and [OnlyDatabase] attributes.
    /// </summary>
    public static IEnumerable<TestCaseData> GetTestCases()
    {
        var baseType = typeof(ServerTestsBase);
        var methods = baseType.GetMethods(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
            .Where(m => m.IsFamily || m.IsFamilyOrAssembly) // protected or protected internal
            .Where(m => m.ReturnType == typeof(void))
            .Where(m => m.GetParameters().Length > 0 && m.GetParameters()[0].ParameterType == typeof(DatabaseType));

        foreach (var method in methods)
        {
            // Determine which databases to test based on attributes
            var databasesToTest = GetDatabasesForMethod(method);
            var parameters = method.GetParameters();

            if (parameters.Length == 1)
            {
                // Simple test: void TestName(DatabaseType type)
                foreach (var db in databasesToTest)
                {
                    yield return new TestCaseData(method.Name, db)
                        .SetName($"{method.Name}_{db}");
                }
            }
            else if (parameters.Length == 2 && parameters[1].ParameterType == typeof(bool))
            {
                // Test with bool parameter: void TestName(DatabaseType type, bool flag)
                foreach (var db in databasesToTest)
                {
                    yield return new TestCaseData(method.Name, db, true)
                        .SetName($"{method.Name}_{db}_True");
                    yield return new TestCaseData(method.Name, db, false)
                        .SetName($"{method.Name}_{db}_False");
                }
            }
            // Add more parameter patterns as needed
        }
    }

    /// <summary>
    /// Determines which databases a test should run on based on [SkipDatabase] and [OnlyDatabase] attributes.
    /// </summary>
    private static IEnumerable<DatabaseType> GetDatabasesForMethod(MethodInfo method)
    {
        // Check for OnlyDatabase attribute (takes precedence)
        var onlyAttr = method.GetCustomAttribute<OnlyDatabaseAttribute>();
        if (onlyAttr != null)
            return onlyAttr.IncludedDatabases;

        // Check for SkipDatabase attributes (can have multiple)
        var skipAttrs = method.GetCustomAttributes<SkipDatabaseAttribute>().ToArray();
        if (skipAttrs.Length > 0)
        {
            var excluded = skipAttrs.SelectMany(a => a.ExcludedDatabases).Distinct().ToArray();
            return AllDatabases.Except(excluded);
        }

        // No filtering - run on all databases
        return AllDatabases;
    }

    [TestCaseSource(nameof(GetTestCases))]
    public void RunTest(string methodName, params object[] args)
    {
        var method = typeof(ServerTestsBase).GetMethod(methodName,
            BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);

        if (method == null)
            throw new InvalidOperationException($"Method {methodName} not found");

        try
        {
            method.Invoke(this, args);
        }
        catch (TargetInvocationException ex)
        {
            // Unwrap the actual exception from reflection
            throw ex.InnerException ?? ex;
        }
    }
}
