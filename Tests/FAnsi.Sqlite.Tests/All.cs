using FAnsi;
using NUnit.Framework;

namespace FAnsiTests;

/// <summary>
/// SQLite specific tests
/// </summary>
public sealed class All
{
    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on SQLite
    /// </summary>
    public static readonly DatabaseType[] DatabaseTypes = [
        // SQLite tests would go here when SQLite support is fully implemented
        // Currently SQLite is not fully supported in FAnsi
    ];

    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on SQLite
    /// with both permutations of true/false.  Matches exhaustively method signature (DatabaseType,bool)
    /// </summary>
    public static readonly object[] DatabaseTypesWithBoolFlags = [
        // SQLite tests would go here when SQLite support is fully implemented
    ];

    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on SQLite
    /// with all permutations of true/false for 2 args.  Matches exhaustively method signature (DatabaseType,bool,bool)
    /// </summary>
    public static readonly object[] DatabaseTypesWithTwoBoolFlags = [
        // SQLite tests would go here when SQLite support is fully implemented
    ];
}
