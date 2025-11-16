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
        DatabaseType.Sqlite
    ];

    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on SQLite
    /// with both permutations of true/false.  Matches exhaustively method signature (DatabaseType,bool)
    /// </summary>
    public static readonly object[] DatabaseTypesWithBoolFlags = [
        new object[] {DatabaseType.Sqlite,true},
        new object[] {DatabaseType.Sqlite,false}
    ];

    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on SQLite
    /// with all permutations of true/false for 2 args.  Matches exhaustively method signature (DatabaseType,bool,bool)
    /// </summary>
    public static readonly object[] DatabaseTypesWithTwoBoolFlags = [
        new object[] {DatabaseType.Sqlite,true,true},
        new object[] {DatabaseType.Sqlite,true,false},
        new object[] {DatabaseType.Sqlite,false,true},
        new object[] {DatabaseType.Sqlite,false,false}
    ];
}
