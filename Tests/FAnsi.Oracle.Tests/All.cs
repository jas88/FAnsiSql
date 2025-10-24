using FAnsi;
using NUnit.Framework;

namespace FAnsiTests;

/// <summary>
/// Oracle specific tests
/// </summary>
public sealed class All
{
    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on Oracle
    /// </summary>
    public static readonly DatabaseType[] DatabaseTypes = [
        DatabaseType.Oracle
    ];

    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on Oracle
    /// with both permutations of true/false.  Matches exhaustively method signature (DatabaseType,bool)
    /// </summary>
    public static readonly object[] DatabaseTypesWithBoolFlags = [
        new object[] {DatabaseType.Oracle,true},
        new object[] {DatabaseType.Oracle,false}
    ];

    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on Oracle
    /// with all permutations of true/false for 2 args.  Matches exhaustively method signature (DatabaseType,bool,bool)
    /// </summary>
    public static readonly object[] DatabaseTypesWithTwoBoolFlags = [
        new object[] {DatabaseType.Oracle,true,true},
        new object[] {DatabaseType.Oracle,true,false},
        new object[] {DatabaseType.Oracle,false,true},
        new object[] {DatabaseType.Oracle,false,false}
    ];
}
