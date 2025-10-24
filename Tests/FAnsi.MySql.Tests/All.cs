using FAnsi;
using NUnit.Framework;

namespace FAnsiTests;

/// <summary>
/// MySQL specific tests
/// </summary>
public sealed class All
{
    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on MySQL
    /// </summary>
    public static readonly DatabaseType[] DatabaseTypes = [
        DatabaseType.MySql
    ];

    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on MySQL
    /// with both permutations of true/false.  Matches exhaustively method signature (DatabaseType,bool)
    /// </summary>
    public static readonly object[] DatabaseTypesWithBoolFlags = [
        new object[] {DatabaseType.MySql,true},
        new object[] {DatabaseType.MySql,false}
    ];

    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on MySQL
    /// with all permutations of true/false for 2 args.  Matches exhaustively method signature (DatabaseType,bool,bool)
    /// </summary>
    public static readonly object[] DatabaseTypesWithTwoBoolFlags = [
        new object[] {DatabaseType.MySql,true,true},
        new object[] {DatabaseType.MySql,true,false},
        new object[] {DatabaseType.MySql,false,true},
        new object[] {DatabaseType.MySql,false,false}
    ];
}
