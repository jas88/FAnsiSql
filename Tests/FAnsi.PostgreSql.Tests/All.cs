using FAnsi;
using NUnit.Framework;

namespace FAnsiTests;

/// <summary>
/// PostgreSQL specific tests
/// </summary>
public sealed class All
{
    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on PostgreSQL
    /// </summary>
    public static readonly DatabaseType[] DatabaseTypes = [
        DatabaseType.PostgreSql
    ];

    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on PostgreSQL
    /// with both permutations of true/false.  Matches exhaustively method signature (DatabaseType,bool)
    /// </summary>
    public static readonly object[] DatabaseTypesWithBoolFlags = [
        new object[] {DatabaseType.PostgreSql,true},
        new object[] {DatabaseType.PostgreSql,false}
    ];

    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on PostgreSQL
    /// with all permutations of true/false for 2 args.  Matches exhaustively method signature (DatabaseType,bool,bool)
    /// </summary>
    public static readonly object[] DatabaseTypesWithTwoBoolFlags = [
        new object[] {DatabaseType.PostgreSql,true,true},
        new object[] {DatabaseType.PostgreSql,true,false},
        new object[] {DatabaseType.PostgreSql,false,true},
        new object[] {DatabaseType.PostgreSql,false,false}
    ];
}
