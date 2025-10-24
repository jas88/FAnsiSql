using FAnsi;
using NUnit.Framework;

namespace FAnsiTests;

/// <summary>
/// Microsoft SQL Server specific tests
/// </summary>
public sealed class All
{
    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on SQL Server
    /// </summary>
    public static readonly DatabaseType[] DatabaseTypes = [
        DatabaseType.MicrosoftSQLServer
    ];

    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on SQL Server
    /// with both permutations of true/false.  Matches exhaustively method signature (DatabaseType,bool)
    /// </summary>
    public static readonly object[] DatabaseTypesWithBoolFlags = [
        new object[] {DatabaseType.MicrosoftSQLServer,true},
        new object[] {DatabaseType.MicrosoftSQLServer,false}
    ];

    /// <summary>
    /// <see cref="TestCaseSourceAttribute"/> for tests that should run on SQL Server
    /// with all permutations of true/false for 2 args.  Matches exhaustively method signature (DatabaseType,bool,bool)
    /// </summary>
    public static readonly object[] DatabaseTypesWithTwoBoolFlags = [
        new object[] {DatabaseType.MicrosoftSQLServer,true,true},
        new object[] {DatabaseType.MicrosoftSQLServer,true,false},
        new object[] {DatabaseType.MicrosoftSQLServer,false,true},
        new object[] {DatabaseType.MicrosoftSQLServer,false,false}
    ];
}
