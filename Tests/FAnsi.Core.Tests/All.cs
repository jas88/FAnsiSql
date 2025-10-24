using FAnsi;
using NUnit.Framework;

namespace FAnsiTests;

/// <summary>
/// Core tests - DBMS-agnostic tests that don't require a database
/// </summary>
public sealed class All
{
    /// <summary>
    /// Empty array - core tests don't run against databases
    /// </summary>
    public static readonly DatabaseType[] DatabaseTypes = [];

    public static readonly object[] DatabaseTypesWithBoolFlags = [];

    public static readonly object[] DatabaseTypesWithTwoBoolFlags = [];
}
