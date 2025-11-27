using System;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace FAnsi.Discovery.Helpers;

/// <summary>
/// Helper utilities for high-performance string operations using ReadOnlySpan&lt;char&gt;
/// to reduce allocations and improve performance in critical paths.
/// </summary>
public static class StringComparisonHelper
{
    /// <summary>
    /// Performs a case-insensitive comparison using ReadOnlySpan for zero allocation.
    /// Equivalent to string.Equals(str1, str2, StringComparison.OrdinalIgnoreCase).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool EqualsOrdinalIgnoreCase(ReadOnlySpan<char> span1, ReadOnlySpan<char> span2)
    {
        return span1.Equals(span2, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Performs a case-insensitive comparison using ReadOnlySpan for zero allocation.
    /// Equivalent to string.Equals(str1, str2, StringComparison.InvariantCultureIgnoreCase).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool EqualsInvariantCultureIgnoreCase(ReadOnlySpan<char> span1, ReadOnlySpan<char> span2)
    {
        return span1.Equals(span2, StringComparison.InvariantCultureIgnoreCase);
    }

    /// <summary>
    /// Performs a case-insensitive comparison using ReadOnlySpan for zero allocation.
    /// Equivalent to string.Equals(str1, str2, StringComparison.CurrentCultureIgnoreCase).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool EqualsCurrentCultureIgnoreCase(ReadOnlySpan<char> span1, ReadOnlySpan<char> span2)
    {
        return span1.Equals(span2, StringComparison.CurrentCultureIgnoreCase);
    }

    /// <summary>
    /// Checks if a span contains another span using ordinal ignore case comparison.
    /// Equivalent to string.Contains(str, substring, StringComparison.OrdinalIgnoreCase).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ContainsOrdinalIgnoreCase(ReadOnlySpan<char> span, ReadOnlySpan<char> value)
    {
        return span.Contains(value, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Checks if a span contains another span using invariant culture ignore case comparison.
    /// Equivalent to string.Contains(str, substring, StringComparison.InvariantCultureIgnoreCase).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ContainsInvariantCultureIgnoreCase(ReadOnlySpan<char> span, ReadOnlySpan<char> value)
    {
        return span.Contains(value, StringComparison.InvariantCultureIgnoreCase);
    }

    /// <summary>
    /// Checks if a span contains another span using current culture ignore case comparison.
    /// Equivalent to string.Contains(str, substring, StringComparison.CurrentCultureIgnoreCase).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ContainsCurrentCultureIgnoreCase(ReadOnlySpan<char> span, ReadOnlySpan<char> value)
    {
        return span.Contains(value, StringComparison.CurrentCultureIgnoreCase);
    }

    /// <summary>
    /// Helper method for common column name comparison pattern used throughout the codebase.
    /// Performs case-insensitive comparison using CurrentCultureIgnoreCase (most common pattern).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ColumnNamesEqual(string columnName1, string columnName2)
    {
        return columnName1.AsSpan().Equals(columnName2.AsSpan(), StringComparison.CurrentCultureIgnoreCase);
    }

    /// <summary>
    /// Helper method for common database object name comparison pattern.
    /// Performs case-insensitive comparison using InvariantCultureIgnoreCase.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool DatabaseObjectNamesEqual(string objectName1, string objectName2)
    {
        return objectName1.AsSpan().Equals(objectName2.AsSpan(), StringComparison.InvariantCultureIgnoreCase);
    }

    /// <summary>
    /// Helper method for common SQL type comparison pattern.
    /// Performs case-insensitive comparison using OrdinalIgnoreCase.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool SqlTypesEqual(string sqlType1, string sqlType2)
    {
        return sqlType1.AsSpan().Equals(sqlType2.AsSpan(), StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Helper method for checking if SQL type contains a specific pattern.
    /// Uses OrdinalIgnoreCase for best performance in SQL type comparisons.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool SqlTypeContains(string sqlType, string pattern)
    {
        return sqlType.AsSpan().Contains(pattern.AsSpan(), StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Optimized implementation for the common GetRuntimeName().Equals() pattern.
    /// This is one of the most frequently called operations in the codebase.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool RuntimeNamesEqual(string runtimeName1, string runtimeName2, StringComparison comparisonType = StringComparison.CurrentCultureIgnoreCase)
    {
        return runtimeName1.AsSpan().Equals(runtimeName2.AsSpan(), comparisonType);
    }

    /// <summary>
    /// Optimized implementation for checking if a runtime name matches any of several candidates.
    /// Common in column/table discovery operations.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool RuntimeNameEqualsAny(string runtimeName, ReadOnlySpan<string> candidates, StringComparison comparisonType = StringComparison.CurrentCultureIgnoreCase)
    {
        var nameSpan = runtimeName.AsSpan();
        for (int i = 0; i < candidates.Length; i++)
        {
            if (nameSpan.Equals(candidates[i].AsSpan(), comparisonType))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Optimized implementation for checking if a runtime name contains a specific pattern.
    /// Used in type translation and discovery operations.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool RuntimeNameContains(string runtimeName, string pattern, StringComparison comparisonType = StringComparison.OrdinalIgnoreCase)
    {
        return runtimeName.AsSpan().Contains(pattern.AsSpan(), comparisonType);
    }
}
