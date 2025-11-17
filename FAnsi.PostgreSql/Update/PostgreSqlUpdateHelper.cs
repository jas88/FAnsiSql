using System;
using System.Collections.Generic;
using System.Linq;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Update;

namespace FAnsi.Implementations.PostgreSql.Update;

/// <summary>
/// PostgreSQL-specific implementation of update query helper. Provides functionality for building
/// UPDATE statements with JOIN conditions using PostgreSQL's UPDATE...FROM syntax.
/// </summary>
/// <remarks>
/// PostgreSQL uses UPDATE...FROM syntax for multi-table updates:
/// <code>
/// UPDATE table1 AS t1
/// SET column = value
/// FROM table2 AS t2
/// WHERE t1.id = t2.id
/// </code>
/// Note: The SET clause cannot have table qualifiers on the LHS (column name being set),
/// but the RHS (expression) can reference t1 or t2 for self-joins and complex expressions.
/// </remarks>
public sealed class PostgreSqlUpdateHelper : UpdateHelper
{
    /// <summary>
    /// Gets the singleton instance of <see cref="PostgreSqlUpdateHelper"/>.
    /// </summary>
    public static readonly PostgreSqlUpdateHelper Instance = new();

    private PostgreSqlUpdateHelper() { }

    /// <summary>
    /// Builds an UPDATE query that updates one table based on values from another table.
    /// </summary>
    /// <param name="table1">The table to update</param>
    /// <param name="table2">The table to join with (can be the same as table1 for self-joins)</param>
    /// <param name="lines">The custom lines containing JOIN, SET, and WHERE clauses</param>
    /// <returns>SQL UPDATE statement using PostgreSQL's UPDATE...FROM syntax</returns>
    /// <remarks>
    /// <para>PostgreSQL requires the table being updated to be aliased as t1 in the UPDATE clause.</para>
    /// <para>The SET clause LHS must be unqualified, but the RHS can reference t1 or t2.</para>
    /// <para>For self-joins, both t1 and t2 can reference columns from the same table.</para>
    /// </remarks>
    protected override string BuildUpdateImpl(DiscoveredTable table1, DiscoveredTable table2, List<CustomLine> lines)
    {
        // PostgreSQL UPDATE...FROM syntax reference: https://stackoverflow.com/a/7869611
        // UPDATE table1 AS t1 SET column = expr FROM table2 AS t2 WHERE conditions

        // Extract join conditions (these become part of WHERE clause in PostgreSQL)
        var joinConditions = lines.Where(l => l.LocationToInsert == QueryComponent.JoinInfoJoin)
            .Select(c => c.Text)
            .ToList();

        // For SET statements, handle LHS and RHS differently:
        // LHS (column name): must be unqualified in PostgreSQL
        // RHS (expression): keep t1. and t2. references for self-joins and complex expressions
        var setStatements = lines.Where(l => l.LocationToInsert == QueryComponent.SET)
            .Select(c =>
            {
                var parts = c.Text.Split(['='], 2);
                if (parts.Length != 2) return c.Text;

                // Strip table qualifier from LHS (the column being set)
                var lhs = parts[0].Trim().Replace("t1.", "");

                // Keep RHS as-is - it may reference t1 or t2 for self-joins
                var rhs = parts[1].Trim();

                return $"{lhs} = {rhs}";
            })
            .ToList();

        // Extract additional WHERE conditions
        var whereConditions = lines.Where(l => l.LocationToInsert == QueryComponent.WHERE)
            .Select(c => c.Text)
            .ToList();

        // Combine join and where conditions for the WHERE clause
        // Wrap where conditions in parentheses to preserve operator precedence
        var allConditions = joinConditions
            .Concat(whereConditions.Select(w => $"({w})"))
            .ToList();

        var whereClause = allConditions.Count != 0
            ? $"{Environment.NewLine}WHERE {string.Join(" AND ", allConditions)}"
            : string.Empty;

        return $"""
               UPDATE {table1.GetFullyQualifiedName()} AS t1
               SET
                   {string.Join($", {Environment.NewLine}", setStatements)}
               FROM {table2.GetFullyQualifiedName()} AS t2{whereClause}
               """;
    }
}
