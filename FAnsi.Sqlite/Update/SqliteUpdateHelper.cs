using System;
using System.Collections.Generic;
using System.Linq;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Update;

namespace FAnsi.Implementations.Sqlite.Update;

/// <summary>
/// SQLite-specific implementation of update query helper. Provides functionality for building
/// UPDATE statements with JOIN conditions.
/// </summary>
/// <remarks>
/// SQLite doesn't support multi-table UPDATE with JOIN syntax like MySQL.
/// This implementation uses subqueries with EXISTS for cross-table updates.
/// </remarks>
public sealed class SqliteUpdateHelper : UpdateHelper
{
    /// <summary>
    /// Gets the singleton instance of <see cref="SqliteUpdateHelper"/>.
    /// </summary>
    public static readonly SqliteUpdateHelper Instance = new();

    private SqliteUpdateHelper() { }

    /// <summary>
    /// Builds an UPDATE query that updates one table based on values from another table.
    /// </summary>
    /// <param name="table1">The table to update</param>
    /// <param name="table2">The table to join with</param>
    /// <param name="lines">The custom lines containing JOIN, SET, and WHERE clauses</param>
    /// <returns>SQL UPDATE statement using EXISTS subquery</returns>
    /// <remarks>
    /// <para>SQLite doesn't support UPDATE...FROM or UPDATE...JOIN syntax.</para>
    /// <para>This implementation uses:</para>
    /// <code>
    /// UPDATE table1
    /// SET column = value
    /// WHERE EXISTS (SELECT 1 FROM table2 WHERE join_conditions)
    /// </code>
    /// </remarks>
    protected override string BuildUpdateImpl(DiscoveredTable table1, DiscoveredTable table2, List<CustomLine> lines)
    {
        // SQLite doesn't support multi-table UPDATE with JOIN syntax like MySQL
        // We'll need to use a more compatible syntax with subqueries
        var joinConditions = lines.Where(l => l.LocationToInsert == QueryComponent.JoinInfoJoin).Select(c => c.Text);
        var setStatements = lines.Where(l => l.LocationToInsert == QueryComponent.SET).Select(c => c.Text);
        var whereConditions = lines.Where(l => l.LocationToInsert == QueryComponent.WHERE).Select(c => c.Text);

        return $"""
               UPDATE {table1.GetFullyQualifiedName()}
               SET
                   {string.Join($", {Environment.NewLine}", setStatements)}
               WHERE EXISTS (
                   SELECT 1 FROM {table2.GetFullyQualifiedName()} t2
                   WHERE {string.Join(" AND ", joinConditions.Select(c => c.Replace("t1.", $"{table1.GetFullyQualifiedName()}.").Replace("t2.", "t2.")))}
                   {(whereConditions.Any() ? $"AND {string.Join(" AND ", whereConditions)}" : "")}
               )
               """;
    }
}
