using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Update;

namespace FAnsi.Implementations.Sqlite.Update;

/// <summary>
///     SQLite-specific implementation of update query helper. Provides functionality for building
///     UPDATE statements with JOIN conditions.
/// </summary>
/// <remarks>
///     SQLite supports UPDATE...FROM syntax for multi-table updates since version 3.33.0 (2020-08-14).
///     This implementation uses the UPDATE...FROM pattern for cross-table updates.
/// </remarks>
public sealed class SqliteUpdateHelper : UpdateHelper
{
    /// <summary>
    ///     Gets the singleton instance of <see cref="SqliteUpdateHelper" />.
    /// </summary>
    public static readonly SqliteUpdateHelper Instance = new();

    private SqliteUpdateHelper()
    {
    }

    /// <summary>
    ///     Builds an UPDATE query that updates one table based on values from another table.
    /// </summary>
    /// <param name="table1">The table to update</param>
    /// <param name="table2">The table to join with</param>
    /// <param name="lines">The custom lines containing JOIN, SET, and WHERE clauses</param>
    /// <returns>SQL UPDATE statement using UPDATE...FROM syntax</returns>
    /// <remarks>
    ///     <para>SQLite supports UPDATE...FROM syntax since version 3.33.0 (2020-08-14).</para>
    ///     <para>This implementation uses:</para>
    ///     <code>
    /// UPDATE table1
    /// SET column = value
    /// FROM table2 AS t2
    /// WHERE join_conditions AND where_conditions
    /// </code>
    /// </remarks>
    protected override string BuildUpdateImpl(DiscoveredTable table1, DiscoveredTable table2, List<CustomLine> lines)
    {
        // SQLite supports UPDATE...FROM syntax since version 3.33.0 (2020-08-14)
        // Syntax: UPDATE table1 SET col = expr FROM table2 AS t2 WHERE conditions
        // Note: table1 is NOT aliased, but table2 can be aliased as t2

        var table1Name = table1.GetFullyQualifiedName();
        var joinConditions = lines.Where(l => l.LocationToInsert == QueryComponent.JoinInfoJoin)
            .Select(c => c.Text.Replace("t1.", $"{table1Name}."))
            .ToList();

        // For SET statements, handle LHS and RHS differently to support self-joins:
        // LHS (column name): must be unqualified in SQLite
        // RHS (expression): replace t1. with fully qualified name to avoid ambiguity
        var setStatements = lines.Where(l => l.LocationToInsert == QueryComponent.SET)
            .Select(c =>
            {
                var parts = c.Text.Split(['='], 2);
                if (parts.Length != 2) return c.Text;

                var lhs = parts[0].Trim().Replace("t1.", "");
                var rhs = parts[1].Trim().Replace("t1.", $"{table1Name}.");
                return $"{lhs} = {rhs}";
            });

        var whereConditions = lines.Where(l => l.LocationToInsert == QueryComponent.WHERE)
            .Select(c => c.Text.Replace("t1.", $"{table1Name}."))
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
                UPDATE {table1Name}
                SET
                    {string.Join($", {Environment.NewLine}", setStatements)}
                FROM {table2.GetFullyQualifiedName()} AS t2{whereClause}
                """;
    }
}
