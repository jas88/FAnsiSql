using System;
using System.Collections.Generic;
using System.Linq;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Update;

namespace FAnsi.Implementations.Sqlite.Update;

public sealed class SqliteUpdateHelper : UpdateHelper
{
    public static readonly SqliteUpdateHelper Instance = new();
    private SqliteUpdateHelper() { }

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