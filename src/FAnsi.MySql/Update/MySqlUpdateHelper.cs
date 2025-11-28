using System;
using System.Collections.Generic;
using System.Linq;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Update;

namespace FAnsi.Implementations.MySql.Update;

public sealed class MySqlUpdateHelper : UpdateHelper
{
    public static readonly MySqlUpdateHelper Instance = new();
    private MySqlUpdateHelper() { }
    protected override string BuildUpdateImpl(DiscoveredTable table1, DiscoveredTable table2, List<CustomLine> lines)
    {
        var whereConditions = lines.Where(static l => l.LocationToInsert == QueryComponent.WHERE).Select(static c => c.Text).ToList();
        var whereClause = whereConditions.Count != 0
            ? $"{Environment.NewLine}WHERE {string.Join(" AND ", whereConditions)}"
            : string.Empty;

        return $"""
                UPDATE {table1.GetFullyQualifiedName()} t1
                JOIN {table2.GetFullyQualifiedName()} t2
                    ON {string.Join(" AND ", lines.Where(static l => l.LocationToInsert == QueryComponent.JoinInfoJoin).Select(static c => c.Text))}
                SET
                    {string.Join($", {Environment.NewLine}    ", lines.Where(static l => l.LocationToInsert == QueryComponent.SET).Select(static c => c.Text))}{whereClause}
                """;
    }
}
