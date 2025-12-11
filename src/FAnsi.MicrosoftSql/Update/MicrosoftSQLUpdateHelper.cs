using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Update;

namespace FAnsi.Implementations.MicrosoftSQL.Update;

public sealed class MicrosoftSQLUpdateHelper : UpdateHelper
{
    protected override string BuildUpdateImpl(DiscoveredTable table1, DiscoveredTable table2, List<CustomLine> lines)
    {
        var whereConditions = lines.Where(static l => l.LocationToInsert == QueryComponent.WHERE)
            .Select(static c => c.Text).ToList();
        var whereClause = whereConditions.Count != 0
            ? $"{Environment.NewLine}WHERE {string.Join(" AND ", whereConditions)}"
            : string.Empty;

        return $"""
                UPDATE t1
                SET
                    {string.Join($", {Environment.NewLine}", lines.Where(static l => l.LocationToInsert == QueryComponent.SET).Select(static c => c.Text))}
                FROM {table1.GetFullyQualifiedName()} AS t1
                INNER JOIN {table2.GetFullyQualifiedName()} AS t2
                    ON {string.Join(" AND ", lines.Where(static l => l.LocationToInsert == QueryComponent.JoinInfoJoin).Select(static c => c.Text))}{whereClause}
                """;
    }
}
