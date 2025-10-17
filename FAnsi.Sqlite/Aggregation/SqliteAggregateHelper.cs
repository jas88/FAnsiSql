using System;
using System.Linq;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;

namespace FAnsi.Implementations.Sqlite.Aggregation;

public sealed class SqliteAggregateHelper : AggregateHelper
{
    public static readonly SqliteAggregateHelper Instance = new();
    private SqliteAggregateHelper() { }

    public override string GetDatePartOfColumn(AxisIncrement increment, string columnSql)
    {
        return increment switch
        {
            AxisIncrement.Day => $"DATE({columnSql})",
            AxisIncrement.Month => $"strftime('%Y-%m', {columnSql})",
            AxisIncrement.Year => $"strftime('%Y', {columnSql})",
            AxisIncrement.Quarter => $"strftime('%Y', {columnSql}) || 'Q' || ((strftime('%m', {columnSql}) - 1) / 3 + 1)",
            _ => throw new ArgumentOutOfRangeException(nameof(increment))
        };
    }

    protected override IQuerySyntaxHelper GetQuerySyntaxHelper() => SqliteQuerySyntaxHelper.Instance;

    protected override string BuildAxisAggregate(AggregateCustomLineCollection query)
    {
        var countAlias = query.CountSelect.GetAliasFromText(query.SyntaxHelper);
        var axisColumnAlias = query.AxisSelect.GetAliasFromText(query.SyntaxHelper) ?? "joinDt";

        WrapAxisColumnWithDatePartFunction(query, axisColumnAlias);

        // SQLite doesn't support complex pivoting like MySQL, so we'll use a simpler approach
        // Create a CTE (Common Table Expression) for the date range
        var startDate = query.Axis.StartDate;
        var endDate = query.Axis.EndDate;
        var increment = query.Axis.AxisIncrement;

        return $"""
               WITH RECURSIVE dateAxis AS (
                   SELECT DATE({startDate}) AS dt
                   UNION ALL
                   SELECT DATE(dt, '+1 {increment}')
                   FROM dateAxis
                   WHERE dt < DATE({endDate})
               )
               SELECT
               {GetDatePartOfColumn(increment, "dateAxis.dt")} AS joinDt,
               COALESCE(dataset.{countAlias}, 0) AS {countAlias}
               FROM dateAxis
               LEFT JOIN (
                   {string.Join(Environment.NewLine, query.Lines.Where(c => c.LocationToInsert is >= QueryComponent.SELECT and <= QueryComponent.Having).Select(l => l.Text))}
               ) dataset
               ON dataset.{axisColumnAlias} = {GetDatePartOfColumn(increment, "dateAxis.dt")}
               ORDER BY joinDt
               """;
    }

    protected override string BuildPivotAndAxisAggregate(AggregateCustomLineCollection query)
    {
        // SQLite doesn't have built-in pivot functionality like SQL Server
        // We would need to build this manually with CASE statements
        throw new NotSupportedException("SQLite does not support complex pivot operations with axis aggregates. Consider using simpler aggregation methods.");
    }

    protected override string BuildPivotOnlyAggregate(AggregateCustomLineCollection query, CustomLine nonPivotColumn)
    {
        // SQLite doesn't have built-in pivot functionality like SQL Server
        // We would need to build this manually with CASE statements
        throw new NotSupportedException("SQLite does not support complex pivot operations. Consider using GROUP BY with CASE statements for simple pivoting.");
    }
}