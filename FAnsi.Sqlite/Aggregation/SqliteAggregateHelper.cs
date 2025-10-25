using System;
using System.Linq;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;

namespace FAnsi.Implementations.Sqlite.Aggregation;

/// <summary>
/// SQLite-specific implementation of aggregate query helper. Provides functionality for building
/// complex aggregate queries including date-based grouping and axis-based aggregations.
/// </summary>
/// <remarks>
/// <para>SQLite uses strftime() for date manipulation and doesn't have built-in PIVOT support.</para>
/// <para>Complex pivot operations require manual CASE statement construction.</para>
/// </remarks>
public sealed class SqliteAggregateHelper : AggregateHelper
{
    /// <summary>
    /// Gets the singleton instance of <see cref="SqliteAggregateHelper"/>.
    /// </summary>
    public static readonly SqliteAggregateHelper Instance = new();

    private SqliteAggregateHelper() { }

    /// <summary>
    /// Gets the SQL expression to extract a specific date part from a column for grouping.
    /// </summary>
    /// <param name="increment">The date increment (day, month, year, quarter)</param>
    /// <param name="columnSql">The column SQL expression</param>
    /// <returns>SQLite date function expression using strftime()</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if increment is not supported</exception>
    /// <remarks>
    /// <para>SQLite date part mappings:</para>
    /// <list type="bullet">
    /// <item><description>Day: DATE(column)</description></item>
    /// <item><description>Month: strftime('%Y-%m', column)</description></item>
    /// <item><description>Year: strftime('%Y', column)</description></item>
    /// <item><description>Quarter: Calculated from month using expression</description></item>
    /// </list>
    /// </remarks>
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

    /// <inheritdoc />
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

    /// <summary>
    /// Builds a query with both pivot and axis aggregation.
    /// </summary>
    /// <param name="query">The aggregate query collection</param>
    /// <returns>Never returns (always throws)</returns>
    /// <exception cref="NotSupportedException">
    /// SQLite does not have built-in PIVOT functionality. Manual CASE statement construction would be required.
    /// </exception>
    /// <remarks>
    /// For pivot-like behavior in SQLite, use GROUP BY with CASE WHEN statements to create columns dynamically.
    /// </remarks>
    protected override string BuildPivotAndAxisAggregate(AggregateCustomLineCollection query)
    {
        // SQLite doesn't have built-in pivot functionality like SQL Server
        // We would need to build this manually with CASE statements
        throw new NotSupportedException("SQLite does not support complex pivot operations with axis aggregates. Consider using simpler aggregation methods.");
    }

    /// <summary>
    /// Builds a pivot-only aggregate query.
    /// </summary>
    /// <param name="query">The aggregate query collection</param>
    /// <param name="nonPivotColumn">The non-pivot column</param>
    /// <returns>Never returns (always throws)</returns>
    /// <exception cref="NotSupportedException">
    /// SQLite does not have built-in PIVOT functionality. Use GROUP BY with CASE statements instead.
    /// </exception>
    protected override string BuildPivotOnlyAggregate(AggregateCustomLineCollection query, CustomLine nonPivotColumn)
    {
        // SQLite doesn't have built-in pivot functionality like SQL Server
        // We would need to build this manually with CASE statements
        throw new NotSupportedException("SQLite does not support complex pivot operations. Consider using GROUP BY with CASE statements for simple pivoting.");
    }
}
