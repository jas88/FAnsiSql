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
    /// <para>SQLite date part mappings consistent with other databases:</para>
    /// <list type="bullet">
    /// <item><description>Day: DATE(column) - strips time, returns TEXT, matches SQL Server/PostgreSQL/MySQL behavior</description></item>
    /// <item><description>Month: strftime('%Y-%m', column) - returns TEXT in YYYY-MM format</description></item>
    /// <item><description>Year: strftime('%Y', column) - returns TEXT to maintain type consistency for JOIN conditions</description></item>
    /// <item><description>Quarter: strftime('%Y', column) || 'Q' || ((strftime('%m', column) - 1) / 3 + 1) - returns TEXT</description></item>
    /// </list>
    /// <para>All return TEXT types to ensure JOIN condition compatibility in calendar aggregation queries (TEXT = TEXT).</para>
    /// </remarks>
    public override string GetDatePartOfColumn(AxisIncrement increment, string columnSql)
    {
        return increment switch
        {
            // Use DATE() to strip time like other databases (SQL Server Convert(date), PostgreSQL ::date, MySQL DATE())
            AxisIncrement.Day => $"DATE({columnSql})",
            AxisIncrement.Month => $"strftime('%Y-%m', {columnSql})",
            // Return TEXT (not INTEGER) to maintain type consistency for JOIN conditions in calendar aggregations
            AxisIncrement.Year => $"strftime('%Y', {columnSql})",
            AxisIncrement.Quarter => $"strftime('%Y', {columnSql}) || 'Q' || ((strftime('%m', {columnSql}) - 1) / 3 + 1)",
            _ => throw new ArgumentOutOfRangeException(nameof(increment))
        };
    }

    /// <inheritdoc />
    protected override IQuerySyntaxHelper GetQuerySyntaxHelper() => SqliteQuerySyntaxHelper.Instance;

    protected override string BuildAxisAggregate(AggregateCustomLineCollection query)
    {
        var countAlias = query.CountSelect!.GetAliasFromText(query.SyntaxHelper)!;
        var axisColumnAlias = query.AxisSelect!.GetAliasFromText(query.SyntaxHelper) ?? "joinDt";

        WrapAxisColumnWithDatePartFunction(query, axisColumnAlias);

        // SQLite doesn't support complex pivoting like MySQL, so we'll use a simpler approach
        // Create a CTE (Common Table Expression) for the date range
        var startDate = query.Axis!.StartDate;
        var endDate = query.Axis.EndDate;
        var increment = query.Axis.AxisIncrement;

        // Map AxisIncrement enum to SQLite-compatible date modifier strings
        // Note: Quarter uses "+3 months" directly (not "+1 3 months")
        var sqliteModifier = increment switch
        {
            AxisIncrement.Day => "+1 day",
            AxisIncrement.Month => "+1 month",
            AxisIncrement.Year => "+1 year",
            AxisIncrement.Quarter => "+3 months",  // SQLite workaround - no native quarter support
            _ => throw new ArgumentOutOfRangeException(nameof(increment), increment, "Unsupported AxisIncrement value for SQLite calendar aggregation")
        };

        // For the final SELECT, cast to appropriate types for expected data types, but keep TEXT in JOIN condition
        // Year: INTEGER, others: TEXT (which is what they already return)
        var selectExpression = increment == AxisIncrement.Year
            ? $"CAST({GetDatePartOfColumn(increment, "dateAxis.dt")} AS INTEGER)"
            : GetDatePartOfColumn(increment, "dateAxis.dt");

        return $"""
               WITH RECURSIVE dateAxis AS (
                   SELECT DATE({startDate}) AS dt
                   UNION ALL
                   SELECT DATE(dt, '{sqliteModifier}')
                   FROM dateAxis
                   WHERE dt < DATE({endDate})
               )
               SELECT
               {selectExpression} AS joinDt,
               dataset.{countAlias}
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
