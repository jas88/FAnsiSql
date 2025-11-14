using System;
using System.Globalization;
using System.Linq;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;

namespace FAnsi.Implementations.PostgreSql.Aggregation;

public sealed class PostgreSqlAggregateHelper : AggregateHelper
{
    public static readonly PostgreSqlAggregateHelper Instance = new();
    private PostgreSqlAggregateHelper() { }
    protected override IQuerySyntaxHelper GetQuerySyntaxHelper() => PostgreSqlSyntaxHelper.Instance;

    protected override string BuildAxisAggregate(AggregateCustomLineCollection query)
    {
        var interval = query.Axis?.AxisIncrement switch
        {
            AxisIncrement.Day => "1 day",
            AxisIncrement.Month => "1 month",
            AxisIncrement.Year => "1 year",
            AxisIncrement.Quarter => "3 months",
            _ => throw new ArgumentOutOfRangeException(nameof(query), $"Invalid AxisIncrement {query.Axis?.AxisIncrement}")
        };

        var countAlias = query.CountSelect!.GetAliasFromText(query.SyntaxHelper);
        var axisColumnAlias = query.AxisSelect!.GetAliasFromText(query.SyntaxHelper) ?? "joinDt";

        WrapAxisColumnWithDatePartFunction(query, axisColumnAlias);

        var sql =
            string.Format(CultureInfo.InvariantCulture, """

                          {0}
                          SELECT
                             {1} AS "joinDt",dataset.{6}
                          FROM
                          generate_series({3},
                                       {4},
                                      interval '{5}')
                          LEFT JOIN
                          (
                              {2}
                          ) dataset
                          ON dataset.{7} = {1}
                          ORDER BY
                          {1}

                          """,
                //Anything before the SELECT
                string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert < QueryComponent.SELECT)),
                GetDatePartOfColumn(query.Axis.AxisIncrement, "generate_series.date"),
                //the entire query
                string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert is >= QueryComponent.SELECT and <= QueryComponent.Having)), query.Axis.StartDate,
                query.Axis.EndDate,
                interval,
                countAlias,
                axisColumnAlias);

        return sql;
    }

    protected override string BuildPivotOnlyAggregate(AggregateCustomLineCollection query, CustomLine nonPivotColumn)
    {
        var pivotSqlWithoutAlias = query.PivotSelect!.GetTextWithoutAlias(query.SyntaxHelper);
        var countSqlWithoutAlias = query.CountSelect!.GetTextWithoutAlias(query.SyntaxHelper);

        query.SyntaxHelper.SplitLineIntoOuterMostMethodAndContents(countSqlWithoutAlias, out var aggregateMethod,
            out var aggregateParameter);

        if (aggregateParameter.Equals("*", StringComparison.Ordinal))
            aggregateParameter = "1";

        var nonPivotColumnAlias = nonPivotColumn.GetAliasFromText(query.SyntaxHelper);
        if (string.IsNullOrWhiteSpace(nonPivotColumnAlias))
            nonPivotColumnAlias = query.SyntaxHelper.GetRuntimeName(nonPivotColumn.GetTextWithoutAlias(query.SyntaxHelper));

        var havingSqlIfAny = string.Join(Environment.NewLine,
            query.Lines.Where(static l => l.LocationToInsert == QueryComponent.Having).Select(static l => l.Text));

        // PostgreSQL doesn't have native PIVOT, so we use aggregation with FILTER or CASE statements
        // Similar to MySQL approach but using PostgreSQL syntax
        return string.Format(CultureInfo.InvariantCulture, """

                             /* Get distinct pivot values */
                             WITH pivotValues AS (
                                 SELECT DISTINCT {0} as piv
                                 {2}
                                 WHERE {0} IS NOT NULL
                                 ORDER BY {0}
                             ),
                             /* Build the aggregated dataset */
                             dataset AS (
                                 {3}
                             )
                             /* Main query with dynamic columns */
                             SELECT
                                 dataset.{1},
                                 {4}
                             FROM dataset
                             CROSS JOIN pivotValues
                             GROUP BY dataset.{1}
                             ORDER BY dataset.{1}
                             {5}

                             """,
            pivotSqlWithoutAlias,
            nonPivotColumnAlias,
            string.Join(Environment.NewLine,
                query.Lines.Where(static l =>
                    l.LocationToInsert is >= QueryComponent.FROM and <= QueryComponent.WHERE)),
            string.Join(Environment.NewLine,
                query.Lines.Where(static c => c.LocationToInsert is >= QueryComponent.SELECT and < QueryComponent.GroupBy)),
            $"{aggregateMethod}({aggregateParameter}) FILTER (WHERE {pivotSqlWithoutAlias} = pivotValues.piv)",
            havingSqlIfAny
        );
    }

    protected override string BuildPivotAndAxisAggregate(AggregateCustomLineCollection query)
    {
        var interval = query.Axis?.AxisIncrement switch
        {
            AxisIncrement.Day => "1 day",
            AxisIncrement.Month => "1 month",
            AxisIncrement.Year => "1 year",
            AxisIncrement.Quarter => "3 months",
            _ => throw new ArgumentOutOfRangeException(nameof(query), $"Invalid AxisIncrement {query.Axis?.AxisIncrement}")
        };

        var pivotSqlWithoutAlias = query.PivotSelect!.GetTextWithoutAlias(query.SyntaxHelper);
        var countSqlWithoutAlias = query.CountSelect!.GetTextWithoutAlias(query.SyntaxHelper);
        var axisColumnAlias = query.AxisSelect!.GetAliasFromText(query.SyntaxHelper) ?? "joinDt";

        query.SyntaxHelper.SplitLineIntoOuterMostMethodAndContents(countSqlWithoutAlias, out var aggregateMethod,
            out var aggregateParameter);

        if (aggregateParameter.Equals("*", StringComparison.Ordinal))
            aggregateParameter = "1";

        WrapAxisColumnWithDatePartFunction(query, axisColumnAlias);

        // PostgreSQL approach: Use crosstab from tablefunc extension or manual CASE statements
        // For simplicity, using FILTER clause with aggregation
        return string.Format(CultureInfo.InvariantCulture, """

                             {0}
                             /* Get distinct pivot values */
                             WITH pivotValues AS (
                                 SELECT DISTINCT {1} as piv
                                 {2}
                                 WHERE {1} IS NOT NULL
                                 ORDER BY {1}
                             ),
                             /* Build the dataset with date axis */
                             dataset AS (
                                 {3}
                             )
                             /* Main query joining date axis with pivoted data */
                             SELECT
                                 {4} AS "joinDt",
                                 {5}
                             FROM generate_series({6}, {7}, interval '{8}') gs(date)
                             CROSS JOIN pivotValues
                             LEFT JOIN dataset ON dataset.{9} = {4}
                             GROUP BY {4}, pivotValues.piv
                             ORDER BY {4}

                             """,
            string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert < QueryComponent.SELECT)),
            pivotSqlWithoutAlias,
            string.Join(Environment.NewLine,
                query.Lines.Where(static l =>
                    l.LocationToInsert is >= QueryComponent.FROM and <= QueryComponent.WHERE &&
                    l.Role != CustomLineRole.Axis)),
            string.Join(Environment.NewLine,
                query.Lines.Where(static c => c.LocationToInsert is >= QueryComponent.SELECT and <= QueryComponent.Having)),
            GetDatePartOfColumn(query.Axis.AxisIncrement, "gs.date"),
            $"{aggregateMethod}({aggregateParameter}) FILTER (WHERE {pivotSqlWithoutAlias} = pivotValues.piv)",
            query.Axis.StartDate,
            query.Axis.EndDate,
            interval,
            axisColumnAlias
        );
    }

    public override string GetDatePartOfColumn(AxisIncrement increment, string columnSql) =>
        increment switch
        {
            AxisIncrement.Day => $"{columnSql}::date",
            AxisIncrement.Month => $"to_char({columnSql},'YYYY-MM')",
            AxisIncrement.Year => $"date_part('year', {columnSql})",
            AxisIncrement.Quarter => $"to_char({columnSql},'YYYY\"Q\"Q')",
            _ => throw new ArgumentOutOfRangeException(nameof(increment), increment, null)
        };
}
