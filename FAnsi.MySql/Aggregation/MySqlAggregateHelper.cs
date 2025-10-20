using System;
using System.Linq;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;

namespace FAnsi.Implementations.MySql.Aggregation;

public sealed class MySqlAggregateHelper : AggregateHelper
{
    public static readonly MySqlAggregateHelper Instance = new();
    private MySqlAggregateHelper() { }
    /// <summary>
    /// Generates a date axis CTE using MySQL 8.0+ recursive CTEs.
    /// Much simpler and more efficient than the legacy cross-join approach.
    /// </summary>
    private static string GetDateAxisTableDeclaration(IQueryAxis axis)
    {
        var intervalUnit = axis.AxisIncrement switch
        {
            AxisIncrement.Day => "DAY",
            AxisIncrement.Month => "MONTH",
            AxisIncrement.Year => "YEAR",
            AxisIncrement.Quarter => "QUARTER",
            _ => throw new ArgumentOutOfRangeException(nameof(axis))
        };

        return $"""
                WITH RECURSIVE dateAxis AS (
                    SELECT {axis.StartDate} AS dt
                    UNION ALL
                    SELECT DATE_ADD(dt, INTERVAL 1 {intervalUnit})
                    FROM dateAxis
                    WHERE dt < {axis.EndDate}
                )
                """;
    }

    public override string GetDatePartOfColumn(AxisIncrement increment, string columnSql)
    {
        return increment switch
        {
            AxisIncrement.Day => $"DATE({columnSql})",
            AxisIncrement.Month => $"DATE_FORMAT({columnSql},'%Y-%m')",
            AxisIncrement.Year => $"YEAR({columnSql})",
            AxisIncrement.Quarter => $"CONCAT(YEAR({columnSql}),'Q',QUARTER({columnSql}))",
            _ => throw new ArgumentOutOfRangeException(nameof(increment))
        };
    }


    protected override IQuerySyntaxHelper GetQuerySyntaxHelper() => MySqlQuerySyntaxHelper.Instance;

    protected override string BuildAxisAggregate(AggregateCustomLineCollection query)
    {
        var countAlias = query.CountSelect.GetAliasFromText(query.SyntaxHelper);
        var axisColumnAlias = query.AxisSelect.GetAliasFromText(query.SyntaxHelper) ?? "joinDt";

        WrapAxisColumnWithDatePartFunction(query, axisColumnAlias);


        return string.Format(
            """

            {0}
            {1}

            SELECT
            {2} AS joinDt,dataset.{3}
            FROM
            dateAxis
            LEFT JOIN
            (
                {4}
            ) dataset
            ON dataset.{5} = {2}
            ORDER BY
            {2}

            """
            ,
            string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert < QueryComponent.SELECT)),
            GetDateAxisTableDeclaration(query.Axis),

            GetDatePartOfColumn(query.Axis.AxisIncrement, "dateAxis.dt"),
            countAlias,

            //the entire query
            string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert is >= QueryComponent.SELECT and <= QueryComponent.Having)),
            axisColumnAlias
        ).Trim();

    }

    protected override string BuildPivotAndAxisAggregate(AggregateCustomLineCollection query)
    {
        var axisColumnWithoutAlias = query.AxisSelect.GetTextWithoutAlias(query.SyntaxHelper);
        var part1 = GetPivotPart1(query, skipSessionSettings: true);

        return string.Format("""

                             SET SESSION cte_max_recursion_depth = 50000, group_concat_max_len = 1000000;

                             {0}

                             {1}

                             {2}

                             SET @sql =

                             CONCAT(
                             '
                             SELECT
                             {3} as joinDt,',@columnsSelectFromDataset,'
                             FROM
                             dateAxis
                             LEFT JOIN
                             (
                                 {4}
                                 {5} AS joinDt,
                             '
                                 ,@columnsSelectCases,
                             '
                             {6}
                             group by
                             {5}
                             ) dataset
                             ON {3} = dataset.joinDt
                             ORDER BY
                             {3}
                             ');

                             PREPARE stmt FROM @sql;
                             EXECUTE stmt;
                             DEALLOCATE PREPARE stmt;
                             """,
            string.Join(Environment.NewLine, query.Lines.Where(static l => l.LocationToInsert < QueryComponent.SELECT)),
            GetDateAxisTableDeclaration(query.Axis, skipSessionSettings: true),
            part1,
            query.SyntaxHelper.Escape(GetDatePartOfColumn(query.Axis.AxisIncrement, "dateAxis.dt")),
            string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert == QueryComponent.SELECT)),

            //the from including all table joins and where but no calendar table join
            query.SyntaxHelper.Escape(GetDatePartOfColumn(query.Axis.AxisIncrement, axisColumnWithoutAlias)),

            //the order by (should be count so that heavy populated columns come first)
            string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert is >= QueryComponent.FROM and <= QueryComponent.WHERE).Select(x => query.SyntaxHelper.Escape(x.Text)))
        );
    }

    protected override string BuildPivotOnlyAggregate(AggregateCustomLineCollection query, CustomLine nonPivotColumn)
    {
        var part1 = GetPivotPart1(query);

        var joinAlias = nonPivotColumn.GetAliasFromText(query.SyntaxHelper);

        return string.Format("""

                             {0}

                             {1}

                             SET @sql =

                             CONCAT(
                             '
                             SELECT
                             {2}',@columnsSelectCases,'

                             {3}
                             GROUP BY
                             {4}
                             ORDER BY
                             {4}
                             {5}
                             ');

                             PREPARE stmt FROM @sql;
                             EXECUTE stmt;
                             DEALLOCATE PREPARE stmt;
                             """,
            string.Join(Environment.NewLine, query.Lines.Where(static l => l.LocationToInsert < QueryComponent.SELECT)),
            part1,
            nonPivotColumn,

            //everything inclusive of FROM but stopping before GROUP BY
            query.SyntaxHelper.Escape(string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert is >= QueryComponent.FROM and < QueryComponent.GroupBy))),

            joinAlias,

            //any HAVING SQL
            query.SyntaxHelper.Escape(string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert == QueryComponent.Having)))
        );
    }

    /// <summary>
    /// Returns the section of the PIVOT which identifies unique values.
    /// For MySQL 8.0+ this uses a CTE instead of a temporary table and builds dynamic CASE statements.
    /// </summary>
    private static string GetPivotPart1(AggregateCustomLineCollection query, bool skipSessionSettings = false)
    {
        var pivotSqlWithoutAlias = query.PivotSelect.GetTextWithoutAlias(query.SyntaxHelper);
        var countSqlWithoutAlias = query.CountSelect.GetTextWithoutAlias(query.SyntaxHelper);

        query.SyntaxHelper.SplitLineIntoOuterMostMethodAndContents(countSqlWithoutAlias, out var aggregateMethod,
            out var aggregateParameter);

        if (aggregateParameter.Equals("*"))
            aggregateParameter = "1";

        // If there is an axis, only pull pivot values where the values appear in that axis range
        var whereDateColumnNotNull = "";
        if (query.AxisSelect != null)
        {
            var axisColumnWithoutAlias = query.AxisSelect.GetTextWithoutAlias(query.SyntaxHelper);
            whereDateColumnNotNull += query.Lines.Any(static l => l.LocationToInsert == QueryComponent.WHERE) ? "AND " : "WHERE ";
            whereDateColumnNotNull += $"{axisColumnWithoutAlias} IS NOT NULL";
        }

        // Work out how to order the pivot columns
        var orderBy = $"{countSqlWithoutAlias} desc";

        var topXOrderByLine =
            query.Lines.SingleOrDefault(static c => c.LocationToInsert == QueryComponent.OrderBy && c.Role == CustomLineRole.TopX);
        if (topXOrderByLine != null)
            orderBy = topXOrderByLine.Text;

        var topXLimitLine =
            query.Lines.SingleOrDefault(static c => c.LocationToInsert == QueryComponent.Postfix && c.Role == CustomLineRole.TopX);
        var topXLimitSqlIfAny = topXLimitLine != null ? topXLimitLine.Text : "";

        var havingSqlIfAny = string.Join(Environment.NewLine,
            query.Lines.Where(static l => l.LocationToInsert == QueryComponent.Having).Select(static l => l.Text));

        var sessionSettings = skipSessionSettings ? "" : "SET SESSION group_concat_max_len = 1000000;\n\n                             ";

        return string.Format("""

                             {8}/* Get unique pivot values and build both column lists in a single query */
                             WITH pivotValues AS (
                                 SELECT
                                 {1} as piv
                                 {3}
                                 {4}
                                 GROUP BY
                                 {1}
                                 {7}
                                 ORDER BY
                                 {6}
                                 {5}
                             )
                             SELECT
                               GROUP_CONCAT(
                                 CONCAT(
                                   '{0}(CASE WHEN {1} = ', QUOTE(piv), ' THEN {2} ELSE NULL END) AS `', piv,'`'
                                 ) ORDER BY piv
                               ),
                               GROUP_CONCAT(
                                 CONCAT('dataset.`', piv,'`') ORDER BY piv
                               )
                             INTO @columnsSelectCases, @columnsSelectFromDataset
                             FROM pivotValues;

                             """,
            aggregateMethod,
            pivotSqlWithoutAlias,
            aggregateParameter,
            string.Join(Environment.NewLine,
                query.Lines.Where(static l =>
                    l.LocationToInsert is >= QueryComponent.FROM and <= QueryComponent.WHERE &&
                    l.Role != CustomLineRole.Axis)),
            whereDateColumnNotNull,
            topXLimitSqlIfAny,
            orderBy,
            havingSqlIfAny,
            sessionSettings
        );
    }


}