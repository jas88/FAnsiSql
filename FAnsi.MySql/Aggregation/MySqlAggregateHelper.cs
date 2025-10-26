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
    /// Sets recursion depth to 50000 to support large date ranges (up to ~137 years of daily data).
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
                    WHERE DATE_ADD(dt, INTERVAL 1 {intervalUnit}) <= {axis.EndDate}
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
            _ => throw new ArgumentOutOfRangeException(nameof(increment), increment, null)
        };
    }

    protected override string BuildAxisAggregate(AggregateCustomLineCollection query)
    {
        //this code is a bit different from the MSSQL implementation because:
        //1. in MSSQL you declare a table variable and then INSERT the dateAxis values into it before running the final query (and using SET to join on a variable)
        //2. in MySql there is no table variable only a temporary table and you have to reference the temporary table directly and cannot use SET to join on it as a variable

        if (query.AxisSelect == null)
            throw new InvalidOperationException("BuildAxisAggregate requires AxisSelect to be non-null");
        if (query.CountSelect == null)
            throw new InvalidOperationException("BuildAxisAggregate requires CountSelect to be non-null");
        if (query.Axis == null)
            throw new InvalidOperationException("BuildAxisAggregate requires Axis to be non-null");

        var axisColumnAlias = query.AxisSelect.GetAliasFromText(query.SyntaxHelper);

        if (string.IsNullOrWhiteSpace(axisColumnAlias))
            axisColumnAlias = query.SyntaxHelper.GetRuntimeName(query.AxisSelect.GetTextWithoutAlias(query.SyntaxHelper));

        WrapAxisColumnWithDatePartFunction(query, axisColumnAlias);

        var countAlias = query.CountSelect.GetAliasFromText(query.SyntaxHelper);

        if (string.IsNullOrWhiteSpace(countAlias))
            countAlias = query.SyntaxHelper.GetRuntimeName(query.CountSelect.GetTextWithoutAlias(query.SyntaxHelper));

        var sql = string.Format("""

                                 {0}

                                 SET SESSION cte_max_recursion_depth = 50000;

                                 {1}
                                 SELECT
                                 {2} AS "joinDt",
                                 dataset.{3} AS "{3}"
                                 FROM
                                 dateAxis
                                 LEFT JOIN
                                 (
                                    {4}
                                 ) dataset
                                 ON dataset.{5} = {2}
                                 ORDER BY
                                 {2}

                                 """,
            string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert < QueryComponent.SELECT)),
            GetDateAxisTableDeclaration(query.Axis),

            GetDatePartOfColumn(query.Axis.AxisIncrement, "dateAxis.dt"),
            countAlias,

            //the entire query
            string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert is >= QueryComponent.SELECT and <= QueryComponent.Having)),
            axisColumnAlias
        ).Trim();

        return sql;
    }

    protected override string BuildPivotAndAxisAggregate(AggregateCustomLineCollection query)
    {
        if (query.AxisSelect == null)
            throw new InvalidOperationException("BuildPivotAndAxisAggregate requires AxisSelect to be non-null");
        if (query.Axis == null)
            throw new InvalidOperationException("BuildPivotAndAxisAggregate requires Axis to be non-null");

        var axisColumnWithoutAlias = query.AxisSelect.GetTextWithoutAlias(query.SyntaxHelper);
        var part1 = GetPivotPart1(query);

        // Get the dateAxis CTE to include in the dynamic SQL
        var dateAxisCte = GetDateAxisTableDeclaration(query.Axis);

        return string.Format("""

                             SET SESSION cte_max_recursion_depth = 50000, group_concat_max_len = 1000000;

                             {0}

                             {1}

                             SET @sql =

                             CONCAT(
                             '{2}
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
            part1,
            query.SyntaxHelper.Escape(dateAxisCte),
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
    ///
    /// IMPORTANT: When using TOP X (LIMIT), the LIMIT clause must be placed at the CTE level
    /// AFTER the GROUP BY and HAVING clauses, NOT inside the ROW_NUMBER() OVER() window function.
    /// MySQL does not allow LIMIT inside window function OVER() clauses (GitHub Issue #38).
    /// </summary>
    private static string GetPivotPart1(AggregateCustomLineCollection query)
    {
        if (query.PivotSelect == null)
            throw new InvalidOperationException("GetPivotPart1 requires PivotSelect to be non-null");
        if (query.CountSelect == null)
            throw new InvalidOperationException("GetPivotPart1 requires CountSelect to be non-null");

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

        return string.Format("""

                             /* Get unique pivot values and build both column lists in a single query */
                             WITH pivotValues AS (
                                 SELECT
                                 {1} as piv,
                                 ROW_NUMBER() OVER (ORDER BY {6}) as rn
                                 {3}
                                 {4}
                                 GROUP BY
                                 {1}
                                 {7}
                                 ORDER BY {6}
                                 {5}
                             )
                             SELECT
                               GROUP_CONCAT(
                                 CONCAT(
                                   '{0}(CASE WHEN {1} = ', QUOTE(piv), ' THEN {2} ELSE NULL END) AS `', piv,'`'
                                 ) ORDER BY rn
                               ),
                               GROUP_CONCAT(
                                 CONCAT('dataset.`', piv,'`') ORDER BY rn
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
            havingSqlIfAny
        );
    }

    protected override IQuerySyntaxHelper GetQuerySyntaxHelper() => MySqlQuerySyntaxHelper.Instance;
}
