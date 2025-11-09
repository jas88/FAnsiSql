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
            string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert >= QueryComponent.SELECT && c.LocationToInsert <= QueryComponent.Having)),
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

            //the FROM, WHERE, and HAVING clauses for proper aggregation (ORDER BY is handled by GetPivotPart1)
            string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert >= QueryComponent.FROM && c.LocationToInsert <= QueryComponent.Having).Select(x => query.SyntaxHelper.Escape(x.Text)))
        );
    }

    protected override string BuildPivotOnlyAggregate(AggregateCustomLineCollection query, CustomLine nonPivotColumn)
    {
        var part1 = GetPivotPart1(query);

        var joinAlias = nonPivotColumn.GetAliasFromText(query.SyntaxHelper);

        // Get HAVING clause - properly formatted
        var havingLines = query.Lines.Where(static c => c.LocationToInsert == QueryComponent.Having).ToList();
        var havingSqlIfAny = havingLines.Any() ?
            query.SyntaxHelper.Escape(string.Join(Environment.NewLine, havingLines.Select(l => l.Text))) : "";

        // Get ORDER BY clause for the final query (if specified), excluding TopX ORDER BY lines
        var orderByLines = query.Lines.Where(static c => c.LocationToInsert == QueryComponent.OrderBy && c.Role != CustomLineRole.TopX).ToList();
        var finalOrderBySql = orderByLines.Any() ?
            query.SyntaxHelper.Escape(string.Join(Environment.NewLine, orderByLines.Select(l => l.Text))) : joinAlias;

        // Get LIMIT clause from TopX postfix - this should NOT be used in the final query since it's handled in the CTE
        var topXLimitLine = query.Lines.SingleOrDefault(static c =>
            c.LocationToInsert == QueryComponent.Postfix && c.Role == CustomLineRole.TopX);

        // Note: For MySQL, LIMIT is handled in the CTE (GetPivotPart1), not in the final query
        // This is because we need to limit the number of pivot columns generated dynamically

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
                             {5}
                             ORDER BY
                             {6}
                             ');

                             PREPARE stmt FROM @sql;
                             EXECUTE stmt;
                             DEALLOCATE PREPARE stmt;
                             """,
            string.Join(Environment.NewLine, query.Lines.Where(static l => l.LocationToInsert < QueryComponent.SELECT)),
            part1,
            nonPivotColumn,

            //everything inclusive of FROM but stopping before GROUP BY
            query.SyntaxHelper.Escape(string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert >= QueryComponent.FROM && c.LocationToInsert < QueryComponent.GroupBy))),

            joinAlias,
            havingSqlIfAny,     // HAVING comes after GROUP BY if present
            finalOrderBySql    // ORDER BY comes after HAVING
        );
    }

    /// <summary>
    /// Returns the section of the PIVOT which identifies unique values.
    /// Uses a query-first approach with SELECT DISTINCT to get pivot values, then builds dynamic CASE statements.
    /// This replaces the complex CTE implementation with a simpler, more reliable approach.
    ///
    /// IMPORTANT: When using TOP X (LIMIT), the LIMIT clause is applied to the SELECT DISTINCT subquery
    /// to limit the number of pivot columns generated dynamically.
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

        // Work out how to order the pivot columns - this should NOT include LIMIT
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

        // Build the FROM, WHERE, and HAVING clauses for the subquery
        var fromClauses = string.Join(Environment.NewLine,
            query.Lines.Where(static l =>
                l.LocationToInsert >= QueryComponent.FROM && l.LocationToInsert <= QueryComponent.WHERE &&
                l.Role != CustomLineRole.Axis));

        // Add WHERE clause for axis if present
        if (query.AxisSelect != null)
        {
            var axisColumnWithoutAlias = query.AxisSelect.GetTextWithoutAlias(query.SyntaxHelper);
            var axisWhereClause = query.Lines.Any(static l => l.LocationToInsert == QueryComponent.WHERE) ?
                $"AND {axisColumnWithoutAlias} IS NOT NULL" :
                $"WHERE {axisColumnWithoutAlias} IS NOT NULL";
            fromClauses += Environment.NewLine + axisWhereClause;
        }

        // Determine if we need aggregation (HAVING clause or aggregate functions in ORDER BY)
        // MySQL's ONLY_FULL_GROUP_BY mode requires GROUP BY when using aggregates in HAVING or ORDER BY
        var needsAggregation = !string.IsNullOrEmpty(havingSqlIfAny) ||
                               orderBy.Contains("count(", StringComparison.OrdinalIgnoreCase) ||
                               orderBy.Contains("sum(", StringComparison.OrdinalIgnoreCase) ||
                               orderBy.Contains("avg(", StringComparison.OrdinalIgnoreCase) ||
                               orderBy.Contains("max(", StringComparison.OrdinalIgnoreCase) ||
                               orderBy.Contains("min(", StringComparison.OrdinalIgnoreCase);

        // Build the complete subquery to get pivot values
        // Use GROUP BY when aggregation is needed, otherwise use SELECT DISTINCT
        var pivotValuesSubquery = needsAggregation
            ? $"""
            SELECT {pivotSqlWithoutAlias} as piv, {countSqlWithoutAlias} as agg_count
            {fromClauses}
            GROUP BY {pivotSqlWithoutAlias}
            {(!string.IsNullOrEmpty(havingSqlIfAny) ? havingSqlIfAny : "")}
            ORDER BY {orderBy}
            {topXLimitSqlIfAny}
            """
            : $"""
            SELECT DISTINCT {pivotSqlWithoutAlias} as piv
            {fromClauses}
            ORDER BY {pivotSqlWithoutAlias}
            {topXLimitSqlIfAny}
            """;

        return string.Format("""

                             /* Query-first approach: Get distinct pivot values and build column lists */
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
                             FROM ({3}) AS distinctPivotValues;

                             """,
            aggregateMethod,
            pivotSqlWithoutAlias,
            aggregateParameter,
            pivotValuesSubquery
        );
    }

    protected override IQuerySyntaxHelper GetQuerySyntaxHelper() => MySqlQuerySyntaxHelper.Instance;
}
