using System.Globalization;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;

namespace FAnsi.Implementations.MySql.Aggregation;

public sealed class MySqlAggregateHelper : AggregateHelper
{
    public static readonly MySqlAggregateHelper Instance = new();

    /// <summary>
    ///     The most characters in a MySQL column name, which a pivot column's name is cut to
    /// </summary>
    internal const int MaxNameLength = 64;

    /// <summary>
    ///     The assignments for a <c>SET SESSION</c> statement that let the dateAxis CTE recurse 50000 times, enough for
    ///     about 137 years of days.  Both servers stop at 1000 by default, but they name the limit differently and the
    ///     SQL is built without a connection, so each assignment is in an executable comment that only one of them runs:
    ///     <list type="bullet">
    ///         <item>
    ///             MySQL 8.0+ runs <c>/*!80000 ... */</c> (<c>cte_max_recursion_depth</c>, which fails the query at the
    ///             limit) and treats <c>/*M! ... */</c> as an ordinary comment
    ///         </item>
    ///         <item>
    ///             MariaDB 10.2+ runs <c>/*M!100200 ... */</c> (<c>max_recursive_iterations</c>, which stops the query at
    ///             the limit with only a warning, returning a truncated axis) and ignores MySQL comments versioned 5.7 to
    ///             9.x.  It rejects <c>cte_max_recursion_depth</c> as an unknown system variable
    ///         </item>
    ///     </list>
    /// </summary>
    private const string RecursionLimitAssignments =
        "/*!80000 cte_max_recursion_depth = 50000 */ /*M!100200 max_recursive_iterations = 50000 */";

    private MySqlAggregateHelper()
    {
    }

    /// <summary>
    ///     Generates a date axis CTE using MySQL 8.0+ (or MariaDB 10.2+) recursive CTEs.
    ///     Much simpler and more efficient than the legacy cross-join approach.
    ///     The queries that use it raise the recursion limit with <see cref="RecursionLimitAssignments" />.
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
            axisColumnAlias =
                query.SyntaxHelper.GetRuntimeName(query.AxisSelect.GetTextWithoutAlias(query.SyntaxHelper));

        WrapAxisColumnWithDatePartFunction(query, axisColumnAlias);

        var countAlias = query.CountSelect.GetAliasFromText(query.SyntaxHelper);

        if (string.IsNullOrWhiteSpace(countAlias))
            countAlias = query.SyntaxHelper.GetRuntimeName(query.CountSelect.GetTextWithoutAlias(query.SyntaxHelper));

        var sql = string.Format(CultureInfo.InvariantCulture, """

                                                              {0}

                                                              SET SESSION {6};

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
            string.Join(Environment.NewLine,
                query.Lines.Where(static c =>
                    c.LocationToInsert is >= QueryComponent.SELECT and <= QueryComponent.Having)),
            axisColumnAlias,
            RecursionLimitAssignments
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
        var part1 = GetPivotPart1(query, true);

        // Get the dateAxis CTE to include in the dynamic SQL
        var dateAxisCte = GetDateAxisTableDeclaration(query.Axis);

        return string.Format(CultureInfo.InvariantCulture, """

                                                           SET SESSION group_concat_max_len = 1000000, {7};

                                                           {0}

                                                           {1}

                                                           SET @sql =

                                                           CONCAT(
                                                           '{2}
                                                           SELECT
                                                           {3} as joinDt',@columnsSelectFromDataset,'
                                                           FROM
                                                           dateAxis
                                                           LEFT JOIN
                                                           (
                                                               {4}
                                                               {5} AS joinDt',@columnsSelectCases,'
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
            string.Join(Environment.NewLine,
                query.Lines.Where(static c => c.LocationToInsert == QueryComponent.SELECT)),

            //the from including all table joins and where but no calendar table join
            query.SyntaxHelper.Escape(GetDatePartOfColumn(query.Axis.AxisIncrement, axisColumnWithoutAlias)),

            //the from/where/join clauses (stopping before GROUP BY since it's hardcoded in the template)
            string.Join(Environment.NewLine,
                query.Lines.Where(static c => c.LocationToInsert is >= QueryComponent.FROM and < QueryComponent.GroupBy)
                    .Select(x => query.SyntaxHelper.Escape(x.Text))),
            RecursionLimitAssignments
        );
    }

    protected override string BuildPivotOnlyAggregate(AggregateCustomLineCollection query, CustomLine nonPivotColumn)
    {
        var part1 = GetPivotPart1(query, false);

        // The row column without the comma that ends its line, as the pivot columns bring their own
        var syntaxHelper = query.SyntaxHelper;
        var rowSql = nonPivotColumn.GetTextWithoutAlias(syntaxHelper);
        var rowAlias = syntaxHelper.EnsureWrapped(nonPivotColumn.GetAliasFromText(syntaxHelper) ??
                                                  syntaxHelper.GetRuntimeName(rowSql));

        return string.Format(CultureInfo.InvariantCulture, """

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
                                                           {4}
                                                           ');

                                                           PREPARE stmt FROM @sql;
                                                           EXECUTE stmt;
                                                           DEALLOCATE PREPARE stmt;
                                                           """,
            string.Join(Environment.NewLine, query.Lines.Where(static l => l.LocationToInsert < QueryComponent.SELECT)),
            part1,
            syntaxHelper.Escape($"{rowSql} AS {rowAlias}"),

            //everything inclusive of FROM but stopping before GROUP BY
            query.SyntaxHelper.Escape(string.Join(Environment.NewLine,
                query.Lines.Where(static c =>
                    c.LocationToInsert is >= QueryComponent.FROM and < QueryComponent.GroupBy))),
            syntaxHelper.Escape(rowAlias),

            //any HAVING SQL
            query.SyntaxHelper.Escape(string.Join(Environment.NewLine,
                query.Lines.Where(static c => c.LocationToInsert == QueryComponent.Having)))
        );
    }

    /// <summary>
    ///     <para>
    ///         Returns the section of the PIVOT which identifies unique values.  For MySQL 8.0+ this uses a CTE instead of a
    ///         temporary table and builds dynamic CASE statements.  IMPORTANT: When using TOP X (LIMIT), the LIMIT clause must
    ///         be placed at the CTE level AFTER the GROUP BY and HAVING clauses, NOT inside the ROW_NUMBER() OVER() window
    ///         function.  MySQL does not allow LIMIT inside window function OVER() clauses (GitHub Issue #38).
    ///     </para>
    ///     <para>
    ///         The values that get a column are those neither null nor empty (and on a record with a date for the axis, if
    ///         there is one), ranked (rn) in the order of their columns.  Each column is named after its value: its text
    ///         without NUL characters, with any character outside the Basic Multilingual Plane (which a MySQL name cannot
    ///         hold) as ?, trimmed of the spaces MySQL takes off the start of a name and cut to whole characters within
    ///         <see cref="MaxNameLength" />.  A value left with no name, or with the name of an earlier pivot column, gets
    ///         its position among the pivot columns as a suffix (e.g. _3, then _3_2 should that be taken too).  MySQL has
    ///         no loop outside stored programs, so the second suffix is tried once: names can only repeat when values are
    ///         written like other columns' names and suffixes, which MySQL allows in a SELECT list.
    ///     </para>
    ///     <para>
    ///         Each column list (@columnsSelectCases, @columnsSelectFromDataset) starts with the comma that separates it
    ///         from the column before it, and is empty when no value gets a column, so that the pivot then has only its
    ///         row (or axis) column, as on the other DBMS.  (GROUP_CONCAT of no rows is NULL, which would make the whole
    ///         statement NULL.)
    ///     </para>
    /// </summary>
    /// <param name="query">The pivot</param>
    /// <param name="namedInDerivedTable">
    ///     True when the CASE columns (@columnsSelectCases) go into a derived table, as for the axis pivot.  MySQL compares
    ///     column names there ignoring case, so they are named fansi_pivot_ and their position, and the SELECT from the
    ///     derived table (@columnsSelectFromDataset) gives them their names.  Otherwise they get their names themselves.
    /// </param>
    private static string GetPivotPart1(AggregateCustomLineCollection query, bool namedInDerivedTable)
    {
        if (query.PivotSelect == null)
            throw new InvalidOperationException("GetPivotPart1 requires PivotSelect to be non-null");
        if (query.CountSelect == null)
            throw new InvalidOperationException("GetPivotPart1 requires CountSelect to be non-null");

        var pivotSqlWithoutAlias = query.PivotSelect.GetTextWithoutAlias(query.SyntaxHelper);
        var countSqlWithoutAlias = query.CountSelect.GetTextWithoutAlias(query.SyntaxHelper);

        query.SyntaxHelper.SplitLineIntoOuterMostMethodAndContents(countSqlWithoutAlias, out var aggregateMethod,
            out var aggregateParameter);

        if (aggregateParameter.Equals("*", StringComparison.Ordinal))
            aggregateParameter = "1";

        // Only values that are neither null nor empty get a column, and if there is an axis only those of records that
        // are on it
        var whereNotNullOrEmpty = query.Lines.Any(static l => l.LocationToInsert == QueryComponent.WHERE)
            ? "AND "
            : "WHERE ";
        whereNotNullOrEmpty += $"({pivotSqlWithoutAlias}) IS NOT NULL AND CAST(({pivotSqlWithoutAlias}) AS CHAR) <> ''";
        if (query.AxisSelect != null)
            whereNotNullOrEmpty += $" AND ({query.AxisSelect.GetTextWithoutAlias(query.SyntaxHelper)}) IS NOT NULL";

        // Work out how to order the pivot columns
        var orderBy = $"{countSqlWithoutAlias} desc";

        var topXOrderByLine =
            query.Lines.SingleOrDefault(static c =>
                c.LocationToInsert == QueryComponent.OrderBy && c.Role == CustomLineRole.TopX);
        if (topXOrderByLine != null)
            orderBy = topXOrderByLine.Text;

        // break ties on the value itself so that the columns, and so their names, always come out in the same order
        orderBy += $", {pivotSqlWithoutAlias}";

        var topXLimitLine =
            query.Lines.SingleOrDefault(static c =>
                c.LocationToInsert == QueryComponent.Postfix && c.Role == CustomLineRole.TopX);
        var topXLimitSqlIfAny = topXLimitLine != null ? topXLimitLine.Text : "";

        var havingSqlIfAny = string.Join(Environment.NewLine,
            query.Lines.Where(static l => l.LocationToInsert == QueryComponent.Having).Select(static l => l.Text));

        var caseAlias = namedInDerivedTable
            ? "CONCAT('fansi_pivot_', rn)"
            : "CONCAT('`', REPLACE(CONVERT(name USING utf8mb4) COLLATE utf8mb4_bin, '`', '``'), '`')";

        return string.Format(CultureInfo.InvariantCulture, """

                                                           /* GROUP_CONCAT stops at 1024 bytes by default, which would leave out columns */
                                                           SET SESSION group_concat_max_len = 1073741824;

                                                           /* Get unique pivot values and their column names, and build both column lists in a single query */
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
                                                           ),
                                                           pivotTexts AS (
                                                               SELECT piv, rn,
                                                               RTRIM(LEFT(TRIM(REPLACE(CONVERT(CAST(piv AS CHAR) USING utf8mb3), CHAR(0 USING utf8mb3), '')), {8})) AS base
                                                               FROM pivotValues
                                                           ),
                                                           pivotTries AS (
                                                               SELECT piv, rn, base,
                                                               CASE WHEN base <> '' AND ROW_NUMBER() OVER (PARTITION BY CAST(base AS BINARY) ORDER BY rn) = 1 THEN 0 ELSE 1 END AS try
                                                               FROM pivotTexts
                                                           ),
                                                           pivotFirstNames AS (
                                                               SELECT piv, rn, base, try, {9} AS name
                                                               FROM pivotTries
                                                           ),
                                                           pivotNames AS (
                                                               SELECT piv, rn,
                                                               CASE WHEN EXISTS (SELECT 1 FROM pivotFirstNames e WHERE e.rn < f.rn AND CAST(e.name AS BINARY) = CAST(f.name AS BINARY)) THEN {10} ELSE name END AS name
                                                               FROM pivotFirstNames f
                                                           )
                                                           /* Each list starts with a comma, and is '' rather than NULL (GROUP_CONCAT of no rows) if no value gets a column */
                                                           SELECT
                                                             COALESCE(CONCAT(', ', GROUP_CONCAT(
                                                               CONCAT(
                                                                 '{0}(CASE WHEN {1} = ', QUOTE(piv), ' COLLATE utf8mb4_bin THEN {2} ELSE NULL END) AS ', {11}
                                                               ) ORDER BY rn
                                                             )), ''),
                                                             COALESCE(CONCAT(', ', GROUP_CONCAT(
                                                               CONCAT('dataset.fansi_pivot_', rn, ' AS `', REPLACE(CONVERT(name USING utf8mb4) COLLATE utf8mb4_bin, '`', '``'), '`') ORDER BY rn
                                                             )), '')
                                                           INTO @columnsSelectCases, @columnsSelectFromDataset
                                                           FROM pivotNames;

                                                           """,
            aggregateMethod,
            pivotSqlWithoutAlias,
            aggregateParameter,
            string.Join(Environment.NewLine,
                query.Lines.Where(static l =>
                    l.LocationToInsert is >= QueryComponent.FROM and <= QueryComponent.WHERE &&
                    l.Role != CustomLineRole.Axis)),
            whereNotNullOrEmpty,
            topXLimitSqlIfAny,
            orderBy,
            havingSqlIfAny,
            MaxNameLength,
            GetPivotNameSql("try"),
            GetPivotNameSql("f.try + 1", "f."),
            caseAlias
        );
    }

    /// <summary>
    ///     The SQL for the name of a pivot column on try <paramref name="trySql" /> (0 for none, 1 for the suffix _rn and 2 for
    ///     _rn_2): its base name cut to whole characters so that with the suffix it is at most <see cref="MaxNameLength" />
    /// </summary>
    private static string GetPivotNameSql(string trySql, string prefix = "")
    {
        var suffix =
            $"CONCAT(CASE WHEN {trySql} > 0 THEN CONCAT('_', {prefix}rn) ELSE '' END, CASE WHEN {trySql} > 1 THEN CONCAT('_', {trySql}) ELSE '' END)";
        return $"CONCAT(RTRIM(LEFT({prefix}base, {MaxNameLength} - CHAR_LENGTH({suffix}))), {suffix})";
    }

    protected override IQuerySyntaxHelper GetQuerySyntaxHelper() => MySqlQuerySyntaxHelper.Instance;
}
