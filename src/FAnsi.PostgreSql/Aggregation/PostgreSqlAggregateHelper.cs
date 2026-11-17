using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;

namespace FAnsi.Implementations.PostgreSql.Aggregation;

public sealed class PostgreSqlAggregateHelper : AggregateHelper
{
    public static readonly PostgreSqlAggregateHelper Instance = new();

    private PostgreSqlAggregateHelper()
    {
    }

    protected override IQuerySyntaxHelper GetQuerySyntaxHelper() => PostgreSqlSyntaxHelper.Instance;

    protected override string BuildAxisAggregate(AggregateCustomLineCollection query)
    {
        if (query.Axis == null)
            throw new ArgumentNullException(nameof(query), "Query Axis cannot be null");

        var interval = GetInterval(query.Axis.AxisIncrement);

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
                string.Join(Environment.NewLine,
                    query.Lines.Where(static c => c.LocationToInsert < QueryComponent.SELECT)),
                GetDatePartOfColumn(query.Axis.AxisIncrement, "generate_series.date"),
                //the entire query
                string.Join(Environment.NewLine,
                    query.Lines.Where(static c =>
                        c.LocationToInsert is >= QueryComponent.SELECT and <= QueryComponent.Having)),
                query.Axis.StartDate,
                query.Axis.EndDate,
                interval,
                countAlias,
                axisColumnAlias);

        return sql;
    }

    protected override string BuildPivotOnlyAggregate(AggregateCustomLineCollection query, CustomLine nonPivotColumn)
    {
        var syntaxHelper = query.SyntaxHelper;

        var rowAlias = nonPivotColumn.GetAliasFromText(syntaxHelper) ??
                       syntaxHelper.GetRuntimeName(nonPivotColumn.GetTextWithoutAlias(syntaxHelper));

        var pivotValuesSql = GetPivotValuesSql(query, null);

        var selectSql = $"""
                         WITH fansi_pivot_dataset ({GetDatasetColumns(query)}) AS (
                         {GetDatasetSql(query)}
                         )
                         SELECT fansi_pivot_dataset.fansi_row AS {syntaxHelper.EnsureWrapped(rowAlias)}
                         """;

        const string fromSql = """
                               FROM fansi_pivot_dataset
                               GROUP BY fansi_pivot_dataset.fansi_row
                               ORDER BY fansi_pivot_dataset.fansi_row
                               """;

        return BuildPivot(query, pivotValuesSql, selectSql, fromSql);
    }

    protected override string BuildPivotAndAxisAggregate(AggregateCustomLineCollection query)
    {
        if (query.Axis == null)
            throw new ArgumentNullException(nameof(query), "Query Axis cannot be null");

        var axis = query.Axis;
        var interval = GetInterval(axis.AxisIncrement);
        var axisSelect = query.AxisSelect ??
                         throw new ArgumentException("Query has no axis column (CustomLineRole.Axis)", nameof(query));
        var axisColumnAlias = axisSelect.GetAliasFromText(query.SyntaxHelper) ?? "joinDt";

        // Only values of records that are on the axis get a column, which has to be worked out before the axis column is
        // wrapped in a date part function
        var pivotValuesSql = GetPivotValuesSql(query, axisSelect.GetTextWithoutAlias(query.SyntaxHelper));

        WrapAxisColumnWithDatePartFunction(query, axisColumnAlias);

        var axisDate = GetDatePartOfColumn(axis.AxisIncrement, "fansi_pivot_calendar.dt");

        var selectSql = $"""
                         WITH fansi_pivot_dataset ({GetDatasetColumns(query)}) AS (
                         {GetDatasetSql(query)}
                         )
                         SELECT {axisDate} AS "joinDt"
                         """;

        var fromSql = $"""
                       FROM generate_series({axis.StartDate}, {axis.EndDate}, interval '{interval}') AS fansi_pivot_calendar(dt)
                       LEFT JOIN fansi_pivot_dataset ON fansi_pivot_dataset.fansi_axis = {axisDate}
                       GROUP BY fansi_pivot_calendar.dt
                       ORDER BY fansi_pivot_calendar.dt
                       """;

        return BuildPivot(query, pivotValuesSql, selectSql, fromSql);
    }

    /// <summary>
    ///     <para>
    ///         PostgreSQL has no PIVOT and a query cannot choose its own columns, so a pivot is built by a DO block.  It runs
    ///         <paramref name="pivotValuesSql" />, adds a column for each value it lists to the query
    ///         <paramref name="selectSql" /> ... <paramref name="fromSql" /> and opens a cursor on that query (see
    ///         <see cref="GetPivotCursorName" />).  The statements after the block FETCH the rows from the cursor and CLOSE it.
    ///     </para>
    ///     <para>
    ///         Each column sums the aggregate over the rows of the dataset that have its value, compared as the pivot column's
    ///         own type, and is 0 where there are none (as ISNULL(...,0) gives on SQL Server).  The cursor is declared WITH HOLD
    ///         so that it outlives the transaction of the DO statement when each statement is committed on its own.  If an
    ///         earlier run of the same query stopped before its CLOSE and left the cursor open in the session, the block
    ///         closes it first.
    ///     </para>
    /// </summary>
    private static string BuildPivot(AggregateCustomLineCollection query, string pivotValuesSql, string selectSql,
        string fromSql)
    {
        var cursor = GetPivotCursorName(pivotValuesSql, selectSql, fromSql);
        var sqlQuote = GetDollarQuote("fansi_sql", pivotValuesSql + selectSql + fromSql);

        var block = $"""
                     DECLARE
                         fansi_pivot_value text;
                         fansi_pivot_columns text := '';
                     BEGIN
                         FOR fansi_pivot_value IN EXECUTE {sqlQuote}
                     {pivotValuesSql}
                     {sqlQuote}
                         LOOP
                             fansi_pivot_columns := fansi_pivot_columns || format(', COALESCE(SUM(CASE WHEN fansi_pivot_dataset.fansi_value = %L THEN fansi_pivot_dataset.fansi_aggregate END), 0) AS %I', fansi_pivot_value, fansi_pivot_value);
                         END LOOP;

                         IF EXISTS (SELECT 1 FROM pg_catalog.pg_cursors WHERE name = '{cursor}') THEN
                             EXECUTE 'CLOSE {cursor}';
                         END IF;

                         EXECUTE 'DECLARE {cursor} CURSOR WITH HOLD FOR ' || {sqlQuote}
                     {selectSql}
                     {sqlQuote} || fansi_pivot_columns || {sqlQuote}
                     {fromSql}
                     {sqlQuote};
                     END
                     """;

        var blockQuote = GetDollarQuote("fansi_pivot", block);

        return $"""
                {GetLines(query, static l => l.LocationToInsert < QueryComponent.SELECT)}
                DO {blockQuote}
                {block}
                {blockQuote};
                FETCH ALL FROM {cursor};
                CLOSE {cursor};
                """;
    }

    /// <summary>
    ///     Returns a name for the cursor of a pivot, made from a hash of its query.  Every query has its own, so pivots run
    ///     on one connection never use or close each other's cursor, and the same query always gets the same name, so its
    ///     SQL is the same each time it is built (which matters to callers that cache results by their SQL, as RDMP does).
    /// </summary>
    private static string GetPivotCursorName(string pivotValuesSql, string selectSql, string fromSql)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(string.Join("\n", pivotValuesSql, selectSql, fromSql)));

        // fansi_pivot_ and 16 lower case hex digits: an identifier that needs no quotes and is far from the 63 byte limit
        var name = new StringBuilder("fansi_pivot_");
        foreach (var b in hash.AsSpan(0, 8))
            name.Append(b.ToString("x2", CultureInfo.InvariantCulture));

        return name.ToString();
    }

    /// <summary>
    ///     Returns a query listing (as text, in the order their columns appear) the values of the pivot column that get a
    ///     column: those that are neither null nor empty (and are on a record with a date for the axis, if
    ///     <paramref name="axisColumnSql" /> is given), ordered by the TOP X ORDER BY or else by the aggregate, largest first,
    ///     and limited by the TOP X, if there is one
    /// </summary>
    private static string GetPivotValuesSql(AggregateCustomLineCollection query, string? axisColumnSql)
    {
        var pivotSelect = query.PivotSelect ??
                          throw new ArgumentException("Query has no pivot column (CustomLineRole.Pivot)", nameof(query));
        var countSelect = query.CountSelect ??
                          throw new ArgumentException("Query has no aggregate column (CustomLineRole.CountFunction)",
                              nameof(query));
        var pivotSql = pivotSelect.GetTextWithoutAlias(query.SyntaxHelper);
        var countSql = countSelect.GetTextWithoutAlias(query.SyntaxHelper);

        // break ties on the value itself so that the columns always come out in the same order
        var orderBy = $"{query.TopXOrderBy?.Text ?? $"{countSql} desc"}, {pivotSql}";

        var whereOrAnd = query.Lines.Any(static l => l.LocationToInsert == QueryComponent.WHERE) ? "AND" : "WHERE";
        var onAxis = axisColumnSql == null ? "" : $" AND ({axisColumnSql}) IS NOT NULL";

        return $"""
                SELECT CAST(({pivotSql}) AS text)
                {GetLines(query, static l => l.LocationToInsert is >= QueryComponent.FROM and <= QueryComponent.WHERE && l.Role != CustomLineRole.Axis)}
                {whereOrAnd} ({pivotSql}) IS NOT NULL AND CAST(({pivotSql}) AS text) <> ''{onAxis}
                GROUP BY {pivotSql}
                {GetLines(query, static l => l.LocationToInsert == QueryComponent.Having)}
                ORDER BY {orderBy}
                {GetLines(query, static l => l.LocationToInsert == QueryComponent.Postfix && l.Role == CustomLineRole.TopX)}
                """;
    }

    /// <summary>
    ///     The query being pivoted, from SELECT to HAVING.  Any TOP X applies to the choice of pivot values, not to this
    /// </summary>
    private static string GetDatasetSql(AggregateCustomLineCollection query) =>
        GetLines(query, static l =>
            l.LocationToInsert is >= QueryComponent.SELECT and <= QueryComponent.Having && l.Role != CustomLineRole.TopX);

    /// <summary>
    ///     Names for the columns of <see cref="GetDatasetSql" /> (its <see cref="QueryComponent.QueryTimeColumn" /> lines, in
    ///     order).  Referring to the columns by these names means it does not matter whether, or how, the lines alias them.
    /// </summary>
    private static string GetDatasetColumns(AggregateCustomLineCollection query) =>
        string.Join(", ", query.Lines.Where(static l => l.LocationToInsert == QueryComponent.QueryTimeColumn)
            .Select(static l => l.Role switch
            {
                CustomLineRole.CountFunction => "fansi_aggregate",
                CustomLineRole.Pivot => "fansi_value",
                CustomLineRole.Axis => "fansi_axis",
                CustomLineRole.None => "fansi_row",
                _ => throw new InvalidOperationException($"A {l.Role} column cannot be part of a pivot: {l.Text}")
            }));

    private static string GetLines(AggregateCustomLineCollection query, Func<CustomLine, bool> predicate) =>
        string.Join(Environment.NewLine, query.Lines.Where(predicate).Select(static l => l.Text));

    /// <summary>
    ///     Returns a dollar quote (e.g. $fansi_sql$) that does not appear in <paramref name="content" />, so the content can
    ///     be quoted with it without being escaped
    /// </summary>
    private static string GetDollarQuote(string tag, string content)
    {
        var quote = $"${tag}$";

        for (var i = 1; content.Contains(quote, StringComparison.Ordinal); i++)
            quote = string.Create(CultureInfo.InvariantCulture, $"${tag}{i}$");

        return quote;
    }

    private static string GetInterval(AxisIncrement increment) =>
        increment switch
        {
            AxisIncrement.Day => "1 day",
            AxisIncrement.Month => "1 month",
            AxisIncrement.Year => "1 year",
            AxisIncrement.Quarter => "3 months",
            _ => throw new ArgumentOutOfRangeException(nameof(increment), increment,
                $"Invalid AxisIncrement {increment}")
        };

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
