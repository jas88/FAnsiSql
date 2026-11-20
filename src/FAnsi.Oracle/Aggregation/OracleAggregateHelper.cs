using System.Globalization;
using System.Text;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;

namespace FAnsi.Implementations.Oracle.Aggregation;

public sealed class OracleAggregateHelper : AggregateHelper
{
    public static readonly OracleAggregateHelper Instance = new();

    private OracleAggregateHelper()
    {
    }

    protected override IQuerySyntaxHelper GetQuerySyntaxHelper() => OracleQuerySyntaxHelper.Instance;

    /// <summary>
    ///     Wraps AVG() function calls with ROUND() to prevent decimal overflow.
    ///     Oracle's NUMBER type can have up to 38 digits of precision, while .NET decimal only supports 28-29.
    ///     This limits precision to 10 decimal places which is sufficient for most use cases.
    /// </summary>
    private static string WrapAvgWithRound(string text)
    {
        if (string.IsNullOrEmpty(text) || !text.Contains("AVG(", StringComparison.OrdinalIgnoreCase))
            return text;

        // Simple approach: find AVG( and wrap the entire AVG(...) with ROUND(..., 10)
        // Handle nested parentheses properly
        var result = new StringBuilder();
        var i = 0;

        while (i < text.Length)
            // Look for AVG(
            if (i <= text.Length - 4 &&
                text.AsSpan(i, 4).Equals("AVG(", StringComparison.OrdinalIgnoreCase))
            {
                result.Append("ROUND(AVG(");
                i += 4;

                // Find the matching closing parenthesis
                var depth = 1;
                var start = i;
                while (i < text.Length && depth > 0)
                {
                    if (text[i] == '(') depth++;
                    else if (text[i] == ')') depth--;
                    i++;
                }

                // Append the AVG content and close the ROUND
                result.Append(text.AsSpan(start, i - start));
                result.Append(", 10)");
            }
            else
            {
                result.Append(text[i]);
                i++;
            }

        return result.ToString();
    }

    /// <summary>
    ///     Override BuildBasicAggregate to wrap AVG functions with ROUND
    /// </summary>
    protected override string BuildBasicAggregate(AggregateCustomLineCollection query)
    {
        var result = base.BuildBasicAggregate(query);
        return WrapAvgWithRound(result);
    }

    public override string GetDatePartOfColumn(AxisIncrement increment, string columnSql) =>
        increment switch
        {
            AxisIncrement.Day => $"TRUNC({columnSql})",
            AxisIncrement.Month => $"to_char({columnSql},'YYYY-MM')",
            AxisIncrement.Year => $"to_number(to_char({columnSql},'YYYY'))",
            AxisIncrement.Quarter => $"to_char({columnSql},'YYYY') || 'Q' || to_char({columnSql},'Q')",
            _ => throw new ArgumentOutOfRangeException(nameof(increment))
        };

    private static string GetDateAxisTableDeclaration(IQueryAxis axis)
    {
        //https://stackoverflow.com/questions/8374959/how-to-populate-calendar-table-in-oracle

        //expect the date to be either '2010-01-01' or a function that evaluates to a date e.g. CURRENT_TIMESTAMP

        var startDateSql =
            //is it a date in some format or other?
            DateTime.TryParse(axis.StartDate!.Trim('\'', '"'), out var start)
                ? $"to_date('{start:yyyyMMdd}','yyyymmdd')"
                : $"to_date(to_char({axis.StartDate!}, 'YYYYMMDD'), 'yyyymmdd')"; //assume its some Oracle specific syntax that results in a date

        var endDateSql = DateTime.TryParse(axis.EndDate!.Trim('\'', '"'), out var end)
            ? $"to_date('{end:yyyyMMdd}','yyyymmdd')"
            : $"to_date(to_char({axis.EndDate!}, 'YYYYMMDD'), 'yyyymmdd')"; //assume its some Oracle specific syntax that results in a date e.g. CURRENT_TIMESTAMP

        return axis.AxisIncrement switch
        {
            AxisIncrement.Year => $"""

                                   with calendar as (
                                           select add_months({startDateSql},12* (rownum - 1)) as dt
                                           from dual
                                           connect by rownum <= 1+
                                   floor(months_between({endDateSql}, {startDateSql}) /12)
                                       )
                                   """,
            AxisIncrement.Day => $"""

                                  with calendar as (
                                          select {startDateSql} + (rownum - 1) as dt
                                          from dual
                                          connect by rownum <= 1+
                                  floor({endDateSql} - {startDateSql})
                                      )
                                  """,
            AxisIncrement.Month => $"""

                                    with calendar as (
                                            select add_months({startDateSql},rownum - 1) as dt
                                            from dual
                                            connect by rownum <= 1+
                                    floor(months_between({endDateSql}, {startDateSql}))
                                        )
                                    """,
            AxisIncrement.Quarter => $"""

                                      with calendar as (
                                              select add_months({startDateSql},3* (rownum - 1)) as dt
                                              from dual
                                              connect by rownum <= 1+
                                      floor(months_between({endDateSql}, {startDateSql}) /3)
                                          )
                                      """,
            _ => throw new NotImplementedException()
        };
    }

    protected override string BuildAxisAggregate(AggregateCustomLineCollection query)
    {
        //we are trying to produce something like this:
        /*
with calendar as (
    select add_months(to_date('20010101','yyyymmdd'),12* (rownum - 1)) as dt
    from dual
    connect by rownum <= 1+
floor(months_between(to_date(to_char(CURRENT_TIMESTAMP, 'YYYYMMDD'), 'yyyymmdd'), to_date('20010101','yyyymmdd')) /12)
)
select
to_char(dt ,'YYYY') dt,
count(*) NumRecords
from calendar
join
"TEST"."HOSPITALADMISSIONS" on
to_char(dt ,'YYYY') = to_char("TEST"."HOSPITALADMISSIONS"."ADMISSION_DATE" ,'YYYY')
group by
dt
order by dt*/

        var countAlias = query.CountSelect!.GetAliasFromText(query.SyntaxHelper);
        var axisColumnAlias = query.AxisSelect!.GetAliasFromText(query.SyntaxHelper) ?? "joinDt";

        WrapAxisColumnWithDatePartFunction(query, axisColumnAlias);

        var calendar = GetDateAxisTableDeclaration(query.Axis!);

        return string.Format(
            CultureInfo.InvariantCulture,
            """

            {0}
            {1}
            SELECT
            {2} AS "joinDt",dataset.{3}
            FROM
            calendar
            LEFT JOIN
            (
                {4}
            ) dataset
            ON dataset.{5} = {2}
            ORDER BY
            {2}

            """,
            //add everything pre SELECT
            string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert < QueryComponent.SELECT)),
            //then add the calendar
            calendar,
            GetDatePartOfColumn(query.Axis!.AxisIncrement, "dt"),
            countAlias,
            //the entire query
            string.Join(Environment.NewLine,
                query.Lines.Where(static c =>
                    c.LocationToInsert is >= QueryComponent.SELECT and <= QueryComponent.Having)),
            axisColumnAlias
        );
    }

    protected override string BuildPivotOnlyAggregate(AggregateCustomLineCollection query, CustomLine nonPivotColumn)
    {
        var syntaxHelper = query.SyntaxHelper;

        var rowAlias = nonPivotColumn.GetAliasFromText(syntaxHelper) ??
                       syntaxHelper.GetRuntimeName(nonPivotColumn.GetTextWithoutAlias(syntaxHelper));

        var pivotValuesSql = GetPivotValuesSql(query, null);

        var selectSql = $"""
                         WITH fansi_pivot_dataset ({GetDatasetColumns(query)}) AS (
                         {WrapAvgWithRound(GetDatasetSql(query))}
                         ),
                         fansi_pivot_values AS (
                         {pivotValuesSql}
                         )
                         SELECT fansi_pivot_dataset.fansi_row AS {syntaxHelper.EnsureWrapped(rowAlias)}
                         """;

        const string fromSql = """
                               FROM fansi_pivot_dataset
                               LEFT JOIN fansi_pivot_values ON fansi_pivot_values.fansi_value = fansi_pivot_dataset.fansi_value
                               GROUP BY fansi_pivot_dataset.fansi_row
                               ORDER BY fansi_pivot_dataset.fansi_row
                               """;

        return BuildPivot(query, pivotValuesSql, selectSql, fromSql);
    }

    protected override string BuildPivotAndAxisAggregate(AggregateCustomLineCollection query)
    {
        var axis = query.Axis ?? throw new ArgumentNullException(nameof(query), "Query Axis cannot be null");
        var axisSelect = query.AxisSelect ??
                         throw new ArgumentException("Query has no axis column (CustomLineRole.Axis)", nameof(query));
        var axisColumnAlias = axisSelect.GetAliasFromText(query.SyntaxHelper) ?? "joinDt";

        // Only values of records that are on the axis get a column, which has to be worked out before the axis column is
        // wrapped in a date part function
        var pivotValuesSql = GetPivotValuesSql(query, axisSelect.GetTextWithoutAlias(query.SyntaxHelper));

        WrapAxisColumnWithDatePartFunction(query, axisColumnAlias);

        var axisDate = GetDatePartOfColumn(axis.AxisIncrement, "calendar.dt");

        var selectSql = $"""
                         {GetDateAxisTableDeclaration(axis)},
                         fansi_pivot_dataset ({GetDatasetColumns(query)}) AS (
                         {WrapAvgWithRound(GetDatasetSql(query))}
                         ),
                         fansi_pivot_values AS (
                         {pivotValuesSql}
                         )
                         SELECT {axisDate} AS "joinDt"
                         """;

        var fromSql = $"""
                       FROM calendar
                       LEFT JOIN fansi_pivot_dataset ON fansi_pivot_dataset.fansi_axis = {axisDate}
                       LEFT JOIN fansi_pivot_values ON fansi_pivot_values.fansi_value = fansi_pivot_dataset.fansi_value
                       GROUP BY calendar.dt
                       ORDER BY calendar.dt
                       """;

        return BuildPivot(query, pivotValuesSql, selectSql, fromSql);
    }

    /// <summary>
    ///     Returns the PL/SQL block that pivots the query <paramref name="selectSql" /> ... <paramref name="fromSql" /> on the
    ///     values listed by <paramref name="pivotValuesSql" />, see <see cref="OraclePivotBlock.Build" />
    /// </summary>
    private static string BuildPivot(AggregateCustomLineCollection query, string pivotValuesSql, string selectSql,
        string fromSql) =>
        OraclePivotBlock.Build(GetLines(query, static l => l.LocationToInsert < QueryComponent.SELECT), pivotValuesSql,
            selectSql, fromSql);

    /// <summary>
    ///     Returns a query of the values of the pivot column that get a column, numbered (fansi_rank) in the order their
    ///     columns appear: those that are not null (and are on a record with a date for the axis, if
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

        // break ties on the value itself, the ranking has to come out the same each time the query is run
        var orderBy = $"{query.TopXOrderBy?.Text ?? $"{countSql} desc"}, {pivotSql}";

        var whereOrAnd = query.Lines.Any(static l => l.LocationToInsert == QueryComponent.WHERE) ? "AND" : "WHERE";
        var onAxis = axisColumnSql == null ? "" : $" AND ({axisColumnSql}) IS NOT NULL";

        return $"""
                SELECT ROW_NUMBER() OVER (ORDER BY {orderBy}) AS fansi_rank, {pivotSql} AS fansi_value
                {GetLines(query, static l => l.LocationToInsert is >= QueryComponent.FROM and <= QueryComponent.WHERE && l.Role != CustomLineRole.Axis)}
                {whereOrAnd} ({pivotSql}) IS NOT NULL{onAxis}
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
}
