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
        var pivotSqlWithoutAlias = query.PivotSelect!.GetTextWithoutAlias(query.SyntaxHelper);
        var countSqlWithoutAlias = query.CountSelect!.GetTextWithoutAlias(query.SyntaxHelper);

        query.SyntaxHelper.SplitLineIntoOuterMostMethodAndContents(countSqlWithoutAlias, out var aggregateMethod,
            out var aggregateParameter);

        if (aggregateParameter.Equals("*", StringComparison.Ordinal))
            aggregateParameter = "1";

        var nonPivotColumnAlias = nonPivotColumn.GetAliasFromText(query.SyntaxHelper);
        if (string.IsNullOrWhiteSpace(nonPivotColumnAlias))
            nonPivotColumnAlias =
                query.SyntaxHelper.GetRuntimeName(nonPivotColumn.GetTextWithoutAlias(query.SyntaxHelper));

        var havingSqlIfAny = string.Join(Environment.NewLine,
            query.Lines.Where(static l => l.LocationToInsert == QueryComponent.Having).Select(static l => l.Text));

        // Wrap the aggregate method if it's AVG to prevent overflow
        var wrappedAggregateMethod = aggregateMethod.Equals("AVG", StringComparison.OrdinalIgnoreCase)
            ? $"ROUND({aggregateMethod}(case when {pivotSqlWithoutAlias} = pv.piv then {aggregateParameter} else null end), 10)"
            : $"{aggregateMethod}(case when {pivotSqlWithoutAlias} = pv.piv then {aggregateParameter} else null end)";

        // Oracle has native PIVOT syntax but requires knowing the pivot values in advance
        // We'll use a two-step approach: first get distinct values, then use dynamic SQL or CASE statements
        // For now, using CASE statements similar to MySQL approach
        return string.Format(
            CultureInfo.InvariantCulture,
            """

            {0}
            /* Oracle pivot implementation using subquery */
            with source_data as (
                {1}
            ),
            pivot_values as (
                select distinct {2} as piv
                from source_data
                where {2} is not null
                order by {2}
            )
            select
                s.{3},
                {4}
            from source_data s
            cross join pivot_values pv
            group by s.{3}
            order by s.{3}
            {5}

            """,
            string.Join(Environment.NewLine, query.Lines.Where(static l => l.LocationToInsert < QueryComponent.SELECT)),
            string.Join(Environment.NewLine,
                query.Lines.Where(static c =>
                    c.LocationToInsert is >= QueryComponent.SELECT and < QueryComponent.GroupBy)),
            pivotSqlWithoutAlias,
            nonPivotColumnAlias,
            wrappedAggregateMethod,
            havingSqlIfAny
        );
    }

    protected override string BuildPivotAndAxisAggregate(AggregateCustomLineCollection query)
    {
        var pivotSqlWithoutAlias = query.PivotSelect!.GetTextWithoutAlias(query.SyntaxHelper);
        var countSqlWithoutAlias = query.CountSelect!.GetTextWithoutAlias(query.SyntaxHelper);
        var axisColumnAlias = query.AxisSelect!.GetAliasFromText(query.SyntaxHelper) ?? "joinDt";

        query.SyntaxHelper.SplitLineIntoOuterMostMethodAndContents(countSqlWithoutAlias, out var aggregateMethod,
            out var aggregateParameter);

        if (aggregateParameter.Equals("*", StringComparison.Ordinal))
            aggregateParameter = "1";

        WrapAxisColumnWithDatePartFunction(query, axisColumnAlias);

        var calendar = GetDateAxisTableDeclaration(query.Axis!);

        // Oracle pivot with date axis
        return string.Format(
            CultureInfo.InvariantCulture,
            """

            {0}
            {1},
            source_data as (
                {2}
            ),
            pivot_values as (
                select distinct {3} as piv
                from source_data
                where {3} is not null
                order by {3}
            )
            select
                {4} as "joinDt",
                {5}
            from calendar
            cross join pivot_values
            left join source_data ds on ds.{6} = {4}
            group by calendar.dt, pivot_values.piv
            order by calendar.dt

            """,
            string.Join(Environment.NewLine, query.Lines.Where(static c => c.LocationToInsert < QueryComponent.SELECT)),
            calendar,
            string.Join(Environment.NewLine,
                query.Lines.Where(static c =>
                    c.LocationToInsert is >= QueryComponent.SELECT and <= QueryComponent.Having)),
            pivotSqlWithoutAlias,
            GetDatePartOfColumn(query.Axis!.AxisIncrement, "calendar.dt"),
            $"{aggregateMethod}(case when {pivotSqlWithoutAlias} = pivot_values.piv then {aggregateParameter} else null end)",
            axisColumnAlias
        );
    }
}
