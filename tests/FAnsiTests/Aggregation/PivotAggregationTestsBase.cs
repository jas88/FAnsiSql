using System.Data;
using System.Data.Common;
using System.Globalization;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

internal abstract class PivotAggregationTestsBase : AggregationTests
{
    protected void Test_PivotOnlyCount(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var syntax = svr.GetQuerySyntaxHelper();

        var sql = syntax.AggregateHelper.BuildAggregate(GetPivotLines(tbl, "count(*)"), null);

        using var con = svr.GetConnection();
        con.Open();
        //Expected Test Results:
        /*
                Cat	    2001-01-01 00:00:00.0000000	2002-01-01 00:00:00.0000000	2002-02-01 00:00:00.0000000	2002-03-02 00:00:00.0000000	2003-01-01 00:00:00.0000000	2003-04-02 00:00:00.0000000	2005-01-01 00:00:00.0000000	2001-01-02 00:00:00.0000000
                E&, %a' mp;E	1	1	0	0	0	0	1	0
                F	            0	2	0	0	0	0	0	0
                G	            1	0	0	0	0	0	0	0
                T	            2	0	1	1	1	1	0	1
             */

        var cmd = svr.GetCommand(sql, con);
        var da = svr.GetDataAdapter(cmd);
        using var dt = new DataTable();
        da.Fill(dt);

        // The pivot columns are named after the dates, which each DBMS writes differently, so check the total of each row:
        // the number of records of that category with an EventDate
        var totals = dt.Rows.Cast<DataRow>().ToDictionary(
            static r => Convert.ToString(r[0], CultureInfo.InvariantCulture) ?? string.Empty,
            static r => r.ItemArray.Skip(1)
                .Sum(static v => v == DBNull.Value ? 0 : Convert.ToInt64(v, CultureInfo.InvariantCulture)));

        Assert.Multiple(() =>
        {
            Assert.That(dt.Columns, Has.Count.EqualTo(9));
            Assert.That(dt.Rows, Has.Count.EqualTo(4));
            Assert.That(dt.Columns[0].ColumnName, Is.EqualTo(syntax.GetRuntimeName("Cat")));
            Assert.That(totals, Is.EquivalentTo(new Dictionary<string, long>
            {
                ["E&, %a' mp;E"] = 3,
                ["F"] = 2,
                ["G"] = 1,
                ["T"] = 7
            }));
        });
    }

    /// <summary>
    ///     A pivot that fails while its rows are read, after it has started (on PostgreSQL, after the DO block has declared
    ///     its cursor), must not stop the pivots after it on the same connection.  On PostgreSQL, neither must a run that
    ///     stopped after the DO block and so left its cursor open.
    /// </summary>
    protected void Test_Pivot_SameConnectionAfterFailedPivot(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var syntax = svr.GetQuerySyntaxHelper();

        var sql = syntax.AggregateHelper.BuildAggregate(GetPivotLines(tbl, "count(*)"), null);

        // RDMP caches results by the SQL that made them, so the same query has to give the same SQL each time
        Assert.That(syntax.AggregateHelper.BuildAggregate(GetPivotLines(tbl, "count(*)"), null), Is.EqualTo(sql));

        // The subquery returns every NumberInTrouble, which every DBMS only rejects when the query runs.  The pivot values
        // are ordered by date, not by this aggregate, so they can be listed before the pivot fails
        var failingLines = GetPivotLines(tbl,
            $"count(*) + (SELECT {syntax.EnsureWrapped("NumberInTrouble")} FROM {tbl.GetFullyQualifiedName()})");
        var orderByDate = new CustomLine($"{syntax.EnsureWrapped("EventDate")} asc", QueryComponent.OrderBy);
        orderByDate.Role = CustomLineRole.TopX;
        failingLines.Add(orderByDate);
        var failingSql = syntax.AggregateHelper.BuildAggregate(failingLines, null);

        using var con = svr.GetConnection();
        con.Open();

        Assert.Catch<DbException>(() =>
        {
            using var failed = Fill(svr, con, failingSql);
        });

        if (type == DatabaseType.PostgreSql)
        {
            // Run the script only as far as its DO block, like a client that stopped there: its cursor stays open
            using (var cmd = svr.GetCommand(sql[..sql.IndexOf("FETCH ALL FROM", StringComparison.Ordinal)], con))
            {
                cmd.ExecuteNonQuery();
            }

            Assert.That(CountPostgreSqlPivotCursors(svr, con), Is.EqualTo(1), "The DO block should leave its cursor open");
        }

        for (var run = 0; run < 2; run++)
        {
            using var dt = Fill(svr, con, sql);

            Assert.Multiple(() =>
            {
                Assert.That(dt.Columns, Has.Count.EqualTo(9));
                Assert.That(dt.Rows, Has.Count.EqualTo(4));
            });
        }

        if (type == DatabaseType.PostgreSql)
            Assert.That(CountPostgreSqlPivotCursors(svr, con), Is.Zero, "No pivot cursor should be left open");
    }

    /// <summary>
    ///     The lines of a pivot of <paramref name="countSql" /> (e.g. the number of records) for each Category and EventDate
    /// </summary>
    private static List<CustomLine> GetPivotLines(DiscoveredTable tbl, string countSql)
    {
        var syntax = tbl.GetQuerySyntaxHelper();
        var category = syntax.EnsureWrapped("Category");
        var eventDate = syntax.EnsureWrapped("EventDate");

        return
        [
            new CustomLine("SELECT", QueryComponent.SELECT),
            new CustomLine($"{countSql} as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new CustomLine($"{category} as Cat,", QueryComponent.QueryTimeColumn),
            new CustomLine($"{eventDate} as Ev", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new CustomLine($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new CustomLine("GROUP BY", QueryComponent.GroupBy),
            new CustomLine($"{category},", QueryComponent.GroupBy),
            new CustomLine(eventDate, QueryComponent.GroupBy) { Role = CustomLineRole.Pivot }
        ];
    }

    private static DataTable Fill(DiscoveredServer svr, DbConnection con, string sql)
    {
        using var cmd = svr.GetCommand(sql, con);
        using var da = svr.GetDataAdapter(cmd);

        var dt = new DataTable();
        da.Fill(dt);
        return dt;
    }

    private static long CountPostgreSqlPivotCursors(DiscoveredServer svr, DbConnection con)
    {
        using var cmd = svr.GetCommand("SELECT count(*) FROM pg_catalog.pg_cursors WHERE name LIKE 'fansi_pivot%'", con);
        return Convert.ToInt64(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
    }
}
