using System.Data;
using System.Data.Common;
using System.Globalization;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using NUnit.Framework;
using TypeGuesser;

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
    ///     Every pivot column gets a valid name of its own, whatever its value (see <see cref="PivotNameValues" />), and
    ///     counts the records of that value
    /// </summary>
    /// <param name="type">The DBMS to pivot on</param>
    /// <param name="expectedNames">The names the pivot columns should get, in the order of <see cref="PivotNameValues" /></param>
    protected void Test_Pivot_ColumnNamesAreValidAndUnique(DatabaseType type, string[] expectedNames)
    {
        using var dt = new DataTable();
        dt.Columns.Add("Grp");
        dt.Columns.Add("Val");
        foreach (var (value, records) in PivotNameValues)
            for (var i = 0; i < records; i++)
                dt.Rows.Add("g", value);

        var db = GetTestTable(type).Database;
        DropIfExists(db, "PivotColumnNames");
        var tbl = db.CreateTable("PivotColumnNames", dt);

        try
        {
            using var pivot = Pivot(tbl);

            Assert.Multiple(() =>
            {
                Assert.That(GetPivotColumnNames(pivot), Is.EqualTo(expectedNames));
                Assert.That(GetPivotCounts(pivot), Is.EqualTo(PivotNameValues.Select(static v => v.Records)));
            });
        }
        finally
        {
            tbl.Drop();
        }
    }

    /// <summary>
    ///     Dates that differ only in their century, time of day or fraction of a second get pivot columns of their own, named
    ///     after the whole date and time whatever the session's date formats: here Oracle's defaults, which leave out the
    ///     century and the time, and PostgreSQL's ISO DateStyle
    /// </summary>
    /// <param name="type">The DBMS to pivot on</param>
    /// <param name="sqlType">The type of the date column</param>
    /// <param name="values">The dates as SQL literals, each with fewer records than the one before so that this is the order of their columns</param>
    /// <param name="expectedNames">The names the pivot columns should get</param>
    protected void Test_Pivot_DateColumnNames(DatabaseType type, string sqlType, string[] values, string[] expectedNames)
    {
        var db = GetTestTable(type).Database;
        DropIfExists(db, "PivotDateNames");
        var tbl = db.CreateTable("PivotDateNames",
        [
            new DatabaseColumnRequest("Grp", new DatabaseTypeRequest(typeof(string), 1)),
            new DatabaseColumnRequest("Val", sqlType)
        ]);

        try
        {
            var syntax = tbl.GetQuerySyntaxHelper();
            using (var con = db.Server.GetConnection())
            {
                con.Open();
                for (var i = 0; i < values.Length; i++)
                    for (var records = values.Length - i; records > 0; records--)
                    {
                        using var cmd = db.Server.GetCommand(
                            $"INSERT INTO {tbl.GetFullyQualifiedName()} ({syntax.EnsureWrapped("Grp")}, {syntax.EnsureWrapped("Val")}) VALUES ('g', {values[i]})",
                            con);
                        cmd.ExecuteNonQuery();
                    }
            }

            string[] sessionSql = type == DatabaseType.Oracle
                ?
                [
                    "ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MON-RR'",
                    "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'DD-MON-RR HH.MI.SSXFF AM'",
                    "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'DD-MON-RR HH.MI.SSXFF AM TZR'"
                ]
                : ["SET DateStyle TO 'ISO, MDY'"];

            using var pivot = Pivot(tbl, sessionSql);

            Assert.Multiple(() =>
            {
                Assert.That(GetPivotColumnNames(pivot), Is.EqualTo(expectedNames));
                Assert.That(GetPivotCounts(pivot), Is.EqualTo(Enumerable.Range(1, values.Length).Reverse()));
            });
        }
        finally
        {
            tbl.Drop();
        }
    }

    /// <summary>
    ///     The pivot values of <see cref="Test_Pivot_ColumnNamesAreValidAndUnique" /> and the number of records of each, all
    ///     different so that the order of the columns does not depend on the collation: values made only of double quotes,
    ///     two that differ only by a double quote and two that start the same and are longer than a name can be
    /// </summary>
    private static readonly (string Value, int Records)[] PivotNameValues =
    [
        ("T", 7),
        ("\"", 6),
        ("\"\"", 5),
        ("a\"b", 4),
        ("ab", 3),
        (new string('x', 140) + "1", 2),
        (new string('x', 140) + "2", 1)
    ];

    /// <summary>
    ///     The lines of a pivot of <paramref name="countSql" /> (e.g. the number of records) for each Category and EventDate
    /// </summary>
    private static List<CustomLine> GetPivotLines(DiscoveredTable tbl, string countSql) =>
        GetPivotLines(tbl, countSql, "Category", "Cat", "EventDate", "Ev");

    /// <summary>
    ///     The lines of a pivot of <paramref name="countSql" /> for each <paramref name="pivotColumn" /> value, in a row for
    ///     each <paramref name="rowColumn" /> value
    /// </summary>
    private static List<CustomLine> GetPivotLines(DiscoveredTable tbl, string countSql, string rowColumn, string rowAlias,
        string pivotColumn, string pivotAlias)
    {
        var syntax = tbl.GetQuerySyntaxHelper();
        var row = syntax.EnsureWrapped(rowColumn);
        var pivot = syntax.EnsureWrapped(pivotColumn);

        return
        [
            new CustomLine("SELECT", QueryComponent.SELECT),
            new CustomLine($"{countSql} as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new CustomLine($"{row} as {rowAlias},", QueryComponent.QueryTimeColumn),
            new CustomLine($"{pivot} as {pivotAlias}", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new CustomLine($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new CustomLine("GROUP BY", QueryComponent.GroupBy),
            new CustomLine($"{row},", QueryComponent.GroupBy),
            new CustomLine(pivot, QueryComponent.GroupBy) { Role = CustomLineRole.Pivot }
        ];
    }

    /// <summary>
    ///     Pivots the number of records of <paramref name="tbl" /> for each Val, in a row for each Grp, on a connection of its
    ///     own (so that the session settings <paramref name="sessionSql" /> run first do not outlast it)
    /// </summary>
    private static DataTable Pivot(DiscoveredTable tbl, params string[] sessionSql)
    {
        var builder = new DbConnectionStringBuilder { ConnectionString = tbl.Database.Server.Builder.ConnectionString };
        builder["Pooling"] = false;
        var svr = new DiscoveredServer(builder.ConnectionString, tbl.Database.Server.DatabaseType);

        var sql = tbl.GetQuerySyntaxHelper().AggregateHelper
            .BuildAggregate(GetPivotLines(tbl, "count(*)", "Grp", "Grp", "Val", "Val"), null);

        using var con = svr.GetConnection();
        con.Open();
        foreach (var setting in sessionSql)
        {
            using var cmd = svr.GetCommand(setting, con);
            cmd.ExecuteNonQuery();
        }

        return Fill(svr, con, sql);
    }

    private static IEnumerable<string> GetPivotColumnNames(DataTable pivot) =>
        pivot.Columns.Cast<DataColumn>().Skip(1).Select(static c => c.ColumnName);

    private static IEnumerable<int> GetPivotCounts(DataTable pivot) =>
        pivot.Rows[0].ItemArray.Skip(1).Select(static v => Convert.ToInt32(v, CultureInfo.InvariantCulture));

    private static void DropIfExists(DiscoveredDatabase db, string tableName)
    {
        var tbl = db.ExpectTable(tableName);
        if (tbl.Exists())
            tbl.Drop();
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
