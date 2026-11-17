using System.Data;
using System.Globalization;
using FAnsi;
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

        var category = syntax.EnsureWrapped("Category");
        var eventDate = syntax.EnsureWrapped("EventDate");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new($"{category} as Cat,", QueryComponent.QueryTimeColumn),
            new($"{eventDate} as Ev", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new($"{category},", QueryComponent.GroupBy),
            new(eventDate, QueryComponent.GroupBy) { Role = CustomLineRole.Pivot }
        };

        var sql = syntax.AggregateHelper.BuildAggregate(lines, null);

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
}
