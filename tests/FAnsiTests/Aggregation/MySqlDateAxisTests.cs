using System.Data;
using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

/// <summary>
///     The date axis of a MySQL calendar aggregate is a recursive CTE, which MySQL and MariaDB both stop after 1000
///     iterations by default.  MySQL then fails the query but MariaDB returns the dates generated so far with only a
///     warning, so an axis longer than that must be checked for every row, on both.
/// </summary>
internal sealed class MySqlDateAxisTests : DatabaseTests
{
    private const DatabaseType DbType = DatabaseType.MySql;

    // 3288 days from 2001-01-01 to 2010-01-01 inclusive
    private const int DaysOnAxis = 3288;

    // The pivot columns are ordered by their total count
    private static readonly string[] PivotColumns = ["joinDt", "Red", "Blue"];

    [TestCase(false)]
    [TestCase(true)]
    public void Calendar_DayAxisLongerThanDefaultRecursionLimit_HasEveryDay(bool withPivot)
    {
        var db = GetTestDatabase(DbType);

        using var data = new DataTable();
        data.Columns.Add("EventDate");
        data.Columns.Add("Category");
        data.Rows.Add("2001-01-01", "Red");
        data.Rows.Add("2001-01-01", "Red");
        data.Rows.Add("2001-01-02", "Blue");
        data.Rows.Add("2009-12-31", "Red");

        var tbl = db.CreateTable("DateAxisData", data);
        var svr = tbl.Database.Server;
        var eventDate = tbl.DiscoverColumn("EventDate").GetFullyQualifiedName();

        var lines = withPivot
            ? new List<CustomLine>
            {
                new("SELECT", QueryComponent.SELECT),
                new("count(*) as MyCount,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
                new($"{eventDate},", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis },
                new("Category", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
                new("FROM ", QueryComponent.FROM),
                new(tbl.GetFullyQualifiedName(), QueryComponent.FROM),
                new("GROUP BY", QueryComponent.GroupBy),
                new($"{eventDate},", QueryComponent.GroupBy) { Role = CustomLineRole.Axis },
                new("Category", QueryComponent.GroupBy) { Role = CustomLineRole.Pivot }
            }
            : new List<CustomLine>
            {
                new("SELECT", QueryComponent.SELECT),
                new("count(*) as MyCount,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
                new(eventDate, QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis },
                new("FROM ", QueryComponent.FROM),
                new(tbl.GetFullyQualifiedName(), QueryComponent.FROM),
                new("GROUP BY", QueryComponent.GroupBy),
                new(eventDate, QueryComponent.GroupBy) { Role = CustomLineRole.Axis }
            };

        var axis = new QueryAxis
        {
            StartDate = "'2001-01-01'",
            EndDate = "'2010-01-01'",
            AxisIncrement = AxisIncrement.Day
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis);

        using var con = svr.GetConnection();
        con.Open();
        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        Assert.That(dt.Rows, Has.Count.EqualTo(DaysOnAxis));
        Assert.Multiple(() =>
        {
            Assert.That(dt.Rows[0][0], Is.EqualTo(new DateTime(2001, 1, 1)));
            Assert.That(dt.Rows[^1][0], Is.EqualTo(new DateTime(2010, 1, 1)));
            Assert.That(dt.Rows[0][1], Is.EqualTo(2));
            Assert.That(dt.Rows[DaysOnAxis - 2][1], Is.EqualTo(1));
        });

        if (withPivot)
            Assert.That(dt.Columns.Cast<DataColumn>().Select(static c => c.ColumnName),
                Is.EqualTo(PivotColumns).AsCollection);

        tbl.Drop();
    }
}
