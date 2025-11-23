using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

/// <summary>
/// Advanced tests for AggregateHelper focusing on routing logic, axis wrapping, and edge cases.
/// These tests verify the BuildAggregate method's decision-making for basic, axis, pivot, and
/// pivot+axis aggregates.
/// </summary>
[NonParallelizable]
internal sealed class AggregateHelperAdvancedTests : AggregationTests
{
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_BuildBasicAggregate_NoAxisNoPivot(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("COUNT(*) as MyCount", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM)
        };

        // No axis means BuildBasicAggregate should be called
        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        Assert.That(sql, Is.Not.Null);
        Assert.That(sql, Does.Contain("SELECT"));
        Assert.That(sql, Does.Contain("COUNT(*)"));
        Assert.That(sql, Does.Contain("FROM"));

        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        var result = Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
        Assert.That(result, Is.EqualTo(14));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_BuildAxisAggregate_DayIncrement(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var eventDate = tbl.DiscoverColumn("EventDate");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new(eventDate.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new(eventDate.GetFullyQualifiedName(), QueryComponent.GroupBy) { Role = CustomLineRole.Axis }
        };

        var axis = new QueryAxis
        {
            StartDate = "'2001-01-01'",
            EndDate = "'2001-01-05'",
            AxisIncrement = AxisIncrement.Day
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis);

        Assert.That(sql, Is.Not.Null);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        // Should have 5 rows (2001-01-01 to 2001-01-05 inclusive)
        Assert.That(dt.Rows, Has.Count.EqualTo(5));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_BuildAxisAggregate_MonthIncrement(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var eventDate = tbl.DiscoverColumn("EventDate");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new(eventDate.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new(eventDate.GetFullyQualifiedName(), QueryComponent.GroupBy) { Role = CustomLineRole.Axis }
        };

        var axis = new QueryAxis
        {
            StartDate = "'2001-01-01'",
            EndDate = "'2001-12-01'",
            AxisIncrement = AxisIncrement.Month
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis);

        Assert.That(sql, Is.Not.Null);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        // Should have 12 rows (Jan to Dec 2001)
        Assert.That(dt.Rows, Has.Count.EqualTo(12));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_AxisAggregate_VerifyWrappedColumn(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var eventDate = tbl.DiscoverColumn("EventDate");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new($"{eventDate.GetFullyQualifiedName()} as EventYear", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.Axis },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new(eventDate.GetFullyQualifiedName(), QueryComponent.GroupBy) { Role = CustomLineRole.Axis }
        };

        var axis = new QueryAxis
        {
            StartDate = "'2001-01-01'",
            EndDate = "'2003-01-01'",
            AxisIncrement = AxisIncrement.Year
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis);

        Assert.That(sql, Is.Not.Null);

        // Verify the SQL contains date part functions
        // Different databases have different syntax, but all should wrap the column
        var lowerSql = sql.ToLower(CultureInfo.InvariantCulture);
        Assert.That(lowerSql, Does.Contain("year").Or.Contains("yyyy"));

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        Assert.That(dt.Rows, Has.Count.EqualTo(3));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_GroupByWithOrderByDesc(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var category = tbl.DiscoverColumn("Category");
        var numberCol = tbl.DiscoverColumn("NumberInTrouble");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new($"SUM({numberCol.GetFullyQualifiedName()}) as Total,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new(category.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn),
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new(category.GetFullyQualifiedName(), QueryComponent.GroupBy),
            new("ORDER BY", QueryComponent.OrderBy),
            new($"SUM({numberCol.GetFullyQualifiedName()}) DESC", QueryComponent.OrderBy)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        Assert.That(dt.Rows, Has.Count.EqualTo(4));

        // First row should be the category with highest sum
        var firstSum = Convert.ToInt32(dt.Rows[0][0], CultureInfo.InvariantCulture);
        Assert.That(firstSum, Is.GreaterThanOrEqualTo(100)); // T or E&, %a' mp;E
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_MultipleAggregatesInSameQuery(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var category = tbl.DiscoverColumn("Category");
        var numberCol = tbl.DiscoverColumn("NumberInTrouble");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new($"COUNT(*) as RecordCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new($"SUM({numberCol.GetFullyQualifiedName()}) as Total,", QueryComponent.QueryTimeColumn),
            new($"AVG({numberCol.GetFullyQualifiedName()}) as Average,", QueryComponent.QueryTimeColumn),
            new($"MIN({numberCol.GetFullyQualifiedName()}) as Minimum,", QueryComponent.QueryTimeColumn),
            new($"MAX({numberCol.GetFullyQualifiedName()}) as Maximum,", QueryComponent.QueryTimeColumn),
            new(category.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn),
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new(category.GetFullyQualifiedName(), QueryComponent.GroupBy),
            new("ORDER BY", QueryComponent.OrderBy),
            new(category.GetFullyQualifiedName(), QueryComponent.OrderBy)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        Assert.That(dt.Rows, Has.Count.EqualTo(4));
        // 5 aggregate columns + 1 group by column = 6 columns
        Assert.That(dt.Columns, Has.Count.EqualTo(6));

        // Verify each row has valid aggregate values
        foreach (DataRow row in dt.Rows)
        {
            var count = Convert.ToInt32(row[0], CultureInfo.InvariantCulture);
            var sum = Convert.ToInt32(row[1], CultureInfo.InvariantCulture);
            var avg = Convert.ToDecimal(row[2], CultureInfo.InvariantCulture);
            var min = Convert.ToInt32(row[3], CultureInfo.InvariantCulture);
            var max = Convert.ToInt32(row[4], CultureInfo.InvariantCulture);

            Assert.That(count, Is.GreaterThan(0));
            Assert.That(sum, Is.GreaterThan(0));
            Assert.That(avg, Is.GreaterThan(0));
            Assert.That(min, Is.LessThanOrEqualTo(max));
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_HavingWithMultipleConditions(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var category = tbl.DiscoverColumn("Category");
        var numberCol = tbl.DiscoverColumn("NumberInTrouble");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new($"COUNT(*) as RecordCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new($"SUM({numberCol.GetFullyQualifiedName()}) as Total,", QueryComponent.QueryTimeColumn),
            new(category.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn),
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new(category.GetFullyQualifiedName(), QueryComponent.GroupBy),
            new("HAVING", QueryComponent.Having),
            new($"COUNT(*) >= 2 AND SUM({numberCol.GetFullyQualifiedName()}) > 50", QueryComponent.Having),
            new("ORDER BY", QueryComponent.OrderBy),
            new(category.GetFullyQualifiedName(), QueryComponent.OrderBy)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        // Should filter to categories with at least 2 records AND sum > 50
        // All 4 categories (T, F, E&, %a' mp;E, G) meet these criteria
        Assert.That(dt.Rows.Count, Is.EqualTo(4));

        foreach (DataRow row in dt.Rows)
        {
            var count = Convert.ToInt32(row[0], CultureInfo.InvariantCulture);
            var sum = Convert.ToInt32(row[1], CultureInfo.InvariantCulture);
            Assert.That(count, Is.GreaterThanOrEqualTo(2));
            Assert.That(sum, Is.GreaterThan(50));
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_EmptyResult_WithAxis(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var eventDate = tbl.DiscoverColumn("EventDate");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new(eventDate.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("WHERE", QueryComponent.WHERE),
            new("1=0", QueryComponent.WHERE), // Impossible condition
            new("GROUP BY", QueryComponent.GroupBy),
            new(eventDate.GetFullyQualifiedName(), QueryComponent.GroupBy) { Role = CustomLineRole.Axis }
        };

        var axis = new QueryAxis
        {
            StartDate = "'2001-01-01'",
            EndDate = "'2001-01-03'",
            AxisIncrement = AxisIncrement.Day
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, axis);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        // With axis, should still have rows for the date range, but with NULL or 0 counts
        Assert.That(dt.Rows, Has.Count.EqualTo(3));
        foreach (DataRow row in dt.Rows)
        {
            // Different databases handle missing data differently:
            // SQL Server/MySQL/PostgreSQL typically return NULL
            // SQLite may return 0 due to COALESCE in the axis implementation
            var countValue = row[1];
            Assert.That(countValue, Is.EqualTo(DBNull.Value).Or.EqualTo(0));
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_AggregateWithComplexExpression(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var numberCol = tbl.DiscoverColumn("NumberInTrouble");
        var category = tbl.DiscoverColumn("Category");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new($"SUM({numberCol.GetFullyQualifiedName()} * 2) as DoubledSum,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new(category.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn),
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new(category.GetFullyQualifiedName(), QueryComponent.GroupBy),
            new("ORDER BY", QueryComponent.OrderBy),
            new(category.GetFullyQualifiedName(), QueryComponent.OrderBy)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        Assert.That(dt.Rows, Has.Count.EqualTo(4));

        // Find category T and verify the sum is doubled
        DataRow? rowT = null;
        foreach (DataRow r in dt.Rows)
        {
            if (r[1].ToString() == "T")
            {
                rowT = r;
                break;
            }
        }
        Assert.That(rowT, Is.Not.Null);
        var doubledSum = Convert.ToInt32(rowT![0], CultureInfo.InvariantCulture);
        // T sum is 139, doubled is 278
        Assert.That(doubledSum, Is.EqualTo(278));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_GroupByWithNullHandling(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var eventDate = tbl.DiscoverColumn("EventDate");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("COUNT(*) as Total,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new(eventDate.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn),
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new(eventDate.GetFullyQualifiedName(), QueryComponent.GroupBy),
            new("ORDER BY", QueryComponent.OrderBy),
            new(eventDate.GetFullyQualifiedName(), QueryComponent.OrderBy)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        // Should include a group for NULL EventDate
        var nullRowCount = 0;
        DataRow? firstNullRow = null;
        foreach (DataRow r in dt.Rows)
        {
            if (r[1] == DBNull.Value)
            {
                nullRowCount++;
                firstNullRow ??= r;
            }
        }
        Assert.That(nullRowCount, Is.EqualTo(1));
        Assert.That(Convert.ToInt32(firstNullRow![0], CultureInfo.InvariantCulture), Is.EqualTo(1));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_AggregateWithCaseExpression(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var numberCol = tbl.DiscoverColumn("NumberInTrouble");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new(
                $"SUM(CASE WHEN {numberCol.GetFullyQualifiedName()} > 30 THEN 1 ELSE 0 END) as HighValueCount",
                QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        var result = Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);

        // Count values > 30: 49, 31, 37, 41, 59, 47, 53 = 7 values
        Assert.That(result, Is.EqualTo(7));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_GetDatePartOfColumn_AllIncrements_Consistency(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var helper = (AggregateHelper)tbl.Database.Server.GetQuerySyntaxHelper().AggregateHelper;
        var eventDateCol = tbl.DiscoverColumn("EventDate");
        var columnName = eventDateCol.GetFullyQualifiedName();

        // Test all increment types return non-empty strings
        var increments = new[]
        {
            AxisIncrement.Day,
            AxisIncrement.Month,
            AxisIncrement.Year,
            AxisIncrement.Quarter
        };

        foreach (var increment in increments)
        {
            var result = helper.GetDatePartOfColumn(increment, columnName);
            Assert.That(result, Is.Not.Null, $"Result is null for {increment}");
            Assert.That(result, Is.Not.Empty, $"Result is empty for {increment}");
            Assert.That(result.Length, Is.GreaterThan(columnName.Length),
                $"Result for {increment} should wrap the column name");
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_AggregateWithJoin(DatabaseType type)
    {
        // This test verifies aggregation works with explicit joins
        // We'll self-join the table to test the mechanism
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var syntax = svr.GetQuerySyntaxHelper();

        var t1Alias = "t1";
        var t2Alias = "t2";
        var t1Category = $"{syntax.EnsureWrapped(t1Alias)}.{syntax.EnsureWrapped("Category")}";
        var t2NumberInTrouble = $"{syntax.EnsureWrapped(t2Alias)}.{syntax.EnsureWrapped("NumberInTrouble")}";

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new($"COUNT(*) as Total,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new(t1Category, QueryComponent.QueryTimeColumn),
            new($"FROM {tbl.GetFullyQualifiedName()} {syntax.EnsureWrapped(t1Alias)}", QueryComponent.FROM),
            new("JOIN", QueryComponent.JoinInfoJoin),
            new(
                $"{tbl.GetFullyQualifiedName()} {syntax.EnsureWrapped(t2Alias)} ON {syntax.EnsureWrapped(t1Alias)}.{syntax.EnsureWrapped("Category")} = {syntax.EnsureWrapped(t2Alias)}.{syntax.EnsureWrapped("Category")}",
                QueryComponent.JoinInfoJoin),
            new("WHERE", QueryComponent.WHERE),
            new($"{t2NumberInTrouble} > 20", QueryComponent.WHERE),
            new("GROUP BY", QueryComponent.GroupBy),
            new(t1Category, QueryComponent.GroupBy),
            new("ORDER BY", QueryComponent.OrderBy),
            new(t1Category, QueryComponent.OrderBy)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        // Should have results for categories that have values > 20
        Assert.That(dt.Rows.Count, Is.GreaterThan(0));
    }
}
