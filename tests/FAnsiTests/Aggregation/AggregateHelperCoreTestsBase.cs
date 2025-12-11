using System.Data;
using System.Globalization;
using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

/// <summary>
///     Tests for core AggregateHelper functionality across all database types.
///     These tests target the GetDatePartOfColumn method and basic aggregate building.
/// </summary>
internal abstract class AggregateHelperCoreTestsBase : AggregationTests
{
    protected void Test_GetDatePartOfColumn_Day(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var helper = (AggregateHelper)tbl.Database.Server.GetQuerySyntaxHelper().AggregateHelper;
        var eventDateCol = tbl.DiscoverColumn("EventDate");

        var result = helper.GetDatePartOfColumn(AxisIncrement.Day, eventDateCol.GetFullyQualifiedName());

        Assert.That(result, Is.Not.Null);
        Assert.That(result, Is.Not.Empty);
        // Result should contain the column name
        Assert.That(result, Does.Contain(eventDateCol.GetRuntimeName()));
    }

    protected void Test_GetDatePartOfColumn_Month(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var helper = (AggregateHelper)tbl.Database.Server.GetQuerySyntaxHelper().AggregateHelper;
        var eventDateCol = tbl.DiscoverColumn("EventDate");

        var result = helper.GetDatePartOfColumn(AxisIncrement.Month, eventDateCol.GetFullyQualifiedName());

        Assert.That(result, Is.Not.Null);
        Assert.That(result, Is.Not.Empty);
        Assert.That(result, Does.Contain(eventDateCol.GetRuntimeName()));
    }

    protected void Test_GetDatePartOfColumn_Year(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var helper = (AggregateHelper)tbl.Database.Server.GetQuerySyntaxHelper().AggregateHelper;
        var eventDateCol = tbl.DiscoverColumn("EventDate");

        var result = helper.GetDatePartOfColumn(AxisIncrement.Year, eventDateCol.GetFullyQualifiedName());

        Assert.That(result, Is.Not.Null);
        Assert.That(result, Is.Not.Empty);
        Assert.That(result, Does.Contain(eventDateCol.GetRuntimeName()));
    }

    protected void Test_GetDatePartOfColumn_Quarter(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var helper = (AggregateHelper)tbl.Database.Server.GetQuerySyntaxHelper().AggregateHelper;
        var eventDateCol = tbl.DiscoverColumn("EventDate");

        var result = helper.GetDatePartOfColumn(AxisIncrement.Quarter, eventDateCol.GetFullyQualifiedName());

        Assert.That(result, Is.Not.Null);
        Assert.That(result, Is.Not.Empty);
        Assert.That(result, Does.Contain(eventDateCol.GetRuntimeName()));
    }

    protected void Test_SumAggregate(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var numberCol = tbl.DiscoverColumn("NumberInTrouble");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new($"SUM({numberCol.GetFullyQualifiedName()}) as Total", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        var result = Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);

        // Sum of all NumberInTrouble values: 7+11+49+13+17+19+23+29+31+37+41+59+47+53 = 436
        Assert.That(result, Is.EqualTo(436));
    }

    protected void Test_AvgAggregate(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var numberCol = tbl.DiscoverColumn("NumberInTrouble");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new($"AVG({numberCol.GetFullyQualifiedName()}) as Average", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        var result = Convert.ToDecimal(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);

        // Average of 436 / 14 = 31.14 (approximately)
        Assert.That(result, Is.GreaterThan(30));
        Assert.That(result, Is.LessThan(32));
    }

    protected void Test_MinAggregate(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var numberCol = tbl.DiscoverColumn("NumberInTrouble");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new($"MIN({numberCol.GetFullyQualifiedName()}) as Minimum", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        var result = Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);

        Assert.That(result, Is.EqualTo(7));
    }

    protected void Test_MaxAggregate(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var numberCol = tbl.DiscoverColumn("NumberInTrouble");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new($"MAX({numberCol.GetFullyQualifiedName()}) as Maximum", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        var result = Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);

        Assert.That(result, Is.EqualTo(59));
    }

    protected void Test_GroupByWithSum(DatabaseType type)
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
            new(category.GetFullyQualifiedName(), QueryComponent.OrderBy)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        Assert.That(dt.Rows, Has.Count.EqualTo(4));

        // Find row with category "E&, %a' mp;E" and verify sum
        DataRow? row = null;
        foreach (DataRow r in dt.Rows)
            if (r[1].ToString() == "E&, %a' mp;E")
            {
                row = r;
                break;
            }

        Assert.That(row, Is.Not.Null);
        // Sum for this category: 37+41+59 = 137
        Assert.That(Convert.ToInt32(row![0], CultureInfo.InvariantCulture), Is.EqualTo(137));
    }

    protected void Test_GroupByWithAvg(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var category = tbl.DiscoverColumn("Category");
        var numberCol = tbl.DiscoverColumn("NumberInTrouble");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new($"AVG({numberCol.GetFullyQualifiedName()}) as Average,", QueryComponent.QueryTimeColumn)
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
        Assert.That(dt.Columns, Has.Count.EqualTo(2));
    }

    protected void Test_CountDistinct(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var category = tbl.DiscoverColumn("Category");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new($"COUNT(DISTINCT {category.GetFullyQualifiedName()}) as UniqueCategories",
                QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        var result = Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);

        // There are 4 distinct categories: T, F, E&, %a' mp;E, G
        Assert.That(result, Is.EqualTo(4));
    }

    protected void Test_HavingClause_GreaterThan(DatabaseType type)
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
            new("HAVING", QueryComponent.Having),
            new($"SUM({numberCol.GetFullyQualifiedName()}) > 100", QueryComponent.Having),
            new("ORDER BY", QueryComponent.OrderBy),
            new(category.GetFullyQualifiedName(), QueryComponent.OrderBy)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        // Only categories with sum > 100 should be returned
        // T: 7+11+49+13+17+19+23 = 139 > 100
        // E&, %a' mp;E: 37+41+59 = 137 > 100
        // F: 29+31 = 60 < 100
        // G: 47+53 = 100 = 100 (not greater)
        Assert.That(dt.Rows, Has.Count.EqualTo(2));
    }

    protected void Test_HavingClause_Count(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var category = tbl.DiscoverColumn("Category");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("COUNT(*) as Total,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new(category.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn),
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new(category.GetFullyQualifiedName(), QueryComponent.GroupBy),
            new("HAVING", QueryComponent.Having),
            new("COUNT(*) >= 3", QueryComponent.Having),
            new("ORDER BY", QueryComponent.OrderBy),
            new(category.GetFullyQualifiedName(), QueryComponent.OrderBy)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        // Categories with 3 or more rows:
        // T: 7 rows
        // E&, %a' mp;E: 3 rows
        // F: 2 rows (excluded)
        // G: 2 rows (excluded)
        Assert.That(dt.Rows, Has.Count.EqualTo(2));
    }

    protected void Test_CustomAggregateAlias(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var numberCol = tbl.DiscoverColumn("NumberInTrouble");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new($"SUM({numberCol.GetFullyQualifiedName()}) as GrandTotal", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        Assert.That(dt.Columns, Has.Count.EqualTo(1));
        // Verify the alias is preserved
        Assert.That(dt.Columns[0].ColumnName, Does.Contain("GrandTotal").IgnoreCase);
    }

    protected void Test_MultipleGroupByColumns(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var category = tbl.DiscoverColumn("Category");
        var eventDate = tbl.DiscoverColumn("EventDate");
        var numberCol = tbl.DiscoverColumn("NumberInTrouble");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new($"SUM({numberCol.GetFullyQualifiedName()}) as Total,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new($"{category.GetFullyQualifiedName()},", QueryComponent.QueryTimeColumn),
            new(eventDate.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn),
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new($"{category.GetFullyQualifiedName()},", QueryComponent.GroupBy),
            new(eventDate.GetFullyQualifiedName(), QueryComponent.GroupBy),
            new("ORDER BY", QueryComponent.OrderBy),
            new($"{category.GetFullyQualifiedName()},", QueryComponent.OrderBy),
            new(eventDate.GetFullyQualifiedName(), QueryComponent.OrderBy)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        using var da = svr.GetDataAdapter(sql, con);
        using var dt = new DataTable();
        da.Fill(dt);

        // Should have more rows than grouping by category alone
        Assert.That(dt.Rows.Count, Is.GreaterThan(4));
        Assert.That(dt.Columns, Has.Count.EqualTo(3));
    }

    protected void Test_WhereClauseWithAggregate(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var category = tbl.DiscoverColumn("Category");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("COUNT(*) as Total", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("WHERE", QueryComponent.WHERE),
            new($"{category.GetFullyQualifiedName()} = 'T'", QueryComponent.WHERE)
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        var result = Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);

        // There are 7 rows with category 'T'
        Assert.That(result, Is.EqualTo(7));
    }

    protected void Test_WhereAndGroupBy(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var category = tbl.DiscoverColumn("Category");
        var eventDate = tbl.DiscoverColumn("EventDate");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("COUNT(*) as Total,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new(category.GetFullyQualifiedName(), QueryComponent.QueryTimeColumn),
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("WHERE", QueryComponent.WHERE),
            new($"{eventDate.GetFullyQualifiedName()} IS NOT NULL", QueryComponent.WHERE),
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

        // Should exclude the row with NULL EventDate (category 'G', one row)
        Assert.That(dt.Rows.Count, Is.GreaterThan(0));
        Assert.That(dt.Rows.Count, Is.LessThanOrEqualTo(4));
    }
}
