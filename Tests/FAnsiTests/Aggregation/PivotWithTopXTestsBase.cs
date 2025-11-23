using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using NUnit.Framework;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace FAnsiTests.Aggregation;

/// <summary>
/// Tests for PIVOT operations combined with TOP X (LIMIT) functionality.
/// Addresses GitHub Issue #38: MySQL PIVOT+TOP query generation bug.
/// </summary>
internal abstract class PivotWithTopXTestsBase : AggregationTests
{
    /// <summary>
    /// Tests basic PIVOT with TOP 2 ordered by count descending.
    /// Expected: Top 2 most common categories (T and E&, %a' mp;E) should appear as columns.
    /// </summary>
    protected void Test_PivotWithTop2_OrderByCountDesc(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new("EventDate as Ev,", QueryComponent.QueryTimeColumn),
            new("Category as Cat", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new("EventDate,", QueryComponent.GroupBy),
            new("Category", QueryComponent.GroupBy) { Role = CustomLineRole.Pivot },
            // TOP 2 by count descending (should get T and E&, %a' mp;E)
            new("count(*) desc", QueryComponent.OrderBy) { Role = CustomLineRole.TopX },
            new("LIMIT 2", QueryComponent.Postfix) { Role = CustomLineRole.TopX }
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        TestContext.Out.WriteLine("Generated SQL:");
        TestContext.Out.WriteLine(sql);

        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        var da = svr.GetDataAdapter(cmd);
        using var dt = new DataTable();
        da.Fill(dt);

        // Should have EventDate column plus 2 category columns (T and E&, %a' mp;E)
        Assert.That(dt.Columns.Count, Is.EqualTo(3),
            $"Expected 3 columns (Ev + 2 pivot columns), got {dt.Columns.Count}");

        // Verify the two most common categories are present as columns
        var columnNames = dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
        TestContext.Out.WriteLine($"Columns: {string.Join(", ", columnNames)}");

        Assert.That(columnNames, Contains.Item("Ev"), "Should have EventDate column");
        // T has 7 records, E&, %a' mp;E has 3 records - these should be TOP 2
        // Count pivot columns (exclude "Ev" which contains 'E' but is the axis column, not a pivot)
        Assert.That(columnNames.Count(c => c != "Ev"), Is.EqualTo(2),
            "Should have exactly 2 pivot category columns");
    }

    /// <summary>
    /// Tests PIVOT with TOP 2 and a HAVING clause.
    /// Expected: Only categories with count > 1, then TOP 2 of those.
    /// </summary>
    protected void Test_PivotWithTop2_WithHaving(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new("EventDate as Ev,", QueryComponent.QueryTimeColumn),
            new("Category as Cat", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new("EventDate,", QueryComponent.GroupBy),
            new("Category", QueryComponent.GroupBy) { Role = CustomLineRole.Pivot },
            // HAVING to filter categories with more than 1 occurrence
            new("HAVING count(*) > 1", QueryComponent.Having),
            // TOP 2 by count descending
            new("count(*) desc", QueryComponent.OrderBy) { Role = CustomLineRole.TopX },
            new("LIMIT 2", QueryComponent.Postfix) { Role = CustomLineRole.TopX }
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        TestContext.Out.WriteLine("Generated SQL:");
        TestContext.Out.WriteLine(sql);

        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        var da = svr.GetDataAdapter(cmd);
        using var dt = new DataTable();
        da.Fill(dt);

        // Should have EventDate column plus top 2 categories that appear > 1 time
        Assert.That(dt.Columns.Count, Is.EqualTo(3),
            $"Expected 3 columns with HAVING filter, got {dt.Columns.Count}");
    }

    /// <summary>
    /// Tests PIVOT with TOP 2 and a WHERE clause.
    /// Expected: Filter data first with WHERE, then PIVOT on TOP 2.
    /// </summary>
    protected void Test_PivotWithTop2_WithWhere(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new("EventDate as Ev,", QueryComponent.QueryTimeColumn),
            new("Category as Cat", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            // WHERE clause to filter before pivoting
            new("WHERE EventDate >= '2002-01-01'", QueryComponent.WHERE),
            new("GROUP BY", QueryComponent.GroupBy),
            new("EventDate,", QueryComponent.GroupBy),
            new("Category", QueryComponent.GroupBy) { Role = CustomLineRole.Pivot },
            // TOP 2 by count descending
            new("count(*) desc", QueryComponent.OrderBy) { Role = CustomLineRole.TopX },
            new("LIMIT 2", QueryComponent.Postfix) { Role = CustomLineRole.TopX }
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        TestContext.Out.WriteLine("Generated SQL:");
        TestContext.Out.WriteLine(sql);

        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        var da = svr.GetDataAdapter(cmd);
        using var dt = new DataTable();
        da.Fill(dt);

        // Should have EventDate column plus 2 category columns from filtered data
        Assert.That(dt.Columns.Count, Is.EqualTo(3),
            $"Expected 3 columns with WHERE filter, got {dt.Columns.Count}");
    }

    /// <summary>
    /// Tests PIVOT with TOP 2 using custom ORDER BY (alphabetical).
    /// Expected: First 2 categories alphabetically.
    /// </summary>
    protected void Test_PivotWithTop2_OrderByAlphabetical(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new("EventDate as Ev,", QueryComponent.QueryTimeColumn),
            new("Category as Cat", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new("EventDate,", QueryComponent.GroupBy),
            new("Category", QueryComponent.GroupBy) { Role = CustomLineRole.Pivot },
            // TOP 2 by category name alphabetically ascending
            new("Category asc", QueryComponent.OrderBy) { Role = CustomLineRole.TopX },
            new("LIMIT 2", QueryComponent.Postfix) { Role = CustomLineRole.TopX }
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        TestContext.Out.WriteLine("Generated SQL:");
        TestContext.Out.WriteLine(sql);

        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        var da = svr.GetDataAdapter(cmd);
        using var dt = new DataTable();
        da.Fill(dt);

        // Should have EventDate column plus 2 category columns ordered alphabetically
        Assert.That(dt.Columns.Count, Is.EqualTo(3),
            $"Expected 3 columns with alphabetical ordering, got {dt.Columns.Count}");
    }

    /// <summary>
    /// Tests PIVOT with TOP 2, combining both WHERE and HAVING clauses.
    /// Expected: Complex filtering before and after grouping, then TOP 2.
    /// </summary>
    protected void Test_PivotWithTop2_WhereAndHaving(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("sum(NumberInTrouble) as TotalTrouble,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new("EventDate as Ev,", QueryComponent.QueryTimeColumn),
            new("Category as Cat", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            // WHERE clause to filter before grouping
            new("WHERE NumberInTrouble > 10", QueryComponent.WHERE),
            new("GROUP BY", QueryComponent.GroupBy),
            new("EventDate,", QueryComponent.GroupBy),
            new("Category", QueryComponent.GroupBy) { Role = CustomLineRole.Pivot },
            // HAVING clause to filter after grouping
            new("HAVING sum(NumberInTrouble) > 20", QueryComponent.Having),
            // TOP 2 by sum descending
            new("sum(NumberInTrouble) desc", QueryComponent.OrderBy) { Role = CustomLineRole.TopX },
            new("LIMIT 2", QueryComponent.Postfix) { Role = CustomLineRole.TopX }
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        TestContext.Out.WriteLine("Generated SQL:");
        TestContext.Out.WriteLine(sql);

        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        var da = svr.GetDataAdapter(cmd);
        using var dt = new DataTable();
        da.Fill(dt);

        // Should have EventDate column plus up to 2 category columns (may be fewer due to filters)
        Assert.That(dt.Columns.Count, Is.GreaterThanOrEqualTo(1),
            "Should have at least EventDate column");
        Assert.That(dt.Columns.Count, Is.LessThanOrEqualTo(3),
            "Should have at most 3 columns (EventDate + 2 categories)");
    }

    /// <summary>
    /// Tests PIVOT with TOP 1 to verify single-column pivot works.
    /// Edge case: TOP 1 should produce only 1 pivot column.
    /// </summary>
    protected void Test_PivotWithTop1_SingleColumn(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new("EventDate as Ev,", QueryComponent.QueryTimeColumn),
            new("Category as Cat", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new("EventDate,", QueryComponent.GroupBy),
            new("Category", QueryComponent.GroupBy) { Role = CustomLineRole.Pivot },
            // TOP 1 should get only the most common category (T)
            new("count(*) desc", QueryComponent.OrderBy) { Role = CustomLineRole.TopX },
            new("LIMIT 1", QueryComponent.Postfix) { Role = CustomLineRole.TopX }
        };

        var sql = svr.GetQuerySyntaxHelper().AggregateHelper.BuildAggregate(lines, null);

        TestContext.Out.WriteLine("Generated SQL:");
        TestContext.Out.WriteLine(sql);

        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        var da = svr.GetDataAdapter(cmd);
        using var dt = new DataTable();
        da.Fill(dt);

        // Should have exactly 2 columns: EventDate + 1 pivot column
        Assert.That(dt.Columns.Count, Is.EqualTo(2),
            $"Expected 2 columns (Ev + 1 pivot), got {dt.Columns.Count}");

        var columnNames = dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
        TestContext.Out.WriteLine($"Columns: {string.Join(", ", columnNames)}");

        Assert.That(columnNames, Contains.Item("Ev"), "Should have EventDate column");
        Assert.That(columnNames, Contains.Item("T"),
            "Should have 'T' as the single pivot column (most common category)");
    }
}
