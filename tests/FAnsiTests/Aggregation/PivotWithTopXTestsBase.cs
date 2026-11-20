using System.Data;
using FAnsi;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

/// <summary>
///     Tests for PIVOT operations combined with TOP X (LIMIT) functionality.
///     Addresses GitHub Issue #38: MySQL PIVOT+TOP query generation bug.
/// </summary>
internal abstract class PivotWithTopXTestsBase : AggregationTests
{
    /// <summary>
    ///     Tests basic PIVOT with TOP 2 ordered by count descending.
    ///     Expected: Top 2 most common categories (T and E&amp;, %a' mp;E) should appear as columns.
    /// </summary>
    protected void Test_PivotWithTop2_OrderByCountDesc(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var syntax = svr.GetQuerySyntaxHelper();

        var eventDate = syntax.EnsureWrapped("EventDate");
        var category = syntax.EnsureWrapped("Category");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new($"{eventDate} as Ev,", QueryComponent.QueryTimeColumn),
            new($"{category} as Cat", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new($"{eventDate},", QueryComponent.GroupBy),
            new(category, QueryComponent.GroupBy) { Role = CustomLineRole.Pivot },
            // TOP 2 by count descending (should get T and E&, %a' mp;E)
            new("count(*) desc", QueryComponent.OrderBy) { Role = CustomLineRole.TopX },
            GetTopXLine(syntax, 2)
        };

        var sql = syntax.AggregateHelper.BuildAggregate(lines, null);


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

        var ev = syntax.GetRuntimeName("Ev");
        Assert.That(columnNames, Contains.Item(ev), "Should have EventDate column");
        // T has 7 records, E&, %a' mp;E has 3 records - these should be TOP 2
        // Count pivot columns (exclude "Ev" which contains 'E' but is the axis column, not a pivot)
        Assert.That(columnNames.Count(c => c != ev), Is.EqualTo(2),
            "Should have exactly 2 pivot category columns");
        Assert.That(columnNames, Has.Member("T").And.Member("E&, %a' mp;E"),
            "The pivot columns should be the 2 most common categories");
    }

    /// <summary>
    ///     Tests PIVOT with TOP 2 and a HAVING clause.
    ///     Expected: Only categories with count > 1, then TOP 2 of those.
    /// </summary>
    protected void Test_PivotWithTop2_WithHaving(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var syntax = svr.GetQuerySyntaxHelper();

        var eventDate = syntax.EnsureWrapped("EventDate");
        var category = syntax.EnsureWrapped("Category");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new($"{eventDate} as Ev,", QueryComponent.QueryTimeColumn),
            new($"{category} as Cat", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new($"{eventDate},", QueryComponent.GroupBy),
            new(category, QueryComponent.GroupBy) { Role = CustomLineRole.Pivot },
            // HAVING to filter categories with more than 1 occurrence
            new("HAVING count(*) > 1", QueryComponent.Having),
            // TOP 2 by count descending
            new("count(*) desc", QueryComponent.OrderBy) { Role = CustomLineRole.TopX },
            GetTopXLine(syntax, 2)
        };

        var sql = syntax.AggregateHelper.BuildAggregate(lines, null);


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
    ///     Tests PIVOT with TOP 2 and a WHERE clause.
    ///     Expected: Filter data first with WHERE, then PIVOT on TOP 2.
    /// </summary>
    protected void Test_PivotWithTop2_WithWhere(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var syntax = svr.GetQuerySyntaxHelper();

        var eventDate = syntax.EnsureWrapped("EventDate");
        var category = syntax.EnsureWrapped("Category");

        // Records from 2002 onwards.  Comparing the year avoids writing a date literal, which every DBMS reads differently
        var eventYear = ((AggregateHelper)syntax.AggregateHelper).GetDatePartOfColumn(AxisIncrement.Year, eventDate);

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new($"{eventDate} as Ev,", QueryComponent.QueryTimeColumn),
            new($"{category} as Cat", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            // WHERE clause to filter before pivoting
            new($"WHERE {eventYear} >= 2002", QueryComponent.WHERE),
            new("GROUP BY", QueryComponent.GroupBy),
            new($"{eventDate},", QueryComponent.GroupBy),
            new(category, QueryComponent.GroupBy) { Role = CustomLineRole.Pivot },
            // TOP 2 by count descending
            new("count(*) desc", QueryComponent.OrderBy) { Role = CustomLineRole.TopX },
            GetTopXLine(syntax, 2)
        };

        var sql = syntax.AggregateHelper.BuildAggregate(lines, null);


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
    ///     Tests PIVOT with TOP 2 using custom ORDER BY (alphabetical).
    ///     Expected: First 2 categories alphabetically.
    /// </summary>
    protected void Test_PivotWithTop2_OrderByAlphabetical(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var syntax = svr.GetQuerySyntaxHelper();

        var eventDate = syntax.EnsureWrapped("EventDate");
        var category = syntax.EnsureWrapped("Category");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new($"{eventDate} as Ev,", QueryComponent.QueryTimeColumn),
            new($"{category} as Cat", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new($"{eventDate},", QueryComponent.GroupBy),
            new(category, QueryComponent.GroupBy) { Role = CustomLineRole.Pivot },
            // TOP 2 by category name alphabetically ascending
            new($"{category} asc", QueryComponent.OrderBy) { Role = CustomLineRole.TopX },
            GetTopXLine(syntax, 2)
        };

        var sql = syntax.AggregateHelper.BuildAggregate(lines, null);


        using var con = svr.GetConnection();
        con.Open();

        var cmd = svr.GetCommand(sql, con);
        var da = svr.GetDataAdapter(cmd);
        using var dt = new DataTable();
        da.Fill(dt);

        // Should have EventDate column plus 2 category columns ordered alphabetically
        Assert.That(dt.Columns.Count, Is.EqualTo(3),
            $"Expected 3 columns with alphabetical ordering, got {dt.Columns.Count}");

        var columnNames = dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
        Assert.That(columnNames, Has.Member("E&, %a' mp;E").And.Member("F"),
            "The pivot columns should be the first 2 categories alphabetically");
    }

    /// <summary>
    ///     Tests PIVOT with TOP 2, combining both WHERE and HAVING clauses.
    ///     Expected: Complex filtering before and after grouping, then TOP 2.
    /// </summary>
    protected void Test_PivotWithTop2_WhereAndHaving(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var syntax = svr.GetQuerySyntaxHelper();

        var eventDate = syntax.EnsureWrapped("EventDate");
        var category = syntax.EnsureWrapped("Category");
        var numberInTrouble = syntax.EnsureWrapped("NumberInTrouble");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new($"sum({numberInTrouble}) as TotalTrouble,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new($"{eventDate} as Ev,", QueryComponent.QueryTimeColumn),
            new($"{category} as Cat", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            // WHERE clause to filter before grouping
            new($"WHERE {numberInTrouble} > 10", QueryComponent.WHERE),
            new("GROUP BY", QueryComponent.GroupBy),
            new($"{eventDate},", QueryComponent.GroupBy),
            new(category, QueryComponent.GroupBy) { Role = CustomLineRole.Pivot },
            // HAVING clause to filter after grouping
            new($"HAVING sum({numberInTrouble}) > 20", QueryComponent.Having),
            // TOP 2 by sum descending
            new($"sum({numberInTrouble}) desc", QueryComponent.OrderBy) { Role = CustomLineRole.TopX },
            GetTopXLine(syntax, 2)
        };

        var sql = syntax.AggregateHelper.BuildAggregate(lines, null);


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
    ///     Tests PIVOT with TOP 1 to verify single-column pivot works.
    ///     Edge case: TOP 1 should produce only 1 pivot column.
    /// </summary>
    protected void Test_PivotWithTop1_SingleColumn(DatabaseType type)
    {
        var tbl = GetTestTable(type);
        var svr = tbl.Database.Server;
        var syntax = svr.GetQuerySyntaxHelper();

        var eventDate = syntax.EnsureWrapped("EventDate");
        var category = syntax.EnsureWrapped("Category");

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new($"{eventDate} as Ev,", QueryComponent.QueryTimeColumn),
            new($"{category} as Cat", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new($"{eventDate},", QueryComponent.GroupBy),
            new(category, QueryComponent.GroupBy) { Role = CustomLineRole.Pivot },
            // TOP 1 should get only the most common category (T)
            new("count(*) desc", QueryComponent.OrderBy) { Role = CustomLineRole.TopX },
            GetTopXLine(syntax, 1)
        };

        var sql = syntax.AggregateHelper.BuildAggregate(lines, null);


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

        Assert.That(columnNames, Contains.Item(syntax.GetRuntimeName("Ev")), "Should have EventDate column");
        Assert.That(columnNames, Contains.Item("T"),
            "Should have 'T' as the single pivot column (most common category)");
    }

    /// <summary>
    ///     The line that limits the pivot to its first <paramref name="x" /> values, written as the DBMS needs it (e.g. TOP X
    ///     for SQL Server, LIMIT X for MySQL)
    /// </summary>
    private static CustomLine GetTopXLine(IQuerySyntaxHelper syntax, int x)
    {
        var topX = syntax.HowDoWeAchieveTopX(x);
        return new CustomLine(topX.SQL, topX.Location) { Role = CustomLineRole.TopX };
    }
}
