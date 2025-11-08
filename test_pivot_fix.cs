using System;
using System.Collections.Generic;
using System.Linq;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;
using FAnsi.Implementations.MicrosoftSQL;

namespace TestPivotFix;

class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("Testing SQL Server PIVOT TOP clause fix...");

        var syntaxHelper = MicrosoftQuerySyntaxHelper.Instance;
        var aggregateHelper = syntaxHelper.AggregateHelper;

        var lines = new List<CustomLine>
        {
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn)
                { Role = CustomLineRole.CountFunction },
            new("EventDate as Ev,", QueryComponent.QueryTimeColumn),
            new("Category as Cat", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new("FROM TestTable", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new("EventDate,", QueryComponent.GroupBy),
            new("Category", QueryComponent.GroupBy) { Role = CustomLineRole.Pivot },
            new("count(*) desc", QueryComponent.OrderBy) { Role = CustomLineRole.TopX },
            new("LIMIT 2", QueryComponent.Postfix) { Role = CustomLineRole.TopX }
        };

        var query = new AggregateCustomLineCollection(lines, syntaxHelper);

        try
        {
            var sql = aggregateHelper.BuildAggregate(query, null);

            Console.WriteLine("Generated SQL:");
            Console.WriteLine(sql);
            Console.WriteLine();

            // Check if the fix worked
            var hasInlineTop = sql.Contains("SELECT TOP 2") && !sql.Contains("SELECT TOP 2\n");
            var hasCorrectOrdering = sql.Contains("order by\ncount(*) desc");

            Console.WriteLine("Analysis:");
            Console.WriteLine($"✓ TOP clause is inline with SELECT: {hasInlineTop}");
            Console.WriteLine($"✓ ORDER BY clause present: {hasCorrectOrdering}");

            if (hasInlineTop && hasCorrectOrdering)
            {
                Console.WriteLine("\n🎉 Fix appears to be working correctly!");
                Console.WriteLine("The TOP clause is now inline with SELECT as required by SQL Server.");
            }
            else
            {
                Console.WriteLine("\n❌ Fix may not be working as expected.");
                if (!hasInlineTop)
                    Console.WriteLine("The TOP clause is not properly inline with SELECT.");
                if (!hasCorrectOrdering)
                    Console.WriteLine("The ORDER BY clause is missing or incorrect.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error generating SQL: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
        }
    }
}
