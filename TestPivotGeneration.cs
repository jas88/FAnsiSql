using System;
using System.Collections.Generic;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Discovery.QuerySyntax.Aggregation;
using FAnsi.Implementations.MySql;

namespace TestPivotGeneration
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Testing MySQL Pivot SQL Generation...");

            var syntaxHelper = MySqlQuerySyntaxHelper.Instance;

            // Test 1: Basic Pivot with TOP 2
            Console.WriteLine("\n=== Test 1: Basic Pivot with TOP 2 ===");
            var lines1 = new List<CustomLine>
            {
                new("SELECT", QueryComponent.SELECT),
                new("count(*) as MyCount,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
                new("EventDate as Ev,", QueryComponent.QueryTimeColumn),
                new("Category as Cat", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
                new("FROM testTable", QueryComponent.FROM),
                new("GROUP BY", QueryComponent.GroupBy),
                new("EventDate,", QueryComponent.GroupBy),
                new("Category", QueryComponent.GroupBy) { Role = CustomLineRole.Pivot },
                new("count(*) desc", QueryComponent.OrderBy) { Role = CustomLineRole.TopX },
                new("LIMIT 2", QueryComponent.Postfix) { Role = CustomLineRole.TopX }
            };

            try
            {
                var sql1 = syntaxHelper.AggregateHelper.BuildAggregate(lines1, null);
                Console.WriteLine("Generated SQL:");
                Console.WriteLine(sql1);

                // Verify the SQL structure
                ValidateSqlStructure(sql1, "Test 1");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in Test 1: {ex.Message}");
            }

            // Test 2: Pivot with HAVING clause
            Console.WriteLine("\n=== Test 2: Pivot with HAVING clause ===");
            var lines2 = new List<CustomLine>
            {
                new("SELECT", QueryComponent.SELECT),
                new("count(*) as MyCount,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
                new("EventDate as Ev,", QueryComponent.QueryTimeColumn),
                new("Category as Cat", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
                new("FROM testTable", QueryComponent.FROM),
                new("GROUP BY", QueryComponent.GroupBy),
                new("EventDate,", QueryComponent.GroupBy),
                new("Category", QueryComponent.GroupBy) { Role = CustomLineRole.Pivot },
                new("HAVING count(*) > 1", QueryComponent.Having),
                new("count(*) desc", QueryComponent.OrderBy) { Role = CustomLineRole.TopX },
                new("LIMIT 2", QueryComponent.Postfix) { Role = CustomLineRole.TopX }
            };

            try
            {
                var sql2 = syntaxHelper.AggregateHelper.BuildAggregate(lines2, null);
                Console.WriteLine("Generated SQL:");
                Console.WriteLine(sql2);

                ValidateSqlStructure(sql2, "Test 2");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in Test 2: {ex.Message}");
            }

            // Test 3: Calendar with Pivot
            Console.WriteLine("\n=== Test 3: Calendar with Pivot ===");
            var lines3 = new List<CustomLine>
            {
                new("SELECT", QueryComponent.SELECT),
                new("count(*) as MyCount,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
                new("EventDate,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Axis },
                new("Category", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
                new("FROM testTable", QueryComponent.FROM),
                new("GROUP BY", QueryComponent.GroupBy),
                new("EventDate,", QueryComponent.GroupBy) { Role = CustomLineRole.Axis },
                new("Category", QueryComponent.GroupBy) { Role = CustomLineRole.Pivot }
            };

            var axis = new QueryAxis
            {
                StartDate = "'2001-01-01'",
                EndDate = "'2010-01-01'",
                AxisIncrement = AxisIncrement.Year
            };

            try
            {
                var sql3 = syntaxHelper.AggregateHelper.BuildAggregate(lines3, axis);
                Console.WriteLine("Generated SQL:");
                Console.WriteLine(sql3);

                ValidateSqlStructure(sql3, "Test 3");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in Test 3: {ex.Message}");
            }

            Console.WriteLine("\n=== Testing Complete ===");
        }

        static void ValidateSqlStructure(string sql, string testName)
        {
            Console.WriteLine($"\nValidating {testName}:");

            // Check for proper CTE structure
            if (sql.Contains("WITH pivotValues AS"))
            {
                Console.WriteLine("✓ Contains CTE structure");
            }
            else
            {
                Console.WriteLine("✗ Missing CTE structure");
            }

            // Check that LIMIT is not in window function
            if (sql.Contains("ROW_NUMBER() OVER (") && !sql.Contains("ROW_NUMBER() OVER (ORDER BY") || !sql.Split(new[] { "ROW_NUMBER() OVER (" }, StringSplitOptions.None)[1].Split(')')[0].Contains("LIMIT"))
            {
                Console.WriteLine("✓ LIMIT not in window function");
            }
            else if (sql.Contains("ROW_NUMBER() OVER ("))
            {
                var windowFunc = sql.Split(new[] { "ROW_NUMBER() OVER (" }, StringSplitOptions.None)[1].Split(')')[0];
                if (windowFunc.Contains("LIMIT"))
                {
                    Console.WriteLine("✗ LIMIT found in window function");
                }
                else
                {
                    Console.WriteLine("✓ LIMIT not in window function");
                }
            }

            // Check for proper HAVING placement (should be after GROUP BY)
            if (sql.Contains("HAVING"))
            {
                var groupByIndex = sql.IndexOf("GROUP BY");
                var havingIndex = sql.IndexOf("HAVING");

                if (havingIndex > groupByIndex)
                {
                    Console.WriteLine("✓ HAVING comes after GROUP BY");
                }
                else
                {
                    Console.WriteLine("✗ HAVING comes before GROUP BY");
                }
            }

            // Check for proper ORDER BY placement
            if (sql.Contains("ORDER BY"))
            {
                var havingIndex = sql.Contains("HAVING") ? sql.IndexOf("HAVING") : sql.IndexOf("GROUP BY");
                var orderByIndex = sql.IndexOf("ORDER BY");

                if (orderByIndex > havingIndex)
                {
                    Console.WriteLine("✓ ORDER BY comes after HAVING/GROUP BY");
                }
                else
                {
                    Console.WriteLine("✗ ORDER BY placement issue");
                }
            }

            // Check for dynamic SQL structure
            if (sql.Contains("CONCAT(") && sql.Contains("PREPARE stmt FROM @sql"))
            {
                Console.WriteLine("✓ Contains dynamic SQL structure");
            }
            else
            {
                Console.WriteLine("✗ Missing dynamic SQL structure");
            }
        }
    }
}
