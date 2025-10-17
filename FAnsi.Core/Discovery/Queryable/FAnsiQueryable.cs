using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace FAnsi.Discovery.QueryableAbstraction
{
    /// <summary>
    /// IQueryable implementation for FAnsiSql that enables LINQ-to-SQL translation.
    /// Wraps a query provider and expression tree, executing the query when enumerated.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence</typeparam>
    /// <example>
    /// <code>
    /// // Basic usage with DiscoveredTable
    /// using var connection = table.Database.Server.GetConnection();
    /// connection.Open();
    /// var queryable = table.GetQueryable&lt;Patient&gt;(connection);
    ///
    /// // Build query with LINQ
    /// var query = queryable
    ///     .Where(p => p.Age >= 18 &amp;&amp; p.Age &lt;= 65)
    ///     .Where(p => p.IsActive)
    ///     .OrderBy(p => p.LastName)
    ///     .ThenBy(p => p.FirstName)
    ///     .Take(100);
    ///
    /// // Query executes when enumerated
    /// foreach (var patient in query)
    /// {
    ///     Console.WriteLine($"{patient.FirstName} {patient.LastName}");
    /// }
    ///
    /// // Or materialize to list
    /// var results = query.ToList();
    /// </code>
    /// </example>
    public sealed class FAnsiQueryable<T> : IOrderedQueryable<T>
    {
        /// <summary>
        /// Gets the query provider that executes the query.
        /// </summary>
        public IQueryProvider Provider { get; }

        /// <summary>
        /// Gets the expression tree representing the query.
        /// </summary>
        public Expression Expression { get; }

        /// <summary>
        /// Gets the element type of the sequence.
        /// </summary>
        public Type ElementType => typeof(T);

        /// <summary>
        /// Creates a new queryable with the specified provider (root query).
        /// </summary>
        /// <param name="provider">The query provider that will execute the query</param>
        public FAnsiQueryable(IQueryProvider provider)
        {
            Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            Expression = Expression.Constant(this);
        }

        /// <summary>
        /// Creates a new queryable with the specified provider and expression.
        /// Used internally when building query chains (Where, OrderBy, etc.).
        /// </summary>
        /// <param name="provider">The query provider that will execute the query</param>
        /// <param name="expression">The expression tree representing the query</param>
        public FAnsiQueryable(IQueryProvider provider, Expression expression)
        {
            Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            Expression = expression ?? throw new ArgumentNullException(nameof(expression));

            if (!typeof(IQueryable<T>).IsAssignableFrom(expression.Type))
            {
                throw new ArgumentException(
                    $"Expression must be assignable to IQueryable<{typeof(T).Name}>",
                    nameof(expression));
            }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the query results.
        /// This is where query execution actually happens - the expression tree is
        /// translated to SQL, executed, and results are materialized.
        /// </summary>
        /// <returns>An enumerator for the query results</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the database connection is not open or when the query
        /// contains operations that cannot be translated to SQL
        /// </exception>
        public IEnumerator<T> GetEnumerator()
        {
            // Execute the query and get results
            var results = Provider.Execute<IEnumerable<T>>(Expression);
            return results.GetEnumerator();
        }

        /// <summary>
        /// Returns a non-generic enumerator.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Returns a string representation of the query for debugging.
        /// Note: This does not show the actual SQL - that's generated during execution.
        /// </summary>
        public override string ToString()
        {
            if (Expression is ConstantExpression)
            {
                return $"FAnsiQueryable<{typeof(T).Name}>";
            }

            return Expression.ToString();
        }
    }

    // NOTE: Extension methods removed - using .AsQueryable() wrapper pattern instead.
    // Future implementations can add GetQueryable<T> methods to DiscoveredTable/DiscoveredColumn
    // when full LINQ-to-SQL translation is implemented.
}
