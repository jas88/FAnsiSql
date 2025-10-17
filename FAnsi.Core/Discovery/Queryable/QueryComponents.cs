using System;
using System.Collections.Generic;
using System.Linq;

namespace FAnsi.Discovery.QueryableAbstraction
{
    /// <summary>
    /// Represents a DBMS-agnostic representation of a query extracted from LINQ expression trees.
    /// This is the intermediate format that gets translated to DBMS-specific SQL.
    /// </summary>
    /// <example>
    /// <code>
    /// // From: table.GetQueryable&lt;Patient&gt;().Where(p => p.Age > 18).OrderBy(p => p.Name).Take(10)
    /// // To: QueryComponents with:
    /// //   WhereClauses: [{ Property: "Age", Operator: GreaterThan, Value: 18 }]
    /// //   OrderByClauses: [{ Property: "Name", Ascending: true }]
    /// //   Take: 10
    /// </code>
    /// </example>
    public sealed class QueryComponents
    {
        /// <summary>
        /// WHERE clause conditions. Multiple conditions are combined with AND.
        /// </summary>
        public List<WhereClause> WhereClauses { get; } = new List<WhereClause>();

        /// <summary>
        /// ORDER BY clauses. Applied in the order they appear in the list.
        /// </summary>
        public List<OrderByClause> OrderByClauses { get; } = new List<OrderByClause>();

        /// <summary>
        /// LIMIT/TOP clause. Null means no limit.
        /// </summary>
        public int? Take { get; set; }

        /// <summary>
        /// OFFSET clause. Null means no offset. Only valid if Take is also specified.
        /// </summary>
        public int? Skip { get; set; }

        /// <summary>
        /// Indicates if any query components have been specified.
        /// </summary>
        public bool IsEmpty => !WhereClauses.Any() && !OrderByClauses.Any() && Take == null && Skip == null;

        /// <summary>
        /// Creates a deep copy of this QueryComponents instance.
        /// </summary>
        public QueryComponents Clone()
        {
            return new QueryComponents
            {
                Take = Take,
                Skip = Skip
            };
        }

        /// <summary>
        /// Adds a WHERE clause to the query.
        /// </summary>
        public void AddWhereClause(string propertyName, WhereOperator op, object? value)
        {
            WhereClauses.Add(new WhereClause(propertyName, op, value));
        }

        /// <summary>
        /// Adds an ORDER BY clause to the query.
        /// </summary>
        public void AddOrderByClause(string propertyName, bool ascending)
        {
            OrderByClauses.Add(new OrderByClause(propertyName, ascending));
        }
    }

    /// <summary>
    /// Represents a single WHERE condition.
    /// </summary>
    public sealed class WhereClause
    {
        /// <summary>
        /// The property/column name being filtered.
        /// </summary>
        public string PropertyName { get; }

        /// <summary>
        /// The comparison operator.
        /// </summary>
        public WhereOperator Operator { get; }

        /// <summary>
        /// The value to compare against. Null for IS NULL/IS NOT NULL.
        /// </summary>
        public object? Value { get; }

        public WhereClause(string propertyName, WhereOperator op, object? value)
        {
            PropertyName = propertyName ?? throw new ArgumentNullException(nameof(propertyName));
            Operator = op;
            Value = value;
        }

        public override string ToString() => $"{PropertyName} {Operator} {Value}";
    }

    /// <summary>
    /// Represents a single ORDER BY clause.
    /// </summary>
    public sealed class OrderByClause
    {
        /// <summary>
        /// The property/column name to order by.
        /// </summary>
        public string PropertyName { get; }

        /// <summary>
        /// True for ascending, false for descending.
        /// </summary>
        public bool Ascending { get; }

        public OrderByClause(string propertyName, bool ascending)
        {
            PropertyName = propertyName ?? throw new ArgumentNullException(nameof(propertyName));
            Ascending = ascending;
        }

        public override string ToString() => $"{PropertyName} {(Ascending ? "ASC" : "DESC")}";
    }

    /// <summary>
    /// Supported WHERE clause operators.
    /// </summary>
    public enum WhereOperator
    {
        /// <summary>
        /// Equality comparison (=)
        /// </summary>
        Equal,

        /// <summary>
        /// Inequality comparison (!=)
        /// </summary>
        NotEqual,

        /// <summary>
        /// Greater than comparison (&gt;)
        /// </summary>
        GreaterThan,

        /// <summary>
        /// Greater than or equal comparison (&gt;=)
        /// </summary>
        GreaterThanOrEqual,

        /// <summary>
        /// Less than comparison (&lt;)
        /// </summary>
        LessThan,

        /// <summary>
        /// Less than or equal comparison (&lt;=)
        /// </summary>
        LessThanOrEqual,

        /// <summary>
        /// LIKE pattern matching (SQL LIKE)
        /// </summary>
        Like,

        /// <summary>
        /// IS NULL check
        /// </summary>
        IsNull,

        /// <summary>
        /// IS NOT NULL check
        /// </summary>
        IsNotNull
    }
}
