using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace FAnsi.Discovery.QueryableAbstraction
{
    /// <summary>
    /// Interface for DBMS-specific SQL query builders.
    /// Implementations translate DBMS-agnostic QueryComponents into concrete SQL syntax.
    /// </summary>
    /// <example>
    /// <code>
    /// var builder = new SqlServerQueryBuilder();
    /// var components = new QueryComponents();
    /// components.AddWhereClause("Age", WhereOperator.GreaterThan, 18);
    /// components.AddOrderByClause("Name", true);
    /// components.Take = 10;
    ///
    /// string sql = builder.BuildTableQuery("dbo.Patients", components);
    /// // Result: "SELECT TOP 10 * FROM dbo.Patients WHERE Age &gt; @p0 ORDER BY Name ASC"
    /// </code>
    /// </example>
    public interface ISqlQueryBuilder
    {
        /// <summary>
        /// Builds a SELECT query for an entire table based on the query components.
        /// </summary>
        /// <param name="tableName">Fully qualified table name (e.g., "dbo.Patients")</param>
        /// <param name="components">The query components to apply</param>
        /// <param name="parameters">Output parameter collection for parameterized values</param>
        /// <returns>Complete SQL SELECT statement</returns>
        /// <exception cref="NotSupportedException">Thrown when query components contain unsupported operations for this DBMS</exception>
        string BuildTableQuery(string tableName, QueryComponents components, out DbParameter[] parameters);

        /// <summary>
        /// Builds a SELECT query for a specific column based on the query components.
        /// </summary>
        /// <param name="tableName">Fully qualified table name</param>
        /// <param name="columnName">Column name to select</param>
        /// <param name="components">The query components to apply</param>
        /// <param name="parameters">Output parameter collection for parameterized values</param>
        /// <returns>Complete SQL SELECT statement</returns>
        /// <exception cref="NotSupportedException">Thrown when query components contain unsupported operations for this DBMS</exception>
        string BuildColumnQuery(string tableName, string columnName, QueryComponents components, out DbParameter[] parameters);

        /// <summary>
        /// Wraps a column/table name in DBMS-specific delimiters.
        /// </summary>
        /// <param name="identifier">The identifier to wrap</param>
        /// <returns>Wrapped identifier (e.g., [Name] for SQL Server, `Name` for MySQL)</returns>
        string WrapIdentifier(string identifier);

        /// <summary>
        /// Gets the parameter prefix for this DBMS.
        /// </summary>
        /// <returns>Parameter prefix (e.g., "@" for SQL Server, "?" for Oracle)</returns>
        string GetParameterPrefix();
    }

    /// <summary>
    /// Base implementation providing common SQL building functionality.
    /// </summary>
    public abstract class SqlQueryBuilderBase : ISqlQueryBuilder
    {
        public abstract string WrapIdentifier(string identifier);
        public abstract string GetParameterPrefix();

        protected abstract DbParameter CreateParameter(string name, object? value);

        public virtual string BuildTableQuery(string tableName, QueryComponents components, out DbParameter[] parameters)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentNullException(nameof(tableName));
            if (components == null)
                throw new ArgumentNullException(nameof(components));

            var paramList = new System.Collections.Generic.List<DbParameter>();
            var sql = BuildSelectClause(components);
            sql += $" FROM {tableName}";
            sql += BuildWhereClause(components, paramList);
            sql += BuildOrderByClause(components);
            sql += BuildLimitClause(components);

            parameters = paramList.ToArray();
            return sql;
        }

        public virtual string BuildColumnQuery(string tableName, string columnName, QueryComponents components, out DbParameter[] parameters)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentNullException(nameof(tableName));
            if (string.IsNullOrWhiteSpace(columnName))
                throw new ArgumentNullException(nameof(columnName));
            if (components == null)
                throw new ArgumentNullException(nameof(components));

            var paramList = new System.Collections.Generic.List<DbParameter>();
            var sql = $"SELECT {WrapIdentifier(columnName)} FROM {tableName}";
            sql += BuildWhereClause(components, paramList);
            sql += BuildOrderByClause(components);
            sql += BuildLimitClause(components);

            parameters = paramList.ToArray();
            return sql;
        }

        protected virtual string BuildSelectClause(QueryComponents components)
        {
            // SQL Server uses TOP, others override this method
            if (components.Take.HasValue && components.Skip == null)
                return $"SELECT TOP {components.Take.Value} *";

            return "SELECT *";
        }

        protected virtual string BuildWhereClause(QueryComponents components, System.Collections.Generic.List<DbParameter> parameters)
        {
            if (!components.WhereClauses.Any())
                return string.Empty;

            var conditions = new System.Collections.Generic.List<string>();
            int paramIndex = 0;

            foreach (var clause in components.WhereClauses)
            {
                string condition = clause.Operator switch
                {
                    WhereOperator.Equal => BuildComparison(clause.PropertyName, "=", clause.Value, ref paramIndex, parameters),
                    WhereOperator.NotEqual => BuildComparison(clause.PropertyName, "!=", clause.Value, ref paramIndex, parameters),
                    WhereOperator.GreaterThan => BuildComparison(clause.PropertyName, ">", clause.Value, ref paramIndex, parameters),
                    WhereOperator.GreaterThanOrEqual => BuildComparison(clause.PropertyName, ">=", clause.Value, ref paramIndex, parameters),
                    WhereOperator.LessThan => BuildComparison(clause.PropertyName, "<", clause.Value, ref paramIndex, parameters),
                    WhereOperator.LessThanOrEqual => BuildComparison(clause.PropertyName, "<=", clause.Value, ref paramIndex, parameters),
                    WhereOperator.Like => BuildComparison(clause.PropertyName, "LIKE", clause.Value, ref paramIndex, parameters),
                    WhereOperator.IsNull => $"{WrapIdentifier(clause.PropertyName)} IS NULL",
                    WhereOperator.IsNotNull => $"{WrapIdentifier(clause.PropertyName)} IS NOT NULL",
                    _ => throw new NotSupportedException($"Operator {clause.Operator} is not supported")
                };

                conditions.Add(condition);
            }

            return " WHERE " + string.Join(" AND ", conditions);
        }

        protected virtual string BuildComparison(string propertyName, string op, object? value, ref int paramIndex, System.Collections.Generic.List<DbParameter> parameters)
        {
            string paramName = $"{GetParameterPrefix()}p{paramIndex}";
            parameters.Add(CreateParameter(paramName, value));
            paramIndex++;

            return $"{WrapIdentifier(propertyName)} {op} {paramName}";
        }

        protected virtual string BuildOrderByClause(QueryComponents components)
        {
            if (!components.OrderByClauses.Any())
                return string.Empty;

            var orderBys = components.OrderByClauses
                .Select(o => $"{WrapIdentifier(o.PropertyName)} {(o.Ascending ? "ASC" : "DESC")}");

            return " ORDER BY " + string.Join(", ", orderBys);
        }

        protected abstract string BuildLimitClause(QueryComponents components);
    }
}
