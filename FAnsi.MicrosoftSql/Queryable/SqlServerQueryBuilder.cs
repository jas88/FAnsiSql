using System;
using System.Data.Common;
using System.Linq;
using FAnsi.Discovery.QueryableAbstraction;
using Microsoft.Data.SqlClient;

namespace FAnsi.Implementations.MicrosoftSQL.Queryable;

/// <summary>
/// SQL Server-specific implementation of ISqlQueryBuilder.
/// Translates QueryComponents into T-SQL with proper parameterization and SQL Server-specific syntax.
/// </summary>
public sealed class SqlServerQueryBuilder : ISqlQueryBuilder
{
    /// <summary>
    /// Wraps an identifier in SQL Server delimiters ([]).
    /// </summary>
    public string WrapIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentNullException(nameof(identifier));

        // Already wrapped
        if (identifier.StartsWith("[") && identifier.EndsWith("]"))
            return identifier;

        return $"[{identifier.Replace("]", "]]")}]";
    }

    /// <summary>
    /// Gets the parameter prefix for SQL Server (@).
    /// </summary>
    public string GetParameterPrefix() => "@";

    /// <summary>
    /// Builds a SELECT query for an entire table.
    /// Translates INFORMATION_SCHEMA.TABLES or sys.* queries for FAnsi's table listing.
    /// </summary>
    public string BuildTableQuery(string tableName, QueryComponents components, out DbParameter[] parameters)
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

    /// <summary>
    /// Builds a SELECT query for a specific column.
    /// </summary>
    public string BuildColumnQuery(string tableName, string columnName, QueryComponents components, out DbParameter[] parameters)
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

    /// <summary>
    /// Builds the SELECT clause with TOP if applicable (SQL Server 2008 compatibility).
    /// </summary>
    private string BuildSelectClause(QueryComponents components)
    {
        // Use TOP if only Take is specified (no Skip) - SQL Server 2008 compatible
        if (components.Take.HasValue && !components.Skip.HasValue)
            return $"SELECT TOP {components.Take.Value} *";

        return "SELECT *";
    }

    /// <summary>
    /// Builds the WHERE clause with parameterized conditions.
    /// </summary>
    private string BuildWhereClause(QueryComponents components, System.Collections.Generic.List<DbParameter> paramList)
    {
        if (!components.WhereClauses.Any())
            return string.Empty;

        var conditions = new System.Collections.Generic.List<string>();
        int paramIndex = 0;

        foreach (var clause in components.WhereClauses)
        {
            string condition = clause.Operator switch
            {
                WhereOperator.Equal => BuildEqualityCondition(clause.PropertyName, clause.Value, ref paramIndex, paramList),
                WhereOperator.NotEqual => BuildInequalityCondition(clause.PropertyName, clause.Value, ref paramIndex, paramList),
                WhereOperator.GreaterThan => BuildComparisonCondition(clause.PropertyName, ">", clause.Value, ref paramIndex, paramList),
                WhereOperator.GreaterThanOrEqual => BuildComparisonCondition(clause.PropertyName, ">=", clause.Value, ref paramIndex, paramList),
                WhereOperator.LessThan => BuildComparisonCondition(clause.PropertyName, "<", clause.Value, ref paramIndex, paramList),
                WhereOperator.LessThanOrEqual => BuildComparisonCondition(clause.PropertyName, "<=", clause.Value, ref paramIndex, paramList),
                WhereOperator.Like => BuildLikeCondition(clause.PropertyName, clause.Value, ref paramIndex, paramList),
                WhereOperator.IsNull => $"{WrapIdentifier(clause.PropertyName)} IS NULL",
                WhereOperator.IsNotNull => $"{WrapIdentifier(clause.PropertyName)} IS NOT NULL",
                _ => throw new NotSupportedException($"Operator '{clause.Operator}' is not supported")
            };

            conditions.Add(condition);
        }

        return " WHERE " + string.Join(" AND ", conditions);
    }

    /// <summary>
    /// Builds an equality condition with proper NULL handling.
    /// </summary>
    private string BuildEqualityCondition(string propertyName, object? value, ref int paramIndex, System.Collections.Generic.List<DbParameter> paramList)
    {
        if (value == null)
            return $"{WrapIdentifier(propertyName)} IS NULL";

        var paramName = $"@p{paramIndex++}";
        paramList.Add(new SqlParameter(paramName, value));
        return $"{WrapIdentifier(propertyName)} = {paramName}";
    }

    /// <summary>
    /// Builds an inequality condition with proper NULL handling.
    /// </summary>
    private string BuildInequalityCondition(string propertyName, object? value, ref int paramIndex, System.Collections.Generic.List<DbParameter> paramList)
    {
        if (value == null)
            return $"{WrapIdentifier(propertyName)} IS NOT NULL";

        var paramName = $"@p{paramIndex++}";
        paramList.Add(new SqlParameter(paramName, value));
        return $"{WrapIdentifier(propertyName)} <> {paramName}";
    }

    /// <summary>
    /// Builds a comparison condition (&lt;, &gt;, &lt;=, &gt;=).
    /// </summary>
    private string BuildComparisonCondition(string propertyName, string op, object? value, ref int paramIndex, System.Collections.Generic.List<DbParameter> paramList)
    {
        if (value == null)
            throw new NotSupportedException($"Cannot use {op} operator with NULL value");

        var paramName = $"@p{paramIndex++}";
        paramList.Add(new SqlParameter(paramName, value));
        return $"{WrapIdentifier(propertyName)} {op} {paramName}";
    }

    /// <summary>
    /// Builds a LIKE condition with proper escaping.
    /// </summary>
    private string BuildLikeCondition(string propertyName, object? value, ref int paramIndex, System.Collections.Generic.List<DbParameter> paramList)
    {
        if (value == null)
            throw new NotSupportedException("LIKE operator requires a non-null value");

        var paramName = $"@p{paramIndex++}";
        // Escape LIKE wildcards in the value
        var escapedValue = value.ToString()!
            .Replace("[", "[[]")
            .Replace("%", "[%]")
            .Replace("_", "[_]");
        paramList.Add(new SqlParameter(paramName, escapedValue));
        return $"{WrapIdentifier(propertyName)} LIKE {paramName}";
    }

    /// <summary>
    /// Builds the ORDER BY clause.
    /// </summary>
    private string BuildOrderByClause(QueryComponents components)
    {
        if (!components.OrderByClauses.Any())
            return string.Empty;

        var orderBys = components.OrderByClauses
            .Select(o => $"{WrapIdentifier(o.PropertyName)} {(o.Ascending ? "ASC" : "DESC")}");

        return " ORDER BY " + string.Join(", ", orderBys);
    }

    /// <summary>
    /// Builds the LIMIT/OFFSET clause using SQL Server OFFSET/FETCH syntax (SQL Server 2012+).
    /// Falls back to TOP for simple cases handled in BuildSelectClause.
    /// </summary>
    private string BuildLimitClause(QueryComponents components)
    {
        // If using OFFSET/FETCH (SQL Server 2012+)
        if (components.Skip.HasValue || (components.Take.HasValue && components.Skip.HasValue))
        {
            // OFFSET/FETCH requires ORDER BY
            if (!components.OrderByClauses.Any())
                throw new InvalidOperationException(
                    "SQL Server requires an ORDER BY clause when using OFFSET/FETCH. " +
                    "Add .OrderBy() to your query before using .Skip() or .Take().");

            var offset = components.Skip ?? 0;
            var sql = $" OFFSET {offset} ROWS";

            if (components.Take.HasValue)
                sql += $" FETCH NEXT {components.Take.Value} ROWS ONLY";

            return sql;
        }

        // Simple TOP case handled in SELECT clause
        return string.Empty;
    }
}
