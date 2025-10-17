using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using FAnsi.Discovery.QueryableAbstraction;
using Npgsql;

namespace FAnsi.Implementations.PostgreSql.Queryable;

/// <summary>
/// Builds parameterized PostgreSQL queries from QueryComponents for IQueryable support.
/// Translates WHERE, ORDER BY, LIMIT/OFFSET operations to PostgreSQL SQL while preventing SQL injection.
/// </summary>
public sealed class PostgreSqlQueryBuilder : SqlQueryBuilderBase
{
    public override string WrapIdentifier(string identifier)
    {
        // PostgreSQL uses double quotes for identifiers
        return $"\"{identifier}\"";
    }

    public override string GetParameterPrefix()
    {
        // PostgreSQL uses @ prefix for parameters
        return "@";
    }

    protected override DbParameter CreateParameter(string name, object? value)
    {
        return new NpgsqlParameter(name, value ?? DBNull.Value);
    }

    protected override string BuildSelectClause(QueryComponents components)
    {
        // PostgreSQL doesn't use TOP - it uses LIMIT at the end
        return "SELECT *";
    }

    protected override string BuildLimitClause(QueryComponents components)
    {
        var sb = new StringBuilder();

        // PostgreSQL uses LIMIT/OFFSET syntax
        if (components.Take.HasValue)
        {
            sb.Append($" LIMIT {components.Take.Value}");
        }

        if (components.Skip.HasValue)
        {
            sb.Append($" OFFSET {components.Skip.Value}");
        }

        return sb.ToString();
    }

    /// <summary>
    /// Builds a parameterized query to list tables from information_schema.tables
    /// </summary>
    public string BuildListTablesQuery(
        QueryComponents components,
        string database,
        bool includeViews,
        out DbParameter[] parameters)
    {
        if (components == null)
            throw new ArgumentNullException(nameof(components));
        if (string.IsNullOrWhiteSpace(database))
            throw new ArgumentNullException(nameof(database));

        var sql = new StringBuilder();
        var paramList = new List<DbParameter>();

        // Base SELECT query using information_schema
        sql.AppendLine("SELECT table_schema, table_name, table_type");
        sql.AppendLine("FROM information_schema.tables");

        // Start WHERE clause with system schema exclusion
        sql.AppendLine("WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')");

        // Filter by table type (BASE TABLE vs VIEW)
        if (includeViews)
        {
            sql.AppendLine("  AND table_type IN ('BASE TABLE', 'VIEW')");
        }
        else
        {
            sql.AppendLine("  AND table_type = 'BASE TABLE'");
        }

        // Add user-defined WHERE clauses
        if (components.WhereClauses?.Count > 0)
        {
            foreach (var whereClause in components.WhereClauses)
            {
                var condition = TranslateWhereClause(whereClause, paramList);
                sql.AppendLine($"  AND {condition}");
            }
        }

        // Add ORDER BY if specified
        if (components.OrderByClauses?.Count > 0)
        {
            var orderByParts = components.OrderByClauses
                .Select(o => $"{MapPropertyToTableColumn(o.PropertyName)} {(o.Ascending ? "ASC" : "DESC")}");
            sql.AppendLine($"ORDER BY {string.Join(", ", orderByParts)}");
        }

        // Add LIMIT/OFFSET for pagination
        if (components.Take.HasValue)
        {
            sql.AppendLine($"LIMIT {components.Take.Value}");
        }

        if (components.Skip.HasValue)
        {
            sql.AppendLine($"OFFSET {components.Skip.Value}");
        }

        parameters = paramList.ToArray();
        return sql.ToString();
    }

    /// <summary>
    /// Builds a parameterized query to list columns from information_schema.columns
    /// </summary>
    public string BuildListColumnsQuery(
        QueryComponents components,
        string tableName,
        string? schema,
        string database,
        out DbParameter[] parameters)
    {
        if (components == null)
            throw new ArgumentNullException(nameof(components));
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentNullException(nameof(tableName));
        if (string.IsNullOrWhiteSpace(database))
            throw new ArgumentNullException(nameof(database));

        schema ??= "public";

        var sql = new StringBuilder();
        var paramList = new List<DbParameter>();

        // Base query using information_schema.columns
        sql.AppendLine(@"SELECT
    column_name,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    is_nullable,
    is_identity,
    collation_name
FROM information_schema.columns
WHERE table_schema = @schemaName
  AND table_name = @tableName");

        // Add schema and table parameters
        paramList.Add(new NpgsqlParameter("@schemaName", schema));
        paramList.Add(new NpgsqlParameter("@tableName", tableName));

        // Add user-defined WHERE clauses
        if (components.WhereClauses?.Count > 0)
        {
            foreach (var whereClause in components.WhereClauses)
            {
                var condition = TranslateWhereClause(whereClause, paramList);
                sql.AppendLine($"  AND {condition}");
            }
        }

        // Add ORDER BY if specified
        if (components.OrderByClauses?.Count > 0)
        {
            var orderByParts = components.OrderByClauses
                .Select(o => $"{MapPropertyToColumnField(o.PropertyName)} {(o.Ascending ? "ASC" : "DESC")}");
            sql.AppendLine($"ORDER BY {string.Join(", ", orderByParts)}");
        }

        // Add LIMIT/OFFSET for pagination
        if (components.Take.HasValue)
        {
            sql.AppendLine($"LIMIT {components.Take.Value}");
        }

        if (components.Skip.HasValue)
        {
            sql.AppendLine($"OFFSET {components.Skip.Value}");
        }

        parameters = paramList.ToArray();
        return sql.ToString();
    }

    /// <summary>
    /// Builds a parameterized query to list databases
    /// </summary>
    public string BuildListDatabasesQuery(
        QueryComponents components,
        out DbParameter[] parameters)
    {
        if (components == null)
            throw new ArgumentNullException(nameof(components));

        var sql = new StringBuilder();
        var paramList = new List<DbParameter>();

        // Base SELECT query
        sql.AppendLine("SELECT datname");
        sql.AppendLine("FROM pg_database");

        // Start WHERE clause - exclude system templates
        var hasWhere = false;

        // Add user-defined WHERE clauses
        if (components.WhereClauses?.Count > 0)
        {
            foreach (var whereClause in components.WhereClauses)
            {
                var condition = TranslateWhereClause(whereClause, paramList);
                if (!hasWhere)
                {
                    sql.AppendLine($"WHERE {condition}");
                    hasWhere = true;
                }
                else
                {
                    sql.AppendLine($"  AND {condition}");
                }
            }
        }

        // Add ORDER BY if specified
        if (components.OrderByClauses?.Count > 0)
        {
            var orderByParts = components.OrderByClauses
                .Select(o => $"{MapPropertyToDatabaseColumn(o.PropertyName)} {(o.Ascending ? "ASC" : "DESC")}");
            sql.AppendLine($"ORDER BY {string.Join(", ", orderByParts)}");
        }

        // Add LIMIT/OFFSET for pagination
        if (components.Take.HasValue)
        {
            sql.AppendLine($"LIMIT {components.Take.Value}");
        }

        if (components.Skip.HasValue)
        {
            sql.AppendLine($"OFFSET {components.Skip.Value}");
        }

        parameters = paramList.ToArray();
        return sql.ToString();
    }

    /// <summary>
    /// Translates a WHERE clause to parameterized PostgreSQL SQL
    /// </summary>
    private string TranslateWhereClause(WhereClause whereClause, List<DbParameter> parameters)
    {
        var columnName = MapPropertyToColumn(whereClause.PropertyName);
        var paramName = $"@p{parameters.Count}";

        return whereClause.Operator switch
        {
            WhereOperator.Equal => BuildEqualityCondition(columnName, paramName, whereClause.Value, parameters),
            WhereOperator.NotEqual => BuildInequalityCondition(columnName, paramName, whereClause.Value, parameters),
            WhereOperator.LessThan => BuildComparisonCondition(columnName, "<", paramName, whereClause.Value, parameters),
            WhereOperator.GreaterThan => BuildComparisonCondition(columnName, ">", paramName, whereClause.Value, parameters),
            WhereOperator.LessThanOrEqual => BuildComparisonCondition(columnName, "<=", paramName, whereClause.Value, parameters),
            WhereOperator.GreaterThanOrEqual => BuildComparisonCondition(columnName, ">=", paramName, whereClause.Value, parameters),
            WhereOperator.Like => BuildLikeCondition(columnName, paramName, whereClause.Value, parameters),
            WhereOperator.IsNull => $"{columnName} IS NULL",
            WhereOperator.IsNotNull => $"{columnName} IS NOT NULL",
            _ => throw new NotSupportedException($"Operator '{whereClause.Operator}' is not supported")
        };
    }

    /// <summary>
    /// Builds an equality condition with proper NULL handling
    /// </summary>
    private string BuildEqualityCondition(string columnName, string paramName, object? value, List<DbParameter> parameters)
    {
        if (value == null)
        {
            return $"{columnName} IS NULL";
        }

        parameters.Add(new NpgsqlParameter(paramName, value));
        return $"{columnName} = {paramName}";
    }

    /// <summary>
    /// Builds an inequality condition with proper NULL handling
    /// </summary>
    private string BuildInequalityCondition(string columnName, string paramName, object? value, List<DbParameter> parameters)
    {
        if (value == null)
        {
            return $"{columnName} IS NOT NULL";
        }

        parameters.Add(new NpgsqlParameter(paramName, value));
        return $"{columnName} != {paramName}";
    }

    /// <summary>
    /// Builds a comparison condition (&lt;, &gt;, &lt;=, &gt;=)
    /// </summary>
    private string BuildComparisonCondition(string columnName, string op, string paramName, object? value, List<DbParameter> parameters)
    {
        if (value == null)
        {
            throw new NotSupportedException($"Cannot use {op} operator with NULL value");
        }

        parameters.Add(new NpgsqlParameter(paramName, value));
        return $"{columnName} {op} {paramName}";
    }

    /// <summary>
    /// Builds a LIKE condition for pattern matching
    /// PostgreSQL is case-sensitive by default, use ILIKE for case-insensitive
    /// </summary>
    private string BuildLikeCondition(string columnName, string paramName, object? value, List<DbParameter> parameters)
    {
        if (value == null)
        {
            throw new NotSupportedException("LIKE operator requires a non-null value");
        }

        parameters.Add(new NpgsqlParameter(paramName, value));
        return $"{columnName} LIKE {paramName}";
    }

    /// <summary>
    /// Maps C# property names to information_schema.tables column names
    /// </summary>
    private string MapPropertyToTableColumn(string propertyName)
    {
        return propertyName switch
        {
            "Schema" or "TableSchema" => "table_schema",
            "Name" or "TableName" => "table_name",
            "Type" or "TableType" => "table_type",
            _ => propertyName.ToLowerInvariant()
        };
    }

    /// <summary>
    /// Maps C# property names to information_schema.columns column names
    /// </summary>
    private string MapPropertyToColumnField(string propertyName)
    {
        return propertyName switch
        {
            "Name" or "ColumnName" => "column_name",
            "DataType" or "Type" => "data_type",
            "IsNullable" => "is_nullable",
            "IsIdentity" or "IsAutoIncrement" => "is_identity",
            "MaxLength" or "CharacterMaximumLength" => "character_maximum_length",
            "Precision" or "NumericPrecision" => "numeric_precision",
            "Scale" or "NumericScale" => "numeric_scale",
            "Collation" or "CollationName" => "collation_name",
            _ => propertyName.ToLowerInvariant()
        };
    }

    /// <summary>
    /// Maps C# property names to pg_database column names
    /// </summary>
    private string MapPropertyToDatabaseColumn(string propertyName)
    {
        return propertyName switch
        {
            "Name" or "DatabaseName" => "datname",
            _ => propertyName.ToLowerInvariant()
        };
    }

    /// <summary>
    /// Maps C# property names to generic column names
    /// </summary>
    private string MapPropertyToColumn(string propertyName)
    {
        // Try each mapping in order
        var tableColumn = MapPropertyToTableColumn(propertyName);
        if (tableColumn != propertyName.ToLowerInvariant())
            return tableColumn;

        var columnField = MapPropertyToColumnField(propertyName);
        if (columnField != propertyName.ToLowerInvariant())
            return columnField;

        var databaseColumn = MapPropertyToDatabaseColumn(propertyName);
        if (databaseColumn != propertyName.ToLowerInvariant())
            return databaseColumn;

        return propertyName.ToLowerInvariant();
    }
}
