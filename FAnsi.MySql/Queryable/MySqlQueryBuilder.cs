using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using FAnsi.Discovery.QueryableAbstraction;
using MySqlConnector;

namespace FAnsi.Implementations.MySql.Queryable;

/// <summary>
/// Builds parameterized MySQL queries from QueryComponents for IQueryable support.
/// Translates WHERE, ORDER BY, LIMIT/OFFSET operations to MySQL SQL while preventing SQL injection.
/// </summary>
public sealed class MySqlQueryBuilder : SqlQueryBuilderBase
{
    public override string WrapIdentifier(string identifier)
    {
        // MySQL uses backticks for identifiers
        return $"`{identifier}`";
    }

    public override string GetParameterPrefix()
    {
        // MySQL uses @ prefix for parameters
        return "@";
    }

    protected override DbParameter CreateParameter(string name, object? value)
    {
        return new MySqlParameter(name, value ?? DBNull.Value);
    }

    protected override string BuildSelectClause(QueryComponents components)
    {
        // MySQL doesn't use TOP - it uses LIMIT at the end
        return "SELECT *";
    }

    protected override string BuildLimitClause(QueryComponents components)
    {
        var sb = new StringBuilder();

        // MySQL uses LIMIT with optional OFFSET syntax: LIMIT [offset,] row_count
        if (components.Take.HasValue && components.Skip.HasValue)
        {
            // LIMIT offset, count
            sb.Append($" LIMIT {components.Skip.Value}, {components.Take.Value}");
        }
        else if (components.Take.HasValue)
        {
            // LIMIT count only
            sb.Append($" LIMIT {components.Take.Value}");
        }
        else if (components.Skip.HasValue)
        {
            // Skip without take - use a large number
            sb.Append($" LIMIT {components.Skip.Value}, 18446744073709551615");
        }

        return sb.ToString();
    }

    /// <summary>
    /// Builds a parameterized query to list tables from INFORMATION_SCHEMA.TABLES
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

        // Base SELECT query using INFORMATION_SCHEMA
        sql.AppendLine("SELECT table_schema, table_name, table_type");
        sql.AppendLine("FROM information_schema.TABLES");

        // Start WHERE clause with system schema exclusion
        sql.AppendLine("WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')");

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

        // Add LIMIT for pagination
        if (components.Take.HasValue || components.Skip.HasValue)
        {
            if (components.Take.HasValue && components.Skip.HasValue)
            {
                sql.AppendLine($"LIMIT {components.Skip.Value}, {components.Take.Value}");
            }
            else if (components.Take.HasValue)
            {
                sql.AppendLine($"LIMIT {components.Take.Value}");
            }
            else if (components.Skip.HasValue)
            {
                sql.AppendLine($"LIMIT {components.Skip.Value}, 18446744073709551615");
            }
        }

        parameters = paramList.ToArray();
        return sql.ToString();
    }

    /// <summary>
    /// Builds a parameterized query to list columns from INFORMATION_SCHEMA.COLUMNS
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

        var sql = new StringBuilder();
        var paramList = new List<DbParameter>();

        // Base query using INFORMATION_SCHEMA.COLUMNS
        sql.AppendLine(@"SELECT
    COLUMN_NAME as column_name,
    DATA_TYPE as data_type,
    CHARACTER_MAXIMUM_LENGTH as max_length,
    NUMERIC_PRECISION as `precision`,
    NUMERIC_SCALE as scale,
    IS_NULLABLE as is_nullable,
    CASE WHEN COLUMN_KEY = 'PRI' THEN 1 ELSE 0 END as is_primary_key,
    CASE WHEN EXTRA = 'auto_increment' THEN 1 ELSE 0 END as is_identity,
    COLLATION_NAME as collation
FROM information_schema.COLUMNS
WHERE table_schema = @database
  AND table_name = @tableName");

        // Add schema and table parameters
        paramList.Add(new MySqlParameter("@database", database));
        paramList.Add(new MySqlParameter("@tableName", tableName));

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

        // Add LIMIT for pagination
        if (components.Take.HasValue || components.Skip.HasValue)
        {
            if (components.Take.HasValue && components.Skip.HasValue)
            {
                sql.AppendLine($"LIMIT {components.Skip.Value}, {components.Take.Value}");
            }
            else if (components.Take.HasValue)
            {
                sql.AppendLine($"LIMIT {components.Take.Value}");
            }
            else if (components.Skip.HasValue)
            {
                sql.AppendLine($"LIMIT {components.Skip.Value}, 18446744073709551615");
            }
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
        sql.AppendLine("SELECT SCHEMA_NAME");
        sql.AppendLine("FROM information_schema.SCHEMATA");

        // Start WHERE clause with system schema exclusion
        sql.AppendLine("WHERE SCHEMA_NAME NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')");

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
                .Select(o => $"{MapPropertyToDatabaseColumn(o.PropertyName)} {(o.Ascending ? "ASC" : "DESC")}");
            sql.AppendLine($"ORDER BY {string.Join(", ", orderByParts)}");
        }

        // Add LIMIT for pagination
        if (components.Take.HasValue || components.Skip.HasValue)
        {
            if (components.Take.HasValue && components.Skip.HasValue)
            {
                sql.AppendLine($"LIMIT {components.Skip.Value}, {components.Take.Value}");
            }
            else if (components.Take.HasValue)
            {
                sql.AppendLine($"LIMIT {components.Take.Value}");
            }
            else if (components.Skip.HasValue)
            {
                sql.AppendLine($"LIMIT {components.Skip.Value}, 18446744073709551615");
            }
        }

        parameters = paramList.ToArray();
        return sql.ToString();
    }

    /// <summary>
    /// Translates a WHERE clause to parameterized MySQL SQL
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

        parameters.Add(new MySqlParameter(paramName, value));
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

        parameters.Add(new MySqlParameter(paramName, value));
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

        parameters.Add(new MySqlParameter(paramName, value));
        return $"{columnName} {op} {paramName}";
    }

    /// <summary>
    /// Builds a LIKE condition for pattern matching
    /// MySQL is case-insensitive by default for LIKE (depends on collation)
    /// </summary>
    private string BuildLikeCondition(string columnName, string paramName, object? value, List<DbParameter> parameters)
    {
        if (value == null)
        {
            throw new NotSupportedException("LIKE operator requires a non-null value");
        }

        parameters.Add(new MySqlParameter(paramName, value));
        return $"{columnName} LIKE {paramName}";
    }

    /// <summary>
    /// Maps C# property names to information_schema.TABLES column names
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
    /// Maps C# property names to information_schema.COLUMNS column names
    /// </summary>
    private string MapPropertyToColumnField(string propertyName)
    {
        return propertyName switch
        {
            "Name" or "ColumnName" => "COLUMN_NAME",
            "DataType" or "Type" => "DATA_TYPE",
            "IsNullable" => "IS_NULLABLE",
            "IsPrimaryKey" => "CASE WHEN COLUMN_KEY = 'PRI' THEN 1 ELSE 0 END",
            "IsIdentity" or "IsAutoIncrement" => "CASE WHEN EXTRA = 'auto_increment' THEN 1 ELSE 0 END",
            "MaxLength" or "CharacterMaximumLength" => "CHARACTER_MAXIMUM_LENGTH",
            "Precision" or "NumericPrecision" => "NUMERIC_PRECISION",
            "Scale" or "NumericScale" => "NUMERIC_SCALE",
            "Collation" or "CollationName" => "COLLATION_NAME",
            _ => propertyName.ToUpperInvariant()
        };
    }

    /// <summary>
    /// Maps C# property names to information_schema.SCHEMATA column names
    /// </summary>
    private string MapPropertyToDatabaseColumn(string propertyName)
    {
        return propertyName switch
        {
            "Name" or "DatabaseName" or "Database" => "SCHEMA_NAME",
            _ => propertyName.ToUpperInvariant()
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
        if (columnField != propertyName.ToUpperInvariant())
            return columnField;

        var databaseColumn = MapPropertyToDatabaseColumn(propertyName);
        if (databaseColumn != propertyName.ToUpperInvariant())
            return databaseColumn;

        return propertyName.ToLowerInvariant();
    }
}
