using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using FAnsi.Discovery;
using FAnsi.Discovery.QueryableAbstraction;
using Npgsql;

namespace FAnsi.Implementations.PostgreSql.Queryable;

/// <summary>
/// Query provider for discovering PostgreSQL table columns with server-side filtering.
/// </summary>
public sealed class PostgreSqlColumnQueryProvider : IQueryProvider
{
    private readonly PostgreSqlQueryBuilder _queryBuilder;
    private readonly DbConnection _connection;
    private readonly DiscoveredTable _discoveredTable;
    private readonly string _database;
    private readonly DbTransaction? _transaction;

    public PostgreSqlColumnQueryProvider(
        PostgreSqlQueryBuilder queryBuilder,
        DbConnection connection,
        DiscoveredTable discoveredTable,
        string database,
        DbTransaction? transaction)
    {
        _queryBuilder = queryBuilder ?? throw new ArgumentNullException(nameof(queryBuilder));
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _discoveredTable = discoveredTable ?? throw new ArgumentNullException(nameof(discoveredTable));
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _transaction = transaction;
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        return new FAnsiQueryable<TElement>(this, expression);
    }

    [RequiresDynamicCode()]
    public IQueryable CreateQuery(Expression expression)
    {
        var elementType = expression.Type.GetGenericArguments()[0];
        var queryableType = typeof(FAnsiQueryable<>).MakeGenericType(elementType);
        return (IQueryable)Activator.CreateInstance(queryableType, this, expression)!;
    }

    public TResult Execute<TResult>(Expression expression)
    {
        return (TResult)Execute(expression);
    }

    public object Execute(Expression expression)
    {
        // Translate expression tree to QueryComponents
        var visitor = new FAnsiExpressionVisitor();
        var components = visitor.Translate(expression);

        // Get primary keys first
        var pks = _discoveredTable is DiscoveredTableValuedFunction
            ? null
            : ListPrimaryKeys().ToHashSet();

        // Build SQL query
        var schema = string.IsNullOrWhiteSpace(_discoveredTable.Schema)
            ? PostgreSqlSyntaxHelper.DefaultPostgresSchema
            : _discoveredTable.Schema;

        var sql = _queryBuilder.BuildListColumnsQuery(
            components,
            _discoveredTable.GetRuntimeName(),
            schema,
            _database,
            out var dbParams);

        // Execute query
        var results = new List<DiscoveredColumn>();

        using var cmd = new NpgsqlCommand(sql, (NpgsqlConnection)_connection);
        cmd.Transaction = _transaction as NpgsqlTransaction;

        foreach (var param in dbParams)
        {
            cmd.Parameters.Add(param);
        }

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var isNullable = Equals(reader["is_nullable"], "YES");

            var columnName = _discoveredTable is DiscoveredTableValuedFunction
                ? $"{_discoveredTable.GetRuntimeName()}.{reader["column_name"]}"
                : reader["column_name"].ToString();

            if (columnName == null) continue;

            var column = new DiscoveredColumn(_discoveredTable, columnName, isNullable)
            {
                IsAutoIncrement = Equals(reader["is_identity"], "YES"),
                Collation = reader["collation_name"] as string
            };

            column.DataType = new DiscoveredDataType(reader, GetSQLType(reader), column);
            column.IsPrimaryKey = pks?.Contains(column.GetRuntimeName()) ?? false;

            results.Add(column);
        }

        return results;
    }

    private IEnumerable<string> ListPrimaryKeys()
    {
        const string query = """
                             SELECT
                                         pg_attribute.attname,
                                         format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
                                         FROM pg_index, pg_class, pg_attribute
                                         WHERE
                                         pg_class.oid = @tableName::regclass AND
                                             indrelid = pg_class.oid AND
                                         pg_attribute.attrelid = pg_class.oid AND
                                         pg_attribute.attnum = any(pg_index.indkey)
                                         AND indisprimary
                             """;

        using var cmd = new NpgsqlCommand(query, (NpgsqlConnection)_connection);
        cmd.Transaction = _transaction as NpgsqlTransaction;

        var p = cmd.CreateParameter();
        p.ParameterName = "@tableName";
        p.Value = _discoveredTable.GetFullyQualifiedName();
        cmd.Parameters.Add(p);

        using var r = cmd.ExecuteReader();
        while (r.Read())
        {
            yield return (string)r["attname"];
        }
    }

    private static string GetSQLType(DbDataReader r)
    {
        var columnType = r["data_type"] as string;
        var lengthQualifier = "";

        if (HasPrecisionAndScale(columnType ?? string.Empty))
        {
            lengthQualifier = $"({r["numeric_precision"]},{r["numeric_scale"]})";
        }
        else if (r["character_maximum_length"] != DBNull.Value)
        {
            lengthQualifier = $"({Convert.ToInt32(r["character_maximum_length"])})";
        }

        return columnType + lengthQualifier;
    }

    private static bool HasPrecisionAndScale(string sqlType)
    {
        return sqlType.ToLowerInvariant() is "numeric" or "decimal";
    }
}
