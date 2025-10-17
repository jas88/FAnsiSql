using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using FAnsi.Discovery.QueryableAbstraction;
using Npgsql;

namespace FAnsi.Implementations.PostgreSql.Queryable;

/// <summary>
/// Query provider for listing PostgreSQL databases with server-side filtering.
/// </summary>
public sealed class PostgreSqlDatabaseQueryProvider : IQueryProvider
{
    private readonly PostgreSqlQueryBuilder _queryBuilder;
    private readonly DbConnection _connection;

    public PostgreSqlDatabaseQueryProvider(
        PostgreSqlQueryBuilder queryBuilder,
        DbConnection connection)
    {
        _queryBuilder = queryBuilder ?? throw new ArgumentNullException(nameof(queryBuilder));
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        return new FAnsiQueryable<TElement>(this, expression);
    }

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

        // Build SQL query
        var sql = _queryBuilder.BuildListDatabasesQuery(components, out var dbParams);

        // Execute query
        var results = new List<string>();

        using var cmd = new NpgsqlCommand(sql, (NpgsqlConnection)_connection);

        foreach (var param in dbParams)
        {
            cmd.Parameters.Add(param);
        }

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var databaseName = reader["datname"] as string;
            if (databaseName != null)
            {
                results.Add(databaseName);
            }
        }

        return results;
    }
}
