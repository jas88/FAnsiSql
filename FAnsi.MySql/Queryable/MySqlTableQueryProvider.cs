using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using FAnsi.Discovery;
using FAnsi.Discovery.QueryableAbstraction;
using FAnsi.Discovery.QuerySyntax;
using MySqlConnector;

namespace FAnsi.Implementations.MySql.Queryable;

/// <summary>
/// Query provider for listing MySQL tables with server-side filtering.
/// </summary>
public sealed class MySqlTableQueryProvider : IQueryProvider
{
    private readonly MySqlQueryBuilder _queryBuilder;
    private readonly DbConnection _connection;
    private readonly DiscoveredDatabase _parent;
    private readonly IQuerySyntaxHelper _querySyntaxHelper;
    private readonly string _database;
    private readonly bool _includeViews;
    private readonly DbTransaction? _transaction;

    public MySqlTableQueryProvider(
        MySqlQueryBuilder queryBuilder,
        DbConnection connection,
        DiscoveredDatabase parent,
        IQuerySyntaxHelper querySyntaxHelper,
        string database,
        bool includeViews,
        DbTransaction? transaction)
    {
        _queryBuilder = queryBuilder ?? throw new ArgumentNullException(nameof(queryBuilder));
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        _querySyntaxHelper = querySyntaxHelper ?? throw new ArgumentNullException(nameof(querySyntaxHelper));
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _includeViews = includeViews;
        _transaction = transaction;
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
        var sql = _queryBuilder.BuildListTablesQuery(components, _database, _includeViews, out var dbParams);

        // Execute query
        var results = new List<DiscoveredTable>();

        using var cmd = new MySqlCommand(sql, (MySqlConnection)_connection);
        cmd.Transaction = _transaction as MySqlTransaction;

        foreach (var param in dbParams)
        {
            cmd.Parameters.Add(param);
        }

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var schema = reader["table_schema"] as string;
            var tableName = reader["table_name"] as string;
            var tableType = reader["table_type"] as string;

            if (tableName != null && _querySyntaxHelper.IsValidTableName(tableName, out _))
            {
                var type = tableType == "VIEW" ? TableType.View : TableType.Table;
                results.Add(new DiscoveredTable(_parent, tableName, _querySyntaxHelper, schema, type));
            }
        }

        return results;
    }
}
