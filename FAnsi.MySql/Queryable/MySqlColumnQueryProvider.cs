using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.QueryableAbstraction;
using MySqlConnector;

namespace FAnsi.Implementations.MySql.Queryable;

/// <summary>
/// Query provider for listing MySQL columns with server-side filtering.
/// </summary>
public sealed class MySqlColumnQueryProvider : IQueryProvider
{
    private readonly MySqlQueryBuilder _queryBuilder;
    private readonly DiscoveredTable _discoveredTable;
    private readonly IManagedConnection _connection;
    private readonly string _database;

    public MySqlColumnQueryProvider(
        MySqlQueryBuilder queryBuilder,
        DiscoveredTable discoveredTable,
        IManagedConnection connection,
        string database)
    {
        _queryBuilder = queryBuilder ?? throw new ArgumentNullException(nameof(queryBuilder));
        _discoveredTable = discoveredTable ?? throw new ArgumentNullException(nameof(discoveredTable));
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _database = database ?? throw new ArgumentNullException(nameof(database));
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        return new FAnsiQueryable<TElement>(this, expression);
    }

    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = "IQueryProvider inherently requires dynamic code for generic type creation")]
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
        var tableName = _discoveredTable.GetRuntimeName();
        var sql = _queryBuilder.BuildListColumnsQuery(components, tableName, null, _database, out var dbParams);

        // Execute query
        var results = new List<DiscoveredColumn>();

        using var cmd = new MySqlCommand(sql, (MySqlConnection)_connection.Connection);
        cmd.Transaction = _connection.Transaction as MySqlTransaction;

        foreach (var param in dbParams)
        {
            cmd.Parameters.Add(param);
        }

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var columnName = reader["column_name"] as string;
            if (columnName != null)
            {
                var isNullable = YesNoToBool(reader["is_nullable"]);
                var column = new DiscoveredColumn(_discoveredTable, columnName, isNullable);

                var isPrimaryKey = reader["is_primary_key"];
                if (isPrimaryKey != DBNull.Value)
                {
                    column.IsPrimaryKey = Convert.ToBoolean(isPrimaryKey);
                }

                var isIdentity = reader["is_identity"];
                if (isIdentity != DBNull.Value)
                {
                    column.IsAutoIncrement = Convert.ToBoolean(isIdentity);
                }

                column.Collation = reader["collation"] as string;
                column.DataType = new DiscoveredDataType(reader, TrimIntDisplayValues(reader["data_type"]?.ToString()), column);

                results.Add(column);
            }
        }

        return results;
    }

    private static bool YesNoToBool(object o)
    {
        if (o is bool b)
            return b;

        if (o == null || o == DBNull.Value)
            return false;

        return o.ToString() switch
        {
            "NO" => false,
            "YES" => true,
            _ => Convert.ToBoolean(o)
        };
    }

    private static string TrimIntDisplayValues(string? type)
    {
        if (string.IsNullOrWhiteSpace(type))
            return "";

        // MySQL int(N) display widths are deprecated and should be trimmed
        if (System.Text.RegularExpressions.Regex.IsMatch(type, @"^int\(\d+\)", System.Text.RegularExpressions.RegexOptions.IgnoreCase))
            return System.Text.RegularExpressions.Regex.Replace(type, @"^int\(\d+\)", "int", System.Text.RegularExpressions.RegexOptions.IgnoreCase);

        if (System.Text.RegularExpressions.Regex.IsMatch(type, @"^smallint\(\d+\)", System.Text.RegularExpressions.RegexOptions.IgnoreCase))
            return System.Text.RegularExpressions.Regex.Replace(type, @"^smallint\(\d+\)", "smallint", System.Text.RegularExpressions.RegexOptions.IgnoreCase);

        if (System.Text.RegularExpressions.Regex.IsMatch(type, @"^bit\(\d+\)", System.Text.RegularExpressions.RegexOptions.IgnoreCase))
            return System.Text.RegularExpressions.Regex.Replace(type, @"^bit\(\d+\)", "bit", System.Text.RegularExpressions.RegexOptions.IgnoreCase);

        return type;
    }
}
