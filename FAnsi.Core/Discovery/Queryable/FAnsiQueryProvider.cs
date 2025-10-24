using System;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Diagnostics.CodeAnalysis;

namespace FAnsi.Discovery.QueryableAbstraction
{
    /// <summary>
    /// IQueryProvider implementation that translates LINQ expression trees to SQL
    /// and executes them against the database via DBMS-specific query builders.
    /// </summary>
    /// <example>
    /// <code>
    /// var connection = server.GetConnection();
    /// connection.Open();
    /// try
    /// {
    ///     var provider = new FAnsiQueryProvider(connection, builder, "dbo.Patients");
    ///     var queryable = new FAnsiQueryable&lt;Patient&gt;(provider);
    ///     var results = queryable.Where(p => p.Age &gt; 18).OrderBy(p => p.Name).Take(10).ToList();
    /// }
    /// finally
    /// {
    ///     connection.Close();
    /// }
    /// </code>
    /// </example>
    public sealed class FAnsiQueryProvider : IQueryProvider
    {
        private readonly DbConnection _connection;
        private readonly ISqlQueryBuilder _queryBuilder;
        private readonly string _tableName;
        private readonly string? _columnName;

        /// <summary>
        /// Creates a query provider for querying an entire table.
        /// </summary>
        /// <param name="connection">Active database connection (caller owns lifetime)</param>
        /// <param name="queryBuilder">DBMS-specific SQL builder</param>
        /// <param name="tableName">Fully qualified table name</param>
        public FAnsiQueryProvider(DbConnection connection, ISqlQueryBuilder queryBuilder, string tableName)
            : this(connection, queryBuilder, tableName, null)
        {
        }

        /// <summary>
        /// Creates a query provider for querying a specific column.
        /// </summary>
        /// <param name="connection">Active database connection (caller owns lifetime)</param>
        /// <param name="queryBuilder">DBMS-specific SQL builder</param>
        /// <param name="tableName">Fully qualified table name</param>
        /// <param name="columnName">Column name to query</param>
        public FAnsiQueryProvider(DbConnection connection, ISqlQueryBuilder queryBuilder, string tableName, string? columnName)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _queryBuilder = queryBuilder ?? throw new ArgumentNullException(nameof(queryBuilder));
            _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            _columnName = columnName;
        }

        /// <summary>
        /// Creates a queryable instance for the specified element type.
        /// </summary>
        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            if (!typeof(IQueryable<TElement>).IsAssignableFrom(expression.Type))
            {
                throw new ArgumentException($"Expression must be assignable to IQueryable<{typeof(TElement).Name}>", nameof(expression));
            }

            return new FAnsiQueryable<TElement>(this, expression);
        }

        /// <summary>
        /// Creates a non-generic queryable instance.
        /// </summary>
        [RequiresDynamicCode("Calls System.Linq.Expressions.Expression.Lambda(Expression, params ParameterExpression[]).")]
        public IQueryable CreateQuery(Expression expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            Type elementType = expression.Type.GetGenericArguments()[0];

            try
            {
                return (IQueryable)Activator.CreateInstance(
                    typeof(FAnsiQueryable<>).MakeGenericType(elementType),
                    new object[] { this, expression })!;
            }
            catch (TargetInvocationException ex)
            {
                throw ex.InnerException ?? ex;
            }
        }

        /// <summary>
        /// Executes a query and returns a strongly-typed result.
        /// </summary>
        [RequiresDynamicCode()]
        public TResult Execute<TResult>(Expression expression)
        {
            return (TResult)Execute(expression);
        }

        /// <summary>
        /// Executes a query and returns the result.
        /// This is where expression trees are translated to SQL and executed.
        /// </summary>
        [RequiresDynamicCode("Calls System.Type.MakeGenericType(params Type[])")]
        public object Execute(Expression expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            // Translate expression tree to QueryComponents
            var visitor = new FAnsiExpressionVisitor();
            QueryComponents components;

            try
            {
                components = visitor.Translate(expression);
            }
            catch (NotSupportedException ex)
            {
                // Unsupported operation - fall back to client-side evaluation
                throw new InvalidOperationException(
                    "The LINQ query contains operations that cannot be translated to SQL. " +
                    "Consider breaking the query into server-side and client-side portions. " +
                    "For example: var serverData = query.Where(serverPredicate).ToList(); " +
                    "var clientFiltered = serverData.Where(clientPredicate);",
                    ex);
            }

            // Build SQL query
            string sql;
            DbParameter[] parameters;

            try
            {
                sql = _columnName != null
                    ? _queryBuilder.BuildColumnQuery(_tableName, _columnName, components, out parameters)
                    : _queryBuilder.BuildTableQuery(_tableName, components, out parameters);
            }
            catch (NotSupportedException ex)
            {
                throw new InvalidOperationException(
                    $"The query cannot be executed on this database system: {ex.Message}",
                    ex);
            }

            // Determine result type
            Type elementType = GetElementType(expression.Type);

            // Execute query and materialize results
            return ExecuteQuery(sql, parameters, elementType);
        }

        [RequiresDynamicCode("Calls System.Type.MakeGenericType(params Type[])")]
        private object ExecuteQuery(string sql, DbParameter[] parameters, Type elementType)
        {
            if (_connection.State != System.Data.ConnectionState.Open)
            {
                throw new InvalidOperationException(
                    "Database connection must be open before executing queries. " +
                    "The caller is responsible for opening and closing the connection.");
            }

            using var command = _connection.CreateCommand();
            command.CommandText = sql;

            foreach (var param in parameters)
            {
                command.Parameters.Add(param);
            }

            using var reader = command.ExecuteReader();

            // Create a list of the appropriate type
            var listType = typeof(List<>).MakeGenericType(elementType);
            var results = (IList)Activator.CreateInstance(listType)!;

            // Materialize results
            while (reader.Read())
            {
                var item = MaterializeRow(reader, elementType);
                results.Add(item);
            }

            return results;
        }

        private object MaterializeRow(DbDataReader reader, Type elementType)
        {
            // For primitive types and strings, just read the first column
            if (elementType.IsPrimitive || elementType == typeof(string) || elementType == typeof(decimal) ||
                elementType == typeof(DateTime) || elementType == typeof(Guid))
            {
                var value = reader.GetValue(0);
                return value == DBNull.Value ? GetDefault(elementType)! : value;
            }

            // For complex types, create an instance and populate properties
            var instance = Activator.CreateInstance(elementType)!;
            var properties = elementType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanWrite)
                .ToDictionary(p => p.Name, StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < reader.FieldCount; i++)
            {
                var columnName = reader.GetName(i);

                if (properties.TryGetValue(columnName, out var property))
                {
                    var value = reader.GetValue(i);

                    if (value != DBNull.Value)
                    {
                        // Handle nullable types
                        var targetType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

                        try
                        {
                            var convertedValue = Convert.ChangeType(value, targetType);
                            property.SetValue(instance, convertedValue);
                        }
                        catch (Exception ex)
                        {
                            throw new InvalidOperationException(
                                $"Failed to convert column '{columnName}' value '{value}' to type '{property.PropertyType.Name}'.",
                                ex);
                        }
                    }
                }
            }

            return instance;
        }

        private static Type GetElementType(Type type)
        {
            // Handle IQueryable<T>
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IQueryable<>))
            {
                return type.GetGenericArguments()[0];
            }

            // Handle IEnumerable<T>
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                return type.GetGenericArguments()[0];
            }

            // Handle arrays
            if (type.IsArray)
            {
                return type.GetElementType()!;
            }

            // Try to find IEnumerable<T> in interfaces
            var enumerable = type.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));

            if (enumerable != null)
            {
                return enumerable.GetGenericArguments()[0];
            }

            return type;
        }

        private static object? GetDefault(Type type)
        {
            return type.IsValueType ? Activator.CreateInstance(type) : null;
        }
    }
}
