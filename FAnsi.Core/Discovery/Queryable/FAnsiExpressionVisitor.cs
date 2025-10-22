using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace FAnsi.Discovery.QueryableAbstraction
{
    /// <summary>
    /// Translates LINQ expression trees into DBMS-agnostic QueryComponents.
    /// Supports: Where, OrderBy, OrderByDescending, ThenBy, ThenByDescending, Take, Skip.
    /// Thread-safe for expression tree traversal.
    /// </summary>
    /// <example>
    /// <code>
    /// Expression&lt;Func&lt;Patient, bool&gt;&gt; predicate = p => p.Age &gt; 18 &amp;&amp; p.Name.StartsWith("J");
    /// var visitor = new FAnsiExpressionVisitor();
    /// var components = visitor.Translate(expression);
    /// // components.WhereClauses contains: Age &gt; 18, Name LIKE 'J%'
    /// </code>
    /// </example>
    public sealed class FAnsiExpressionVisitor : ExpressionVisitor
    {
        private readonly QueryComponents _components = new QueryComponents();
        private bool _isWhereClause;

        /// <summary>
        /// Translates an expression tree into QueryComponents.
        /// </summary>
        /// <param name="expression">The expression tree to translate</param>
        /// <returns>QueryComponents representing the query</returns>
        /// <exception cref="NotSupportedException">Thrown when an unsupported LINQ operation is encountered</exception>
        public QueryComponents Translate(Expression expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            Visit(expression);
            return _components;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(Queryable) || node.Method.DeclaringType == typeof(Enumerable))
            {
                switch (node.Method.Name)
                {
                    case "Where":
                        VisitWhere(node);
                        return node;

                    case "OrderBy":
                    case "ThenBy":
                        VisitOrderBy(node, ascending: true);
                        return node;

                    case "OrderByDescending":
                    case "ThenByDescending":
                        VisitOrderBy(node, ascending: false);
                        return node;

                    case "Take":
                        VisitTake(node);
                        return node;

                    case "Skip":
                        VisitSkip(node);
                        return node;

                    case "Select":
                        // Select is handled by the provider (materializes to different type)
                        // We just visit the source
                        Visit(node.Arguments[0]);
                        return node;

                    default:
                        throw new NotSupportedException(
                            $"The LINQ method '{node.Method.Name}' is not supported for server-side evaluation. " +
                            $"The query will execute client-side, which may be inefficient for large datasets.");
                }
            }

            // String methods that can be translated to SQL
            if (node.Method.DeclaringType == typeof(string))
            {
                return VisitStringMethod(node);
            }

            throw new NotSupportedException($"Method '{node.Method.Name}' on type '{node.Method.DeclaringType?.Name}' cannot be translated to SQL.");
        }

        private void VisitWhere(MethodCallExpression node)
        {
            // Visit the source first
            Visit(node.Arguments[0]);

            // Extract the predicate (second argument is the lambda)
            var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);

            _isWhereClause = true;
            Visit(lambda.Body);
            _isWhereClause = false;
        }

        private void VisitOrderBy(MethodCallExpression node, bool ascending)
        {
            // Visit the source first
            Visit(node.Arguments[0]);

            // Extract the key selector (second argument is the lambda)
            var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);

            if (lambda.Body is MemberExpression memberExpr)
            {
                _components.AddOrderByClause(memberExpr.Member.Name, ascending);
            }
            else
            {
                throw new NotSupportedException("OrderBy must specify a simple property access (e.g., p => p.Name).");
            }
        }

        private void VisitTake(MethodCallExpression node)
        {
            // Visit the source first
            Visit(node.Arguments[0]);

            // Extract the count (second argument is a constant)
            if (node.Arguments[1] is ConstantExpression constantExpr && constantExpr.Value is int count)
            {
                _components.Take = count;
            }
            else
            {
                throw new NotSupportedException("Take must have a constant integer argument.");
            }
        }

        private void VisitSkip(MethodCallExpression node)
        {
            // Visit the source first
            Visit(node.Arguments[0]);

            // Extract the count (second argument is a constant)
            if (node.Arguments[1] is ConstantExpression constantExpr && constantExpr.Value is int count)
            {
                _components.Skip = count;
            }
            else
            {
                throw new NotSupportedException("Skip must have a constant integer argument.");
            }
        }

        private Expression VisitStringMethod(MethodCallExpression node)
        {
            if (!_isWhereClause)
                throw new NotSupportedException("String methods are only supported in WHERE clauses.");

            switch (node.Method.Name)
            {
                case "StartsWith":
                    VisitStartsWith(node);
                    break;

                case "EndsWith":
                    VisitEndsWith(node);
                    break;

                case "Contains":
                    VisitContains(node);
                    break;

                default:
                    throw new NotSupportedException($"String method '{node.Method.Name}' cannot be translated to SQL.");
            }

            return node;
        }

        [RequiresDynamicCode("Calls FAnsi.Discovery.QueryableAbstraction.FAnsiExpressionVisitor.GetConstantValue(Expression)")]
        private void VisitStartsWith(MethodCallExpression node)
        {
            var propertyName = GetPropertyName(node.Object);
            var value = GetConstantValue(node.Arguments[0]);
            _components.AddWhereClause(propertyName, WhereOperator.Like, $"{value}%");
        }

        [RequiresDynamicCode("Calls FAnsi.Discovery.QueryableAbstraction.FAnsiExpressionVisitor.GetConstantValue(Expression)")]
        private void VisitEndsWith(MethodCallExpression node)
        {
            var propertyName = GetPropertyName(node.Object);
            var value = GetConstantValue(node.Arguments[0]);
            _components.AddWhereClause(propertyName, WhereOperator.Like, $"%{value}");
        }

        [RequiresDynamicCode("Calls FAnsi.Discovery.QueryableAbstraction.FAnsiExpressionVisitor.GetConstantValue(Expression)")]
        private void VisitContains(MethodCallExpression node)
        {
            var propertyName = GetPropertyName(node.Object);
            var value = GetConstantValue(node.Arguments[0]);
            _components.AddWhereClause(propertyName, WhereOperator.Like, $"%{value}%");
        }

        [RequiresDynamicCode()]
        protected override Expression VisitBinary(BinaryExpression node)
        {
            if (!_isWhereClause)
                return base.VisitBinary(node);

            // Handle logical operators
            if (node.NodeType == ExpressionType.AndAlso)
            {
                Visit(node.Left);
                Visit(node.Right);
                return node;
            }

            if (node.NodeType == ExpressionType.OrElse)
            {
                throw new NotSupportedException("OR conditions are not yet supported. Use multiple Where() calls for AND conditions.");
            }

            // Handle comparison operators
            var propertyName = GetPropertyName(node.Left);
            var value = GetConstantValue(node.Right);
            var op = GetWhereOperator(node.NodeType);

            _components.AddWhereClause(propertyName, op, value);
            return node;
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            // Handle negation: !p.IsActive => IsActive = false
            if (node.NodeType == ExpressionType.Not && _isWhereClause &&
                node.Operand is MemberExpression memberExpr && memberExpr.Type == typeof(bool))
            {
                _components.AddWhereClause(memberExpr.Member.Name, WhereOperator.Equal, false);
                return node;
            }

            return base.VisitUnary(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            // Handle boolean properties: p.IsActive => IsActive = true
            if (_isWhereClause && node.Type == typeof(bool))
            {
                _components.AddWhereClause(node.Member.Name, WhereOperator.Equal, true);
                return node;
            }

            return base.VisitMember(node);
        }

        private static string GetPropertyName(Expression? expression)
        {
            if (expression is MemberExpression memberExpr)
                return memberExpr.Member.Name;

            if (expression is UnaryExpression unaryExpr && unaryExpr.Operand is MemberExpression innerMember)
                return innerMember.Member.Name;

            throw new NotSupportedException("Expected a property access expression.");
        }

        [RequiresDynamicCode("Calls System.Linq.Expressions.Expression.Lambda(Expression, params ParameterExpression[])")]
        private static object? GetConstantValue(Expression expression)
        {
            // Direct constant
            if (expression is ConstantExpression constantExpr)
                return constantExpr.Value;

            // Member access on a constant (e.g., captured variable)
            if (expression is MemberExpression memberExpr && memberExpr.Expression is ConstantExpression objExpr)
            {
                if (memberExpr.Member is FieldInfo field)
                    return field.GetValue(objExpr.Value);

                if (memberExpr.Member is PropertyInfo property)
                    return property.GetValue(objExpr.Value);
            }

            // Compile and evaluate the expression
            var lambda = Expression.Lambda(expression);
            return lambda.Compile().DynamicInvoke();
        }

        private static WhereOperator GetWhereOperator(ExpressionType nodeType)
        {
            return nodeType switch
            {
                ExpressionType.Equal => WhereOperator.Equal,
                ExpressionType.NotEqual => WhereOperator.NotEqual,
                ExpressionType.GreaterThan => WhereOperator.GreaterThan,
                ExpressionType.GreaterThanOrEqual => WhereOperator.GreaterThanOrEqual,
                ExpressionType.LessThan => WhereOperator.LessThan,
                ExpressionType.LessThanOrEqual => WhereOperator.LessThanOrEqual,
                _ => throw new NotSupportedException($"Expression type '{nodeType}' is not supported in WHERE clauses.")
            };
        }

        private static Expression StripQuotes(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Quote)
            {
                expression = ((UnaryExpression)expression).Operand;
            }
            return expression;
        }
    }
}
