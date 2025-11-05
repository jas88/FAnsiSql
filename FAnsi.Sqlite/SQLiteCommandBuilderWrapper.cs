using System;
using System.Data;
using System.Data.Common;

namespace FAnsi.Implementations.Sqlite;

/// <summary>
/// SQLite-compatible implementation of DbCommandBuilder.
/// </summary>
/// <remarks>
/// SQLite doesn't support automatic command generation from DataTable schemas,
/// so this is a minimal implementation that provides basic functionality.
/// </remarks>
internal sealed class SQLiteCommandBuilderWrapper : DbCommandBuilder
{
    /// <summary>
    /// Gets the parameter placeholder for SQLite.
    /// </summary>
    /// <param name="parameterIndex">The zero-based index of the parameter</param>
    /// <returns>The parameter placeholder string (e.g., "@p0")</returns>
    protected override string GetParameterPlaceholder(int parameterIndex)
    {
        return $"@p{parameterIndex}";
    }

    /// <summary>
    /// Gets the parameter name for SQLite.
    /// </summary>
    /// <param name="parameterIndex">The zero-based index of the parameter</param>
    /// <returns>The parameter name (e.g., "@p0")</returns>
    protected override string GetParameterName(int parameterIndex)
    {
        return $"@p{parameterIndex}";
    }

    /// <summary>
    /// Gets the parameter name for SQLite from a parameter name.
    /// </summary>
    /// <param name="parameterName">The parameter name</param>
    /// <returns>The parameter name (SQLite parameter names don't need transformation)</returns>
    protected override string GetParameterName(string parameterName)
    {
        return parameterName.StartsWith("@") ? parameterName : $"@{parameterName}";
    }

    /// <summary>
    /// Applies parameter information for SQLite.
    /// </summary>
    /// <param name="parameter">The DbParameter to configure</param>
    /// <param name="row">The DataRow containing schema information</param>
    /// <param name="statementType">The type of statement</param>
    /// <param name="whereClause">Whether this is a WHERE clause parameter</param>
    protected override void ApplyParameterInfo(DbParameter parameter, DataRow row, StatementType statementType, bool whereClause)
    {
        // SQLite parameters are straightforward - just use the value as-is
        // Note: DbParameter doesn't expose AllowDBNull directly, so we skip this for SQLite
    }

    /// <summary>
    /// Sets the row updating handler for SQLite.
    /// </summary>
    /// <param name="dataAdapter">The DbDataAdapter to set the handler for</param>
    protected override void SetRowUpdatingHandler(DbDataAdapter dataAdapter)
    {
        // SQLite doesn't need special row updating handling
    }
}