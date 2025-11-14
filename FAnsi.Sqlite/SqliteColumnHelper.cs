using System.Globalization;
using System.Text;
using FAnsi.Discovery;
using FAnsi.Naming;

namespace FAnsi.Implementations.Sqlite;

/// <summary>
/// SQLite-specific implementation of column helper functionality. Provides column-level operations
/// such as generating queries and handling column alterations.
/// </summary>
/// <remarks>
/// SQLite has limited ALTER COLUMN support. Most column type changes require recreating the entire table.
/// </remarks>
public sealed class SqliteColumnHelper : IDiscoveredColumnHelper
{
    /// <summary>
    /// Generates SQL to retrieve the top X values from a specific column.
    /// </summary>
    /// <param name="database">The database containing the table</param>
    /// <param name="table">The table containing the column</param>
    /// <param name="column">The column to query</param>
    /// <param name="topX">The number of rows to return</param>
    /// <param name="discardNulls">Whether to exclude NULL values</param>
    /// <returns>SQL query string using SQLite's LIMIT clause</returns>
    public string GetTopXSqlForColumn(IHasRuntimeName database, IHasFullyQualifiedNameToo table, IHasRuntimeName column, int topX, bool discardNulls)
    {
        var syntax = SqliteQuerySyntaxHelper.Instance;

        var sql = new StringBuilder();

        sql.Append(CultureInfo.InvariantCulture, $"SELECT {syntax.EnsureWrapped(column.GetRuntimeName())} FROM {table.GetFullyQualifiedName()}");

        if (discardNulls)
            sql.Append(CultureInfo.InvariantCulture, $" WHERE {syntax.EnsureWrapped(column.GetRuntimeName())} IS NOT NULL");

        sql.Append(CultureInfo.InvariantCulture, $" LIMIT {topX}");
        return sql.ToString();
    }

    /// <summary>
    /// Generates SQL to alter a column's type and nullability.
    /// </summary>
    /// <param name="column">The column to alter</param>
    /// <param name="newType">The new data type</param>
    /// <param name="allowNulls">Whether the column should allow NULLs</param>
    /// <returns>Never returns (always throws)</returns>
    /// <exception cref="System.NotSupportedException">
    /// SQLite does not support ALTER COLUMN. Column type changes require recreating the entire table.
    /// </exception>
    /// <remarks>
    /// SQLite's ALTER TABLE is limited. To change a column type, you must:
    /// <list type="number">
    /// <item><description>Create a new table with the desired schema</description></item>
    /// <item><description>Copy data from the old table to the new table</description></item>
    /// <item><description>Drop the old table</description></item>
    /// <item><description>Rename the new table to the original name</description></item>
    /// </list>
    /// </remarks>
    public string GetAlterColumnToSql(DiscoveredColumn column, string newType, bool allowNulls)
    {
        // SQLite doesn't support ALTER COLUMN directly - would need to recreate table
        throw new System.NotSupportedException("SQLite does not support altering column types directly. Table recreation would be required.");
    }
}
