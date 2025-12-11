using System.Data;
using System.Data.Common;
using System.Globalization;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using Microsoft.Data.Sqlite;

namespace FAnsi.Implementations.Sqlite;

/// <summary>
///     SQLite-specific implementation of database helper functionality. Handles database-level operations
///     including listing tables, creating backups, and managing database schemas.
/// </summary>
/// <remarks>
///     SQLite has several limitations compared to traditional RDBMS:
///     <list type="bullet">
///         <item>
///             <description>No stored procedures or table-valued functions</description>
///         </item>
///         <item>
///             <description>No traditional schema support (ATTACH DATABASE provides similar functionality)</description>
///         </item>
///         <item>
///             <description>File-based: databases are single files that can be copied/deleted directly</description>
///         </item>
///     </list>
/// </remarks>
public sealed class SqliteDatabaseHelper : DiscoveredDatabaseHelper
{
    /// <summary>
    ///     Lists table-valued functions in the database.
    /// </summary>
    /// <returns>An empty collection (SQLite doesn't support table-valued functions)</returns>
    /// <inheritdoc />
    public override IEnumerable<DiscoveredTableValuedFunction> ListTableValuedFunctions(DiscoveredDatabase parent,
        IQuerySyntaxHelper querySyntaxHelper,
        DbConnection connection, string database, DbTransaction? transaction = null) =>
        Enumerable.Empty<DiscoveredTableValuedFunction>();

    /// <summary>
    ///     Lists stored procedures in the database.
    /// </summary>
    /// <returns>An empty collection (SQLite doesn't support stored procedures)</returns>
    /// <inheritdoc />
    public override IEnumerable<DiscoveredStoredprocedure> ListStoredprocedures(DbConnectionStringBuilder builder,
        string database) =>
        Enumerable.Empty<DiscoveredStoredprocedure>(); // SQLite doesn't support stored procedures

    /// <inheritdoc />
    public override IDiscoveredTableHelper GetTableHelper() => new SqliteTableHelper();

    /// <summary>
    ///     Drops (deletes) a SQLite database by removing its file.
    /// </summary>
    /// <param name="database">The database to drop</param>
    /// <remarks>
    ///     SQLite databases are single files, so dropping involves deleting the file from disk.
    ///     Associated journal/WAL files are also removed by the file system.
    /// </remarks>
    public override void DropDatabase(DiscoveredDatabase database)
    {
        var filePath = database.Server.Builder.TryGetValue("Data Source", out var dataSource)
            ? dataSource?.ToString()
            : null;
        if (!string.IsNullOrEmpty(filePath) && File.Exists(filePath)) File.Delete(filePath);
    }

    /// <summary>
    ///     Retrieves descriptive information about the database file.
    /// </summary>
    /// <param name="builder">The connection string builder</param>
    /// <param name="database">The database name/path</param>
    /// <returns>A dictionary containing file metadata (path, size, timestamps)</returns>
    public override Dictionary<string, string> DescribeDatabase(DbConnectionStringBuilder builder, string database)
    {
        var filePath = builder.TryGetValue("Data Source", out var dataSource) ? dataSource?.ToString() : null;

        var toReturn = new Dictionary<string, string>();

        if (!string.IsNullOrEmpty(filePath) && File.Exists(filePath))
        {
            var fileInfo = new FileInfo(filePath);
            toReturn.Add("Database File", filePath);
            toReturn.Add("File Size", $"{fileInfo.Length} bytes");
            toReturn.Add("Created", fileInfo.CreationTime.ToString(CultureInfo.InvariantCulture));
            toReturn.Add("Modified", fileInfo.LastWriteTime.ToString(CultureInfo.InvariantCulture));
        }

        return toReturn;
    }

    /// <summary>
    ///     Detaches a database and returns its directory.
    /// </summary>
    /// <param name="database">The database to detach</param>
    /// <returns>The directory containing the database file, or null if not found</returns>
    /// <remarks>
    ///     SQLite files are already "detached" (not actively managed by a server process).
    ///     This method simply returns the directory containing the database file.
    /// </remarks>
    public override DirectoryInfo? Detach(DiscoveredDatabase database)
    {
        // SQLite files are already "detached" - just return the directory containing the file
        var filePath = database.Server.Builder.TryGetValue("Data Source", out var dataSource)
            ? dataSource?.ToString()
            : null;
        if (!string.IsNullOrEmpty(filePath))
        {
            var directory = Path.GetDirectoryName(filePath);
            return !string.IsNullOrEmpty(directory) ? new DirectoryInfo(directory) : null;
        }

        return null;
    }

    /// <summary>
    ///     Creates a backup of the database by copying its file.
    /// </summary>
    /// <param name="discoveredDatabase">The database to back up</param>
    /// <param name="backupName">The name for the backup file</param>
    /// <remarks>
    ///     SQLite databases can be backed up by simply copying the file. The backup is created
    ///     in the same directory as the original database file.
    /// </remarks>
    public override void CreateBackup(DiscoveredDatabase discoveredDatabase, string backupName)
    {
        var filePath = discoveredDatabase.Server.Builder.TryGetValue("Data Source", out var dataSource)
            ? dataSource?.ToString()
            : null;
        if (!string.IsNullOrEmpty(filePath) && File.Exists(filePath))
        {
            var directory = Path.GetDirectoryName(filePath);
            if (string.IsNullOrEmpty(directory))
                directory = Environment.CurrentDirectory;

            var backupPath = Path.Join(directory, backupName);
            File.Copy(filePath, backupPath, true);
        }
    }

    /// <summary>
    ///     Creates a schema in the database.
    /// </summary>
    /// <param name="discoveredDatabase">The database to create the schema in</param>
    /// <param name="name">The schema name</param>
    /// <remarks>
    ///     SQLite doesn't support schemas in the traditional sense. This is a no-op for compatibility.
    ///     SQLite uses ATTACH DATABASE for similar functionality.
    /// </remarks>
    public override void CreateSchema(DiscoveredDatabase discoveredDatabase, string name)
    {
        // SQLite doesn't support schemas in the traditional sense
        // This is a no-op
    }

    /// <summary>
    ///     Lists all tables (and optionally views) in the database.
    /// </summary>
    /// <param name="parent">The parent database</param>
    /// <param name="querySyntaxHelper">The query syntax helper</param>
    /// <param name="connection">The open database connection</param>
    /// <param name="database">The database name (file path for SQLite)</param>
    /// <param name="includeViews">Whether to include views in the results</param>
    /// <param name="transaction">Optional transaction</param>
    /// <returns>An enumerable of discovered tables and views</returns>
    /// <remarks>
    ///     Queries the sqlite_master system table to retrieve table and view information.
    ///     Excludes SQLite system tables (those starting with 'sqlite_').
    /// </remarks>
    public override IEnumerable<DiscoveredTable> ListTables(DiscoveredDatabase parent,
        IQuerySyntaxHelper querySyntaxHelper, DbConnection connection, string database, bool includeViews,
        DbTransaction? transaction = null)
    {
        if (connection.State == ConnectionState.Closed)
            throw new InvalidOperationException("Expected connection to be open");

        var tables = new List<DiscoveredTable>();

        // Get plain table names - we'll wrap them when needed for SQL queries
        var sql = includeViews
            ? "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'"
            : "SELECT name, type FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'";

        using var cmd = new SqliteCommand(sql, (SqliteConnection)connection);
        cmd.Transaction = transaction as SqliteTransaction;

        using var r = cmd.ExecuteReader();
        while (r.Read())
        {
            var tableName = (string)r["name"];
            var tableType = (string)r["type"];
            var isView = tableType.Equals("view", StringComparison.OrdinalIgnoreCase);

            tables.Add(new DiscoveredTable(parent, tableName, querySyntaxHelper, null,
                isView ? TableType.View : TableType.Table));
        }

        return tables;
    }

    /// <summary>
    ///     Generates the SQL for a column definition in a CREATE TABLE statement.
    ///     SQLite requires DEFAULT function calls to be wrapped in parentheses.
    /// </summary>
    protected override string GetCreateTableSqlLineForColumn(DatabaseColumnRequest col, string datatype,
        IQuerySyntaxHelper syntaxHelper) =>
        // SQLite requires DEFAULT functions to be wrapped in parentheses: DEFAULT (date('now'))
        $"{syntaxHelper.EnsureWrapped(col.ColumnName)} {datatype} {(col.Default != MandatoryScalarFunctions.None ? $"default ({syntaxHelper.GetScalarFunctionSql(col.Default)})" : "")} {(string.IsNullOrWhiteSpace(col.Collation) ? "" : $"COLLATE {col.Collation}")} {(col.AllowNulls && !col.IsPrimaryKey ? " NULL" : " NOT NULL")} {(col.IsAutoIncrement ? syntaxHelper.GetAutoIncrementKeywordIfAny() : "")}";
}
