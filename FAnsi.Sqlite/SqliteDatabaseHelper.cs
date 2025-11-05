using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;
using Microsoft.Data.Sqlite;

namespace FAnsi.Implementations.Sqlite;

/// <summary>
/// SQLite-specific implementation of database helper functionality. Handles database-level operations
/// including listing tables, creating backups, and managing database schemas.
/// </summary>
/// <remarks>
/// SQLite has several limitations compared to traditional RDBMS:
/// <list type="bullet">
/// <item><description>No stored procedures or table-valued functions</description></item>
/// <item><description>No traditional schema support (ATTACH DATABASE provides similar functionality)</description></item>
/// <item><description>File-based: databases are single files that can be copied/deleted directly</description></item>
/// </list>
/// </remarks>
public sealed class SqliteDatabaseHelper : DiscoveredDatabaseHelper
{
    /// <summary>
    /// Lists table-valued functions in the database.
    /// </summary>
    /// <returns>An empty collection (SQLite doesn't support table-valued functions)</returns>
    /// <inheritdoc />
    public override IEnumerable<DiscoveredTableValuedFunction> ListTableValuedFunctions(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper,
        DbConnection connection, string database, DbTransaction? transaction = null) =>
        Enumerable.Empty<DiscoveredTableValuedFunction>();

    /// <summary>
    /// Lists stored procedures in the database.
    /// </summary>
    /// <returns>An empty collection (SQLite doesn't support stored procedures)</returns>
    /// <inheritdoc />
    public override IEnumerable<DiscoveredStoredprocedure> ListStoredprocedures(DbConnectionStringBuilder builder, string database) =>
        Enumerable.Empty<DiscoveredStoredprocedure>(); // SQLite doesn't support stored procedures

    /// <inheritdoc />
    public override IDiscoveredTableHelper GetTableHelper() => new SqliteTableHelper();

    /// <summary>
    /// Generates SQLite-compatible CREATE TABLE column definition with proper default value syntax.
    /// </summary>
    /// <param name="col">The column request</param>
    /// <param name="datatype">The SQL data type</param>
    /// <param name="syntaxHelper">The query syntax helper</param>
    /// <returns>SQLite-compatible column definition</returns>
    /// <remarks>
    /// SQLite requires specific syntax for default values, especially for functions like date('now').
    /// This method ensures proper formatting without extra parentheses that could cause syntax errors.
    /// </remarks>
    protected override string GetCreateTableSqlLineForColumn(DatabaseColumnRequest col, string datatype, IQuerySyntaxHelper syntaxHelper)
    {
        var parts = new List<string>
        {
            syntaxHelper.EnsureWrapped(col.ColumnName),
            datatype
        };

        // Add default value if specified
        if (col.Default != MandatoryScalarFunctions.None)
        {
            var defaultValue = syntaxHelper.GetScalarFunctionSql(col.Default);
            parts.Add($"DEFAULT {defaultValue}");
        }

        // Add collation if specified
        if (!string.IsNullOrWhiteSpace(col.Collation))
        {
            parts.Add($"COLLATE {col.Collation}");
        }

        // Add NULL/NOT NULL constraint
        if (col.AllowNulls && !col.IsPrimaryKey)
        {
            parts.Add("NULL");
        }
        else
        {
            parts.Add("NOT NULL");
        }

        // Add auto-increment if specified
        if (col.IsAutoIncrement)
        {
            parts.Add(syntaxHelper.GetAutoIncrementKeywordIfAny());
        }

        return string.Join(" ", parts);
    }

    /// <summary>
    /// Generates SQLite-compatible CREATE TABLE SQL with proper comma handling.
    /// </summary>
    /// <param name="database">The database to create the table in</param>
    /// <param name="tableName">The name of the table to create</param>
    /// <param name="columns">The columns to create</param>
    /// <param name="foreignKeyPairs">Foreign key relationships (optional)</param>
    /// <param name="cascadeDelete">Whether to cascade deletes for foreign keys</param>
    /// <param name="schema">The schema name (ignored for SQLite)</param>
    /// <returns>SQLite-compatible CREATE TABLE SQL statement</returns>
    /// <remarks>
    /// Overrides the base implementation to ensure proper comma handling in CREATE TABLE statements.
    /// SQLite is stricter about comma placement and doesn't allow trailing commas before closing parentheses.
    /// </remarks>
    public override string GetCreateTableSql(DiscoveredDatabase database, string tableName,
        DatabaseColumnRequest[] columns, Dictionary<DatabaseColumnRequest, DiscoveredColumn>? foreignKeyPairs,
        bool cascadeDelete, string? schema)
    {
        var syntaxHelper = database.Server.GetQuerySyntaxHelper();
        var fullyQualifiedName = syntaxHelper.EnsureFullyQualified(database.GetRuntimeName(), null, tableName);

        var bodySql = new System.Text.StringBuilder();
        bodySql.AppendLine($"CREATE TABLE {fullyQualifiedName}(");

        for (var i = 0; i < columns.Length; i++)
        {
            var col = columns[i];
            var datatype = col.GetSQLDbType(syntaxHelper.TypeTranslater);

            // Add the column definition
            bodySql.Append(GetCreateTableSqlLineForColumn(col, datatype, syntaxHelper));

            // Add comma if not the last column and there are no primary keys or foreign keys
            var isLastColumn = i == columns.Length - 1;
            var hasPrimaryKey = columns.Any(c => c.IsPrimaryKey);
            var hasForeignKey = foreignKeyPairs != null && foreignKeyPairs.Any();

            if (!isLastColumn || hasPrimaryKey || hasForeignKey)
                bodySql.AppendLine(",");
            else
                bodySql.AppendLine();
        }

        var pks = columns.Where(static c => c.IsPrimaryKey).ToArray();
        if (pks.Length != 0)
        {
            // Add primary key constraint without trailing comma
            var constraintName = MakeSensibleConstraintName("PK_", tableName);
            var pkColumns = string.Join(",", pks.Select(c => syntaxHelper.EnsureWrapped(c.ColumnName)));
            bodySql.AppendLine($" CONSTRAINT {constraintName} PRIMARY KEY ({pkColumns})");
        }

        if (foreignKeyPairs != null)
        {
            bodySql.AppendLine();
            bodySql.AppendLine(GetForeignKeyConstraintSql(tableName, syntaxHelper,
                foreignKeyPairs.ToDictionary(static k => (IHasRuntimeName)k.Key, static v => v.Value), cascadeDelete, null));
        }

        bodySql.AppendLine($");{Environment.NewLine}");

        return bodySql.ToString();
    }

    /// <summary>
    /// Generates a sensible constraint name for SQLite.
    /// </summary>
    /// <param name="prefix">The prefix for the constraint name</param>
    /// <param name="tableName">The table name</param>
    /// <returns>A valid constraint name within SQLite's limits</returns>
    /// <remarks>
    /// SQLite has a 128-character limit on identifiers. This method ensures constraint names
    /// fit within this limit while remaining meaningful.
    /// </remarks>
    private static string MakeSensibleConstraintName(string prefix, string tableName)
    {
        const int MaxIdentifierLength = 128; // SQLite limit

        var constraintName = QuerySyntaxHelper.MakeHeaderNameSensible(tableName);

        if (string.IsNullOrWhiteSpace(constraintName))
        {
            var r = new Random();
            constraintName = $"Constraint{r.Next(10000)}";
            return $"{prefix}{constraintName}";
        }

        var prefixAndName = prefix + constraintName;
        return prefixAndName.Length > MaxIdentifierLength
            ? prefix + constraintName[..(MaxIdentifierLength - prefix.Length)]
            : prefixAndName;
    }

    /// <summary>
    /// Drops (deletes) a SQLite database by removing its file.
    /// </summary>
    /// <param name="database">The database to drop</param>
    /// <remarks>
    /// SQLite databases are single files, so dropping involves deleting the file from disk.
    /// Associated journal/WAL files are also removed by the file system.
    /// </remarks>
    public override void DropDatabase(DiscoveredDatabase database)
    {
        var filePath = database.Server.Builder.TryGetValue("Data Source", out var dataSource) ? dataSource?.ToString() : null;
        if (!string.IsNullOrEmpty(filePath) && File.Exists(filePath))
        {
            File.Delete(filePath);
        }
    }

    /// <summary>
    /// Retrieves descriptive information about the database file.
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
            toReturn.Add("Created", fileInfo.CreationTime.ToString());
            toReturn.Add("Modified", fileInfo.LastWriteTime.ToString());
        }

        return toReturn;
    }

    /// <summary>
    /// Detaches a database and returns its directory.
    /// </summary>
    /// <param name="database">The database to detach</param>
    /// <returns>The directory containing the database file, or null if not found</returns>
    /// <remarks>
    /// SQLite files are already "detached" (not actively managed by a server process).
    /// This method simply returns the directory containing the database file.
    /// </remarks>
    public override DirectoryInfo? Detach(DiscoveredDatabase database)
    {
        // SQLite files are already "detached" - just return the directory containing the file
        var filePath = database.Server.Builder.TryGetValue("Data Source", out var dataSource) ? dataSource?.ToString() : null;
        if (!string.IsNullOrEmpty(filePath))
        {
            var directory = Path.GetDirectoryName(filePath);
            return !string.IsNullOrEmpty(directory) ? new DirectoryInfo(directory) : null;
        }
        return null;
    }

    /// <summary>
    /// Creates a backup of the database by copying its file.
    /// </summary>
    /// <param name="discoveredDatabase">The database to back up</param>
    /// <param name="backupName">The name for the backup file</param>
    /// <remarks>
    /// SQLite databases can be backed up by simply copying the file. The backup is created
    /// in the same directory as the original database file.
    /// </remarks>
    public override void CreateBackup(DiscoveredDatabase discoveredDatabase, string backupName)
    {
        var filePath = discoveredDatabase.Server.Builder.TryGetValue("Data Source", out var dataSource) ? dataSource?.ToString() : null;
        if (!string.IsNullOrEmpty(filePath) && File.Exists(filePath))
        {
            var backupPath = Path.Combine(Path.GetDirectoryName(filePath) ?? "", backupName);
            File.Copy(filePath, backupPath, true);
        }
    }

    /// <summary>
    /// Creates a schema in the database.
    /// </summary>
    /// <param name="discoveredDatabase">The database to create the schema in</param>
    /// <param name="name">The schema name</param>
    /// <remarks>
    /// SQLite doesn't support schemas in the traditional sense. This is a no-op for compatibility.
    /// SQLite uses ATTACH DATABASE for similar functionality.
    /// </remarks>
    public override void CreateSchema(DiscoveredDatabase discoveredDatabase, string name)
    {
        // SQLite doesn't support schemas in the traditional sense
        // This is a no-op
    }

    /// <summary>
    /// Lists all tables (and optionally views) in the database.
    /// </summary>
    /// <param name="parent">The parent database</param>
    /// <param name="querySyntaxHelper">The query syntax helper</param>
    /// <param name="connection">The open database connection</param>
    /// <param name="database">The database name (file path for SQLite)</param>
    /// <param name="includeViews">Whether to include views in the results</param>
    /// <param name="transaction">Optional transaction</param>
    /// <returns>An enumerable of discovered tables and views</returns>
    /// <remarks>
    /// Queries the sqlite_master system table to retrieve table and view information.
    /// Excludes SQLite system tables (those starting with 'sqlite_').
    /// </remarks>
    public override IEnumerable<DiscoveredTable> ListTables(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper, DbConnection connection, string database, bool includeViews, DbTransaction? transaction = null)
    {
        if (connection.State == ConnectionState.Closed)
            throw new InvalidOperationException("Expected connection to be open");

        var tables = new List<DiscoveredTable>();

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

            //skip invalid table names
            if (!querySyntaxHelper.IsValidTableName(tableName, out _))
                continue;

            tables.Add(new DiscoveredTable(parent, tableName, querySyntaxHelper, null, isView ? TableType.View : TableType.Table));
        }

        return tables;
    }
}
