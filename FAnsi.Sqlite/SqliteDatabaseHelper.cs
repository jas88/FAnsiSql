using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using Microsoft.Data.Sqlite;

namespace FAnsi.Implementations.Sqlite;

public sealed class SqliteDatabaseHelper : DiscoveredDatabaseHelper
{
    public override IEnumerable<DiscoveredTableValuedFunction> ListTableValuedFunctions(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper,
        DbConnection connection, string database, DbTransaction? transaction = null) =>
        Enumerable.Empty<DiscoveredTableValuedFunction>();

    public override IEnumerable<DiscoveredStoredprocedure> ListStoredprocedures(DbConnectionStringBuilder builder, string database) => 
        Enumerable.Empty<DiscoveredStoredprocedure>(); // SQLite doesn't support stored procedures

    public override IDiscoveredTableHelper GetTableHelper() => new SqliteTableHelper();

    public override void DropDatabase(DiscoveredDatabase database)
    {
        var filePath = database.Server.Builder.TryGetValue("Data Source", out var dataSource) ? dataSource?.ToString() : null;
        if (!string.IsNullOrEmpty(filePath) && File.Exists(filePath))
        {
            File.Delete(filePath);
        }
    }

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

    public override void CreateBackup(DiscoveredDatabase discoveredDatabase, string backupName)
    {
        var filePath = discoveredDatabase.Server.Builder.TryGetValue("Data Source", out var dataSource) ? dataSource?.ToString() : null;
        if (!string.IsNullOrEmpty(filePath) && File.Exists(filePath))
        {
            var backupPath = Path.Combine(Path.GetDirectoryName(filePath) ?? "", backupName);
            File.Copy(filePath, backupPath, true);
        }
    }

    public override void CreateSchema(DiscoveredDatabase discoveredDatabase, string name)
    {
        // SQLite doesn't support schemas in the traditional sense
        // This is a no-op
    }

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