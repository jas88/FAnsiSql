using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using FAnsi.Naming;
using Microsoft.Data.Sqlite;

namespace FAnsi.Implementations.Sqlite;

/// <summary>
/// SQLite-specific implementation of table helper functionality. Provides table-level operations
/// including querying, schema discovery, indexing, and relationship management.
/// </summary>
/// <remarks>
/// <para>SQLite limitations:</para>
/// <list type="bullet">
/// <item><description>Cannot drop columns (requires table recreation)</description></item>
/// <item><description>Cannot add primary keys to existing tables</description></item>
/// <item><description>Limited ALTER TABLE support</description></item>
/// <item><description>Auto-increment via INTEGER PRIMARY KEY</description></item>
/// </list>
/// </remarks>
public sealed class SqliteTableHelper : DiscoveredTableHelper
{
    /// <summary>
    /// Generates SQL to select the top X rows from a table.
    /// </summary>
    /// <param name="table">The table to query</param>
    /// <param name="topX">The number of rows to return</param>
    /// <returns>SQL query using LIMIT clause</returns>
    public override string GetTopXSqlForTable(IHasFullyQualifiedNameToo table, int topX) =>
        $"SELECT * FROM {table.GetFullyQualifiedName()} LIMIT {topX}";

    /// <summary>
    /// Discovers all columns in a table by querying SQLite's PRAGMA table_info.
    /// </summary>
    /// <param name="discoveredTable">The table to discover columns for</param>
    /// <param name="connection">The managed database connection</param>
    /// <param name="database">The database name (file path for SQLite)</param>
    /// <returns>An enumerable of discovered columns with their properties</returns>
    /// <remarks>
    /// Uses PRAGMA table_info() to retrieve column information including name, type, nullability,
    /// and primary key status. Detects auto-increment by checking for INTEGER PRIMARY KEY.
    /// </remarks>
    public override IEnumerable<DiscoveredColumn> DiscoverColumns(DiscoveredTable discoveredTable, IManagedConnection connection, string database)
    {
        var tableName = discoveredTable.GetRuntimeName();

        using var cmd = discoveredTable.Database.Server.Helper.GetCommand($"PRAGMA table_info({tableName})", connection.Connection);
        cmd.Transaction = connection.Transaction;

        using var r = cmd.ExecuteReader();
        if (!r.HasRows)
            throw new InvalidOperationException($"Could not find any columns for table {tableName} in database {database}");

        while (r.Read())
        {
            var columnName = (string)r["name"];
            var dataType = (string)r["type"];
            var notNull = Convert.ToInt64(r["notnull"], CultureInfo.InvariantCulture) == 1;
            var isPrimaryKey = Convert.ToInt64(r["pk"], CultureInfo.InvariantCulture) > 0;

            var toAdd = new DiscoveredColumn(discoveredTable, columnName, !notNull)
            {
                IsPrimaryKey = isPrimaryKey,
                IsAutoIncrement = isPrimaryKey && dataType.Contains("INTEGER", StringComparison.OrdinalIgnoreCase)
            };

            toAdd.DataType = new DiscoveredDataType(r, dataType, toAdd);

            yield return toAdd;
        }
    }

    /// <inheritdoc />
    public override IDiscoveredColumnHelper GetColumnHelper() => new SqliteColumnHelper();

    /// <summary>
    /// Drops a table-valued function.
    /// </summary>
    /// <param name="connection">The database connection</param>
    /// <param name="functionToDrop">The function to drop</param>
    /// <exception cref="NotSupportedException">SQLite does not support table-valued functions</exception>
    public override void DropFunction(DbConnection connection, DiscoveredTableValuedFunction functionToDrop)
    {
        throw new NotSupportedException("SQLite does not support table-valued functions");
    }

    /// <summary>
    /// Drops a column from a table.
    /// </summary>
    /// <param name="connection">The database connection</param>
    /// <param name="columnToDrop">The column to drop</param>
    /// <exception cref="NotSupportedException">
    /// SQLite does not support DROP COLUMN. Column removal requires recreating the entire table.
    /// </exception>
    public override void DropColumn(DbConnection connection, DiscoveredColumn columnToDrop)
    {
        // SQLite doesn't support DROP COLUMN directly - would need to recreate table
        throw new NotSupportedException("SQLite does not support dropping columns directly. Table recreation would be required.");
    }

    public override int ExecuteInsertReturningIdentity(DiscoveredTable discoveredTable, DbCommand cmd, IManagedTransaction? transaction = null)
    {
        cmd.Transaction = transaction?.Transaction;
        cmd.ExecuteNonQuery();

        // Get the last inserted rowid
        using var identityCmd = discoveredTable.Database.Server.GetCommand("SELECT last_insert_rowid()", cmd.Connection!);
        identityCmd.Transaction = transaction?.Transaction;
        return Convert.ToInt32(identityCmd.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    public override IEnumerable<DiscoveredParameter> DiscoverTableValuedFunctionParameters(DbConnection connection, DiscoveredTableValuedFunction discoveredTableValuedFunction, DbTransaction? transaction)
    {
        // SQLite doesn't support table-valued functions
        return Enumerable.Empty<DiscoveredParameter>();
    }

    public override IBulkCopy BeginBulkInsert(DiscoveredTable discoveredTable, IManagedConnection connection, CultureInfo culture)
    {
        return new SqliteBulkCopy(discoveredTable, connection, culture);
    }

    public override void TruncateTable(DiscoveredTable discoveredTable)
    {
        var server = discoveredTable.Database.Server;
        using var con = server.GetConnection();
        con.Open();
        using var cmd = server.GetCommand($"DELETE FROM {discoveredTable.GetFullyQualifiedName()}", con);
        cmd.ExecuteNonQuery();
    }

    public override void MakeDistinct(DatabaseOperationArgs args, DiscoveredTable discoveredTable)
    {
        var tempTableName = discoveredTable.GetRuntimeName() + "_temp";
        var syntax = discoveredTable.GetQuerySyntaxHelper();

        using var con = args.GetManagedConnection(discoveredTable);

        // Create temp table with distinct values
        using var cmd1 = discoveredTable.Database.Server.GetCommand(
            $"CREATE TABLE {syntax.EnsureWrapped(tempTableName)} AS SELECT DISTINCT * FROM {discoveredTable.GetFullyQualifiedName()}", con);
        args.ExecuteNonQuery(cmd1);

        // Drop original table
        using var cmd2 = discoveredTable.Database.Server.GetCommand(
            $"DROP TABLE {discoveredTable.GetFullyQualifiedName()}", con);
        args.ExecuteNonQuery(cmd2);

        // Rename temp table to original name
        using var cmd3 = discoveredTable.Database.Server.GetCommand(
            $"ALTER TABLE {syntax.EnsureWrapped(tempTableName)} RENAME TO {syntax.EnsureWrapped(discoveredTable.GetRuntimeName())}", con);
        args.ExecuteNonQuery(cmd3);
    }

    public override bool IsEmpty(DatabaseOperationArgs args, DiscoveredTable discoveredTable)
    {
        using var connection = args.GetManagedConnection(discoveredTable);
        using var cmd = discoveredTable.Database.Server.GetCommand($"SELECT COUNT(*) FROM {discoveredTable.GetFullyQualifiedName()} LIMIT 1", connection);
        return Convert.ToInt32(args.ExecuteScalar(cmd), CultureInfo.InvariantCulture) == 0;
    }

    public override void RenameTable(DiscoveredTable discoveredTable, string newName, IManagedConnection connection)
    {
        using var cmd = discoveredTable.Database.Server.Helper.GetCommand(GetRenameTableSql(discoveredTable, newName), connection.Connection, connection.Transaction);
        cmd.ExecuteNonQuery();
    }

    protected override string GetRenameTableSql(DiscoveredTable discoveredTable, string newName)
    {
        var syntax = discoveredTable.GetQuerySyntaxHelper();
        return $"ALTER TABLE {discoveredTable.GetFullyQualifiedName()} RENAME TO {syntax.EnsureWrapped(newName)}";
    }

    public override void CreateIndex(DatabaseOperationArgs args, DiscoveredTable table, string indexName, DiscoveredColumn[] columns, bool unique = false)
    {
        var syntax = table.GetQuerySyntaxHelper();
        var columnNames = string.Join(", ", columns.Select(c => syntax.EnsureWrapped(c.GetRuntimeName())));
        var uniqueKeyword = unique ? "UNIQUE " : "";

        using var con = args.GetManagedConnection(table);
        using var cmd = table.Database.Server.GetCommand(
            $"CREATE {uniqueKeyword}INDEX {syntax.EnsureWrapped(indexName)} ON {table.GetFullyQualifiedName()} ({columnNames})", con);
        args.ExecuteNonQuery(cmd);
    }

    public override void DropIndex(DatabaseOperationArgs args, DiscoveredTable table, string indexName)
    {
        var syntax = table.GetQuerySyntaxHelper();
        using var con = args.GetManagedConnection(table);
        using var cmd = table.Database.Server.GetCommand($"DROP INDEX {syntax.EnsureWrapped(indexName)}", con);
        args.ExecuteNonQuery(cmd);
    }

    /// <summary>
    /// Creates a primary key constraint on existing table columns.
    /// </summary>
    /// <param name="args">Database operation arguments</param>
    /// <param name="table">The table to add primary key to</param>
    /// <param name="discoverColumns">The columns to include in the primary key</param>
    /// <exception cref="NotSupportedException">
    /// SQLite does not support adding primary keys to existing tables. Table recreation would be required.
    /// </exception>
    /// <remarks>
    /// Primary keys must be defined when creating the table in SQLite. To add a primary key to
    /// an existing table, you must recreate the table with the constraint.
    /// </remarks>
    public override void CreatePrimaryKey(DatabaseOperationArgs args, DiscoveredTable table, DiscoveredColumn[] discoverColumns)
    {
        // SQLite doesn't support adding primary keys to existing tables
        throw new NotSupportedException("SQLite does not support adding primary keys to existing tables. Table recreation would be required.");
    }

    /// <summary>
    /// Discovers all foreign key relationships for a table.
    /// </summary>
    /// <param name="discoveredTable">The table to discover relationships for</param>
    /// <param name="connection">The database connection</param>
    /// <param name="transaction">Optional transaction</param>
    /// <returns>An enumerable of discovered relationships</returns>
    /// <remarks>
    /// Uses PRAGMA foreign_key_list() to retrieve foreign key information. SQLite defaults to
    /// NO ACTION for cascade rules.
    /// </remarks>
    public override IEnumerable<DiscoveredRelationship> DiscoverRelationships(DiscoveredTable discoveredTable, DbConnection connection, IManagedTransaction? transaction = null)
    {
        var tableName = discoveredTable.GetRuntimeName();
        var relationships = new Dictionary<string, DiscoveredRelationship>();

        using var cmd = discoveredTable.Database.Server.Helper.GetCommand($"PRAGMA foreign_key_list({tableName})", connection);
        cmd.Transaction = transaction?.Transaction;

        using var r = cmd.ExecuteReader();
        while (r.Read())
        {
            var foreignKeyName = $"FK_{tableName}_{r["table"]}";
            var primaryKeyTable = (string)r["table"];
            var foreignKeyColumn = (string)r["from"];
            var primaryKeyColumn = (string)r["to"];

            if (!relationships.ContainsKey(foreignKeyName))
            {
                var pkTable = discoveredTable.Database.ExpectTable(primaryKeyTable);
                relationships[foreignKeyName] = new DiscoveredRelationship(
                    foreignKeyName,
                    pkTable,
                    discoveredTable,
                    CascadeRule.NoAction  // SQLite defaults to NO ACTION
                );
            }

            relationships[foreignKeyName].AddKeys(primaryKeyColumn, foreignKeyColumn, transaction);
        }

        return relationships.Values;
    }

    public override void FillDataTableWithTopX(DatabaseOperationArgs args, DiscoveredTable table, int topX, DataTable dt)
    {
        using var con = args.GetManagedConnection(table);
        using var cmd = table.Database.Server.GetCommand(GetTopXSqlForTable(table, topX), con);
        using var reader = cmd.ExecuteReader();

        // Manually populate DataTable to avoid reflection (AOT-compatible)
        // Add columns if DataTable is empty
        if (dt.Columns.Count == 0)
            for (var i = 0; i < reader.FieldCount; i++)
                dt.Columns.Add(reader.GetName(i), reader.GetFieldType(i));

        // Add rows
        while (reader.Read())
        {
            var row = dt.NewRow();
            for (var i = 0; i < reader.FieldCount; i++)
                row[i] = reader.GetValue(i);
            dt.Rows.Add(row);
        }
    }

    /// <summary>
    /// Checks if the table has a primary key using a database-specific SQL query (90-99% faster than discovering all columns).
    /// </summary>
    public override bool HasPrimaryKey(DiscoveredTable table, IManagedConnection connection)
    {
        var tableName = table.GetRuntimeName();
        using var cmd = table.Database.Server.Helper.GetCommand($"PRAGMA table_info({tableName})", connection.Connection);
        cmd.Transaction = connection.Transaction;

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            // Column 5 (pk) indicates if the column is part of the primary key
            // pk > 0 means it's part of the primary key
            if (reader.GetInt32(5) > 0)
                return true;
        }

        return false;
    }
}
