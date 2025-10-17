using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using FAnsi.Naming;
using Microsoft.Data.Sqlite;

namespace FAnsi.Implementations.Sqlite;

public sealed class SqliteTableHelper : DiscoveredTableHelper
{
    public override string GetTopXSqlForTable(IHasFullyQualifiedNameToo table, int topX) =>
        $"SELECT * FROM {table.GetFullyQualifiedName()} LIMIT {topX}";

    public override IEnumerable<DiscoveredColumn> DiscoverColumns(DiscoveredTable discoveredTable, IManagedConnection connection, string database)
    {
        var tableName = discoveredTable.GetRuntimeName();

        using var cmd = discoveredTable.Database.Server.Helper.GetCommand($"PRAGMA table_info({tableName})", connection.Connection);
        cmd.Transaction = connection.Transaction;

        using var r = cmd.ExecuteReader();
        if (!r.HasRows)
            throw new Exception($"Could not find any columns for table {tableName} in database {database}");

        while (r.Read())
        {
            var columnName = (string)r["name"];
            var dataType = (string)r["type"];
            var notNull = Convert.ToInt64(r["notnull"]) == 1;
            var isPrimaryKey = Convert.ToInt64(r["pk"]) > 0;

            var toAdd = new DiscoveredColumn(discoveredTable, columnName, !notNull)
            {
                IsPrimaryKey = isPrimaryKey,
                IsAutoIncrement = isPrimaryKey && dataType.Contains("INTEGER", StringComparison.OrdinalIgnoreCase)
            };

            toAdd.DataType = new DiscoveredDataType(r, dataType, toAdd);

            yield return toAdd;
        }
    }

    public override IDiscoveredColumnHelper GetColumnHelper() => new SqliteColumnHelper();

    public override void DropFunction(DbConnection connection, DiscoveredTableValuedFunction functionToDrop)
    {
        throw new NotSupportedException("SQLite does not support table-valued functions");
    }

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
        using var identityCmd = discoveredTable.Database.Server.GetCommand("SELECT last_insert_rowid()", cmd.Connection);
        identityCmd.Transaction = transaction?.Transaction;
        return Convert.ToInt32(identityCmd.ExecuteScalar());
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
        return Convert.ToInt32(args.ExecuteScalar(cmd)) == 0;
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

    public override void CreatePrimaryKey(DatabaseOperationArgs args, DiscoveredTable table, DiscoveredColumn[] discoverColumns)
    {
        // SQLite doesn't support adding primary keys to existing tables
        throw new NotSupportedException("SQLite does not support adding primary keys to existing tables. Table recreation would be required.");
    }

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
        dt.Load(reader);
    }
}