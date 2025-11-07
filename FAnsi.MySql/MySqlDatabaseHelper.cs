using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using MySqlConnector;

namespace FAnsi.Implementations.MySql;

public sealed class MySqlDatabaseHelper : DiscoveredDatabaseHelper
{
    public override IEnumerable<DiscoveredTableValuedFunction> ListTableValuedFunctions(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper,
        DbConnection connection, string database, DbTransaction? transaction = null) =>
        Enumerable.Empty<DiscoveredTableValuedFunction>();

    public override DiscoveredStoredprocedure[] ListStoredprocedures(DbConnectionStringBuilder builder, string database) => throw new NotImplementedException();

    public override IDiscoveredTableHelper GetTableHelper() => MySqlTableHelper.Instance;

    public override void DropDatabase(DiscoveredDatabase database)
    {
        using var con = (MySqlConnection)database.Server.GetConnection();
        con.Open();
        using var cmd = new MySqlCommand($"DROP DATABASE `{database.GetRuntimeName()}`", con);
        cmd.ExecuteNonQuery();
    }

    public override Dictionary<string, string> DescribeDatabase(DbConnectionStringBuilder builder, string database)
    {
        var mysqlBuilder = (MySqlConnectionStringBuilder)builder;

        return new Dictionary<string, string>
        {
            { "UserID", mysqlBuilder.UserID },
            { "Server", mysqlBuilder.Server },
            { "Database", mysqlBuilder.Database }
        };
    }

    protected override string GetCreateTableSqlLineForColumn(DatabaseColumnRequest col, string datatype, IQuerySyntaxHelper syntaxHelper)
    {
        // If user provides explicit collation, let MySQL handle the character set automatically
        // Do not add CHARACTER SET as it can conflict with explicit collations
        if (!string.IsNullOrWhiteSpace(col.Collation))
        {
            return $"{syntaxHelper.EnsureWrapped(col.ColumnName)} {datatype} {(col.Default != MandatoryScalarFunctions.None ? $"default {syntaxHelper.GetScalarFunctionSql(col.Default)}" : "")} COLLATE {col.Collation} {(col.AllowNulls && !col.IsPrimaryKey ? " NULL" : " NOT NULL")} {(col.IsAutoIncrement ? syntaxHelper.GetAutoIncrementKeywordIfAny() : "")}";
        }

        // Unicode columns use utf8mb4 character set when no explicit collation is provided
        if (col.TypeRequested?.Unicode == true)
        {
            // Use utf8mb4 with default binary collation for MySQL 8.0+ compatibility
            return $"{syntaxHelper.EnsureWrapped(col.ColumnName)} {datatype} CHARACTER SET utf8mb4 {(col.Default != MandatoryScalarFunctions.None ? $"default {syntaxHelper.GetScalarFunctionSql(col.Default)}" : "")} COLLATE utf8mb4_bin {(col.AllowNulls && !col.IsPrimaryKey ? " NULL" : " NOT NULL")} {(col.IsAutoIncrement ? syntaxHelper.GetAutoIncrementKeywordIfAny() : "")}";
        }

        // For non-Unicode columns without explicit collation, use base class behavior
        return base.GetCreateTableSqlLineForColumn(col, datatype, syntaxHelper);
    }

    public override DirectoryInfo Detach(DiscoveredDatabase database) => throw new NotImplementedException();

    public override void CreateBackup(DiscoveredDatabase discoveredDatabase, string backupName)
    {
        throw new NotImplementedException();
    }

    public override void CreateSchema(DiscoveredDatabase discoveredDatabase, string name)
    {

    }

    public override IEnumerable<DiscoveredTable> ListTables(DiscoveredDatabase parent, IQuerySyntaxHelper querySyntaxHelper, DbConnection connection, string database, bool includeViews, DbTransaction? transaction = null)
    {
        if (connection.State == ConnectionState.Closed)
            throw new InvalidOperationException("Expected connection to be open");

        var tables = new List<DiscoveredTable>();

        using (var cmd = new MySqlCommand($"SHOW FULL TABLES in `{database}`", (MySqlConnection)connection))
        {
            cmd.Transaction = transaction as MySqlTransaction;

            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                var isView = (string)r[1] == "VIEW";

                //if we are skipping views
                if (isView && !includeViews)
                    continue;

                //skip invalid table names
                if (!querySyntaxHelper.IsValidTableName((string)r[0], out _))
                    continue;

                tables.Add(new DiscoveredTable(parent, (string)r[0], querySyntaxHelper, null, isView ? TableType.View : TableType.Table));//this table fieldname will be something like Tables_in_mydbwhatevernameitis
            }
        }

        return tables.ToArray();
    }

}
