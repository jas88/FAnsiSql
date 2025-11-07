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
        // MySQL requires CHARACTER SET to be specified when using non-default collations
        // Unicode columns use utf8mb4 character set
        if (col.TypeRequested?.Unicode == true)
        {
            return $"{syntaxHelper.EnsureWrapped(col.ColumnName)} {datatype} CHARACTER SET utf8mb4 {(col.Default != MandatoryScalarFunctions.None ? $"default {syntaxHelper.GetScalarFunctionSql(col.Default)}" : "")} COLLATE {col.Collation ?? "utf8mb4_bin"} {(col is { AllowNulls: true, IsPrimaryKey: false } ? " NULL" : " NOT NULL")} {(col.IsAutoIncrement ? syntaxHelper.GetAutoIncrementKeywordIfAny() : "")}";
        }

        // Non-Unicode columns with explicit collations need CHARACTER SET specified
        // Extract character set from collation (e.g., "latin1_german1_ci" -> "latin1")
        if (!string.IsNullOrWhiteSpace(col.Collation))
        {
            var underscoreIndex = col.Collation.IndexOf('_');
            if (underscoreIndex > 0)
            {
                var charset = col.Collation.Substring(0, underscoreIndex);
                return $"{syntaxHelper.EnsureWrapped(col.ColumnName)} {datatype} CHARACTER SET {charset} {(col.Default != MandatoryScalarFunctions.None ? $"default {syntaxHelper.GetScalarFunctionSql(col.Default)}" : "")} COLLATE {col.Collation} {(col is { AllowNulls: true, IsPrimaryKey: false } ? " NULL" : " NOT NULL")} {(col.IsAutoIncrement ? syntaxHelper.GetAutoIncrementKeywordIfAny() : "")}";
            }
        }

        // Default case - no special CHARACTER SET handling needed
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
