using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text.RegularExpressions;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.Constraints;
using FAnsi.Naming;
using MySqlConnector;

namespace FAnsi.Implementations.MySql;

public sealed partial class MySqlTableHelper : DiscoveredTableHelper
{
    public static readonly MySqlTableHelper Instance = new();

    private MySqlTableHelper() { }

    private static readonly Regex IntParentheses = IntParenthesesRe();
    private static readonly Regex SmallintParentheses = SmallintParenthesesRe();
    private static readonly Regex BitParentheses = BitParenthesesRe();

    public override IEnumerable<DiscoveredColumn> DiscoverColumns(DiscoveredTable discoveredTable, IManagedConnection connection,
        string database)
    {
        var tableName = discoveredTable.GetRuntimeName();

        using var cmd = discoveredTable.Database.Server.Helper.GetCommand(
            """
            SELECT * FROM information_schema.`COLUMNS`
            WHERE table_schema = @db
              AND table_name = @tbl
            """, connection.Connection);
        // Do not set cmd.Transaction for information_schema queries.
        // MySQL DDL operations (like ALTER TABLE) auto-commit, invalidating any active transaction.
        // Querying information_schema with a stale transaction reference can return empty results
        // due to snapshot isolation seeing pre-DDL schema state.

        var p = new MySqlParameter("@db", MySqlDbType.String)
        {
            Value = discoveredTable.Database.GetRuntimeName()
        };
        cmd.Parameters.Add(p);

        p = new MySqlParameter("@tbl", MySqlDbType.String)
        {
            Value = discoveredTable.GetRuntimeName()
        };
        cmd.Parameters.Add(p);

        using var r = cmd.ExecuteReader();
        if (!r.HasRows)
            throw new Exception($"Could not find any columns for table {tableName} in database {database}");

        while (r.Read())
        {
            var toAdd = new DiscoveredColumn(discoveredTable, (string)r["COLUMN_NAME"], YesNoToBool(r["IS_NULLABLE"]));

            if (r["COLUMN_KEY"].Equals("PRI"))
                toAdd.IsPrimaryKey = true;

            toAdd.IsAutoIncrement = r["Extra"] as string == "auto_increment";
            toAdd.Collation = r["COLLATION_NAME"] as string;

            //todo the only way to know if something in MySql is unicode is by r["character_set_name"]


            toAdd.DataType = new DiscoveredDataType(r, TrimIntDisplayValues(r["COLUMN_TYPE"].ToString()), toAdd);

            yield return toAdd;
        }

        r.Close();
    }

    private static bool YesNoToBool(object o)
    {
        if (o is bool b)
            return b;

        if (o == null || o == DBNull.Value)
            return false;

        return o.ToString() switch
        {
            "NO" => false,
            "YES" => true,
            _ => Convert.ToBoolean(o)
        };
    }


    private static string TrimIntDisplayValues(string? type)
    {
        if (string.IsNullOrWhiteSpace(type))
            return "";

        //See comments of int(5) means display 5 digits only it doesn't prevent storing larger numbers: https://stackoverflow.com/a/5634147/4824531

        if (IntParentheses.IsMatch(type))
            return IntParentheses.Replace(type, "int");

        if (SmallintParentheses.IsMatch(type))
            return SmallintParentheses.Replace(type, "smallint");

        if (BitParentheses.IsMatch(type))
            return BitParentheses.Replace(type, "bit");

        return type;
    }

    public override bool Exists(DiscoveredTable table, IManagedTransaction? transaction = null)
    {
        if (!table.Database.Exists())
            return false;

        using var connection = table.Database.Server.GetManagedConnection(transaction);

        // Use INFORMATION_SCHEMA.TABLES to check for table/view existence with a single targeted query
        var tableType = table.TableType switch
        {
            TableType.Table => "'BASE TABLE'",
            TableType.View => "'VIEW'",
            _ => "'BASE TABLE', 'VIEW'" // For unknown types, check both
        };

        var sql = $"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.`TABLES`
                WHERE table_schema = @db
                AND table_name = @tbl
                AND table_type IN ({tableType})
            )
            """;

        using var cmd = table.Database.Server.Helper.GetCommand(sql, connection.Connection);
        // Do not set cmd.Transaction for information_schema queries (see DiscoverColumns for details)

        var p = new MySqlParameter("@db", MySqlDbType.String)
        {
            Value = table.Database.GetRuntimeName()
        };
        cmd.Parameters.Add(p);

        p = new MySqlParameter("@tbl", MySqlDbType.String)
        {
            Value = table.GetRuntimeName()
        };
        cmd.Parameters.Add(p);

        var result = cmd.ExecuteScalar();
        return Convert.ToBoolean(result);
    }

    public override bool HasPrimaryKey(DiscoveredTable table, IManagedTransaction? transaction = null)
    {
        using var connection = table.Database.Server.GetManagedConnection(transaction);

        const string sql = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.`TABLE_CONSTRAINTS`
                WHERE table_schema = @db
                AND table_name = @tbl
                AND constraint_type = 'PRIMARY KEY'
            )
            """;

        using var cmd = table.Database.Server.Helper.GetCommand(sql, connection.Connection);
        // Do not set cmd.Transaction for information_schema queries (see DiscoverColumns for details)

        var p = new MySqlParameter("@db", MySqlDbType.String)
        {
            Value = table.Database.GetRuntimeName()
        };
        cmd.Parameters.Add(p);

        p = new MySqlParameter("@tbl", MySqlDbType.String)
        {
            Value = table.GetRuntimeName()
        };
        cmd.Parameters.Add(p);

        var result = cmd.ExecuteScalar();
        return Convert.ToBoolean(result);
    }

    public override IDiscoveredColumnHelper GetColumnHelper() => MySqlColumnHelper.Instance;

    public override void DropColumn(DbConnection connection, DiscoveredColumn columnToDrop)
    {
        using var cmd = new MySqlCommand(
            $"alter table {columnToDrop.Table.GetFullyQualifiedName()} drop column {columnToDrop.GetWrappedName()}", (MySqlConnection)connection);
        cmd.ExecuteNonQuery();
    }


    public override IEnumerable<DiscoveredParameter> DiscoverTableValuedFunctionParameters(DbConnection connection,
        DiscoveredTableValuedFunction discoveredTableValuedFunction, DbTransaction? transaction) =>
        throw new NotImplementedException();

    public override IBulkCopy BeginBulkInsert(DiscoveredTable discoveredTable, IManagedConnection connection,
        CultureInfo culture) => new MySqlBulkCopy(discoveredTable, connection, culture);

    public override DiscoveredRelationship[] DiscoverRelationships(DiscoveredTable table, DbConnection connection, IManagedTransaction? transaction = null)
    {
        var toReturn = new Dictionary<string, DiscoveredRelationship>();

        const string sql = """
                           SELECT DISTINCT
                           u.CONSTRAINT_NAME,
                           u.TABLE_SCHEMA,
                           u.TABLE_NAME,
                           u.COLUMN_NAME,
                           u.REFERENCED_TABLE_SCHEMA,
                           u.REFERENCED_TABLE_NAME,
                           u.REFERENCED_COLUMN_NAME,
                           c.DELETE_RULE
                           FROM
                               INFORMATION_SCHEMA.KEY_COLUMN_USAGE u
                           INNER JOIN
                               INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS c ON c.CONSTRAINT_NAME = u.CONSTRAINT_NAME
                           WHERE
                             u.REFERENCED_TABLE_SCHEMA = @db AND
                             u.REFERENCED_TABLE_NAME = @tbl
                           """;

        using (var cmd = new MySqlCommand(sql, (MySqlConnection)connection, (MySqlTransaction?)transaction?.Transaction))
        {
            var p = new MySqlParameter("@db", MySqlDbType.String)
            {
                Value = table.Database.GetRuntimeName()
            };
            cmd.Parameters.Add(p);

            p = new MySqlParameter("@tbl", MySqlDbType.String)
            {
                Value = table.GetRuntimeName()
            };
            cmd.Parameters.Add(p);

            using var dt = new DataTable();
            var da = table.Database.Server.GetDataAdapter(cmd);
            da.Fill(dt);

            foreach (DataRow r in dt.Rows)
            {
                var fkName = r["CONSTRAINT_NAME"].ToString();
                if (fkName == null) continue;

                //could be a 2+ columns foreign key?
                if (!toReturn.TryGetValue(fkName, out var current))
                {
                    var pkDb = r["REFERENCED_TABLE_SCHEMA"].ToString();
                    var pkTableName = r["REFERENCED_TABLE_NAME"].ToString();

                    var fkDb = r["TABLE_SCHEMA"].ToString();
                    var fkTableName = r["TABLE_NAME"].ToString();

                    if (pkDb == null || pkTableName == null || fkDb == null || fkTableName == null) continue;

                    var pktable = table.Database.Server.ExpectDatabase(pkDb).ExpectTable(pkTableName);
                    var fktable = table.Database.Server.ExpectDatabase(fkDb).ExpectTable(fkTableName);

                    //https://dev.mysql.com/doc/refman/8.0/en/referential-constraints-table.html
                    var deleteRuleString = r["DELETE_RULE"].ToString();

                    var deleteRule = deleteRuleString switch
                    {
                        "CASCADE" => CascadeRule.Delete,
                        "NO ACTION" => CascadeRule.NoAction,
                        "RESTRICT" => CascadeRule.NoAction,
                        "SET NULL" => CascadeRule.SetNull,
                        "SET DEFAULT" => CascadeRule.SetDefault,
                        _ => CascadeRule.Unknown
                    };

                    current = new DiscoveredRelationship(fkName, pktable, fktable, deleteRule);
                    toReturn.Add(current.Name, current);
                }

                var colName = r["COLUMN_NAME"].ToString();
                var refName = r["REFERENCED_COLUMN_NAME"].ToString();
                if (colName != null && refName != null)
                    current.AddKeys(refName, colName, transaction);
            }
        }

        return [.. toReturn.Values];
    }

    protected override string GetRenameTableSql(DiscoveredTable discoveredTable, string newName)
    {
        var syntax = discoveredTable.GetQuerySyntaxHelper();

        return $"RENAME TABLE {discoveredTable.GetWrappedName()} TO {syntax.EnsureWrapped(newName)};";
    }

    public override string GetTopXSqlForTable(IHasFullyQualifiedNameToo table, int topX) => $"SELECT * FROM {table.GetFullyQualifiedName()} LIMIT {topX}";


    public override void DropFunction(DbConnection connection, DiscoveredTableValuedFunction functionToDrop)
    {
        throw new NotImplementedException();
    }

    [GeneratedRegex(@"^int\(\d+\)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex IntParenthesesRe();

    [GeneratedRegex(@"^smallint\(\d+\)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex SmallintParenthesesRe();

    [GeneratedRegex(@"^bit\(\d+\)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex BitParenthesesRe();
}
