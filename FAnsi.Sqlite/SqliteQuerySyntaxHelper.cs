using System;
using System.Collections.Generic;
using System.Text;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementations.Sqlite.Aggregation;
using FAnsi.Implementations.Sqlite.Update;

namespace FAnsi.Implementations.Sqlite;

public sealed class SqliteQuerySyntaxHelper : QuerySyntaxHelper
{
    public static readonly SqliteQuerySyntaxHelper Instance = new();
    
    public override int MaximumDatabaseLength => 1024;  // SQLite has no specific limit, but let's be reasonable
    public override int MaximumTableLength => 1024;
    public override int MaximumColumnLength => 1024;

    public override string OpenQualifier => "[";
    public override string CloseQualifier => "]";

    private SqliteQuerySyntaxHelper() : base(SqliteTypeTranslater.Instance, SqliteAggregateHelper.Instance, SqliteUpdateHelper.Instance, DatabaseType.Sqlite)
    {
    }

    public override bool SupportsEmbeddedParameters() => true;

    public override string EnsureWrappedImpl(string databaseOrTableName) => $"[{GetRuntimeNameWithEscapedBrackets(databaseOrTableName)}]";

    /// <summary>
    /// Returns the runtime name of the string with all brackets escaped (but resulting string is not wrapped in brackets itself)
    /// </summary>
    /// <param name="s"></param>
    /// <returns></returns>
    private string? GetRuntimeNameWithEscapedBrackets(string s) => GetRuntimeName(s)?.Replace("]", "]]");

    protected override string UnescapeWrappedNameBody(string name) => name.Replace("]]", "]");

    public override string EnsureFullyQualified(string? databaseName, string? schema, string tableName)
    {
        // SQLite doesn't support schemas in the same way as other databases
        // Just return the wrapped table name
        return EnsureWrapped(tableName);
    }

    public override TopXResponse HowDoWeAchieveTopX(int x) => new($"LIMIT {x}", QueryComponent.Postfix);

    public override string GetParameterDeclaration(string proposedNewParameterName, string sqlType) =>
        // SQLite doesn't require parameter declaration like SQL Server
        $"/* {proposedNewParameterName} */";

    public override string Escape(string sql)
    {
        // SQLite uses single quotes for string literals
        var r = new StringBuilder(sql.Length);
        foreach (var c in sql)
            r.Append(c switch
            {
                '\'' => "''",  // Escape single quotes by doubling them
                _ => $"{c}"
            });
        return r.ToString();
    }

    public override string GetScalarFunctionSql(MandatoryScalarFunctions function) =>
        function switch
        {
            MandatoryScalarFunctions.GetTodaysDate => "date('now')",
            MandatoryScalarFunctions.GetGuid => "lower(hex(randomblob(16)))",  // SQLite doesn't have native UUID
            MandatoryScalarFunctions.Len => "LENGTH",
            _ => throw new ArgumentOutOfRangeException(nameof(function))
        };

    public override string GetAutoIncrementKeywordIfAny() => ""; // SQLite uses INTEGER PRIMARY KEY for auto-increment

    public override Dictionary<string, string> GetSQLFunctionsDictionary() => Functions;

    private static readonly Dictionary<string, string> Functions = new()
    {
        {"left", "SUBSTR(string, 1, length)"},
        {"right", "SUBSTR(string, -length)"},
        {"upper", "UPPER(string)"},
        {"substring", "SUBSTR(str, start, length)"},
        {"dateadd", "date(date, '+' || value || ' ' || unit)"},  // SQLite date functions
        {"datediff", "julianday(date1) - julianday(date2)"},
        {"getdate", "datetime('now')"},
        {"now", "datetime('now')"},
        {"cast", "CAST(value AS type)"},
        {"convert", "CAST(value AS type)"},  // SQLite doesn't have CONVERT, use CAST
        {"case", "CASE WHEN x=y THEN 'something' WHEN x=z THEN 'something2' ELSE 'something3' END"}
    };

    public override string HowDoWeAchieveMd5(string selectSql) => 
        throw new NotSupportedException("SQLite does not have a built-in MD5 function");
}