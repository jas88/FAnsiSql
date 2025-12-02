using System;
using System.Collections.Generic;
using System.Text;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementations.Sqlite.Aggregation;
using FAnsi.Implementations.Sqlite.Update;

namespace FAnsi.Implementations.Sqlite;

/// <summary>
/// SQLite-specific implementation of query syntax helper. Provides SQL syntax rules, escaping,
/// and query generation for SQLite databases.
/// </summary>
/// <remarks>
/// <para>SQLite uses square brackets [] for identifier quoting and has different function names
/// compared to other databases. It also uses LIMIT for TOP queries.</para>
/// <para>SQLite has very permissive naming rules - identifiers can be up to 1024 characters.</para>
/// </remarks>
public sealed class SqliteQuerySyntaxHelper : QuerySyntaxHelper
{
    /// <summary>
    /// Gets the singleton instance of <see cref="SqliteQuerySyntaxHelper"/>.
    /// </summary>
    public static readonly SqliteQuerySyntaxHelper Instance = new();

    /// <summary>
    /// Gets the maximum length for database names (1024 characters).
    /// </summary>
    /// <remarks>
    /// SQLite has no strict limit, but 1024 is a reasonable practical maximum.
    /// </remarks>
    public override int MaximumDatabaseLength => 1024;

    /// <summary>
    /// Gets the maximum length for table names (1024 characters).
    /// </summary>
    public override int MaximumTableLength => 1024;

    /// <summary>
    /// Gets the maximum length for column names (1024 characters).
    /// </summary>
    public override int MaximumColumnLength => 1024;

    /// <summary>
    /// Gets the opening qualifier character for identifiers (double quote).
    /// </summary>
    /// <remarks>
    /// SQLite supports both square brackets [] (SQL Server style) and double quotes "" (SQL standard).
    /// We use double quotes because square brackets have issues when the identifier contains ][
    /// sequences, which causes parsing errors.
    /// </remarks>
    public override string OpenQualifier => "\"";

    /// <summary>
    /// Gets the closing qualifier character for identifiers (double quote).
    /// </summary>
    public override string CloseQualifier => "\"";

    /// <summary>
    /// Gets the characters that are illegal in database/table/column names.
    /// </summary>
    /// <remarks>
    /// SQLite database names are file paths, so dots and parentheses are valid.
    /// Returns empty array since SQLite has very permissive naming rules.
    /// </remarks>
    public override char[] IllegalNameChars => [];

    private SqliteQuerySyntaxHelper() : base(SqliteTypeTranslater.Instance, SqliteAggregateHelper.Instance, SqliteUpdateHelper.Instance, DatabaseType.Sqlite)
    {
    }

    /// <summary>
    /// Indicates whether SQLite supports embedded parameters in queries.
    /// </summary>
    /// <returns>True (SQLite supports parameterized queries)</returns>
    public override bool SupportsEmbeddedParameters() => true;

    /// <summary>
    /// Gets the runtime name for SQLite identifiers.
    /// </summary>
    /// <param name="s">The identifier (which may be quoted or unquoted)</param>
    /// <returns>The unquoted identifier name</returns>
    /// <remarks>
    /// SQLite allows almost any characters in identifiers when quoted.
    /// The base implementation respects IllegalNameChars (which is empty for SQLite),
    /// handles dots inside wrapped identifiers correctly, and calls UnescapeWrappedNameBody
    /// (which SQLite overrides to handle doubled quotes).
    /// </remarks>
    public override string? GetRuntimeName(string? s) => base.GetRuntimeName(s);

    /// <inheritdoc />
    public override string EnsureWrappedImpl(string databaseOrTableName) => $"\"{GetRuntimeNameWithEscapedQuotes(databaseOrTableName)}\"";

    /// <summary>
    /// Returns the runtime name of the string with all double quotes escaped (but resulting string is not wrapped in quotes itself).
    /// </summary>
    /// <param name="s">The string to escape</param>
    /// <returns>The escaped string with " replaced by ""</returns>
    /// <remarks>
    /// SQLite escapes double quotes by doubling them within quoted identifiers.
    /// </remarks>
    private string GetRuntimeNameWithEscapedQuotes(string s) => GetRuntimeName(s)!.Replace("\"", "\"\"");

    /// <inheritdoc />
    protected override string UnescapeWrappedNameBody(string name) => name.Replace("\"\"", "\"");

    /// <summary>
    /// Ensures a table name is fully qualified with database and schema.
    /// </summary>
    /// <param name="databaseName">The database name (ignored for SQLite)</param>
    /// <param name="schemaName">The schema name (ignored for SQLite)</param>
    /// <param name="tableName">The table name</param>
    /// <returns>The wrapped table name</returns>
    /// <remarks>
    /// SQLite doesn't support schemas in the traditional sense. Only the table name is used.
    /// For multi-database support, use ATTACH DATABASE.
    /// </remarks>
    public override string EnsureFullyQualified(string? databaseName, string? schemaName, string tableName)
    {
        // SQLite doesn't support schemas in the same way as other databases
        // Just return the wrapped table name
        return EnsureWrapped(tableName)!;
    }

    /// <summary>
    /// Specifies how to achieve TOP X functionality in SQLite.
    /// </summary>
    /// <param name="x">The number of rows to return</param>
    /// <returns>A TopXResponse using LIMIT as a postfix clause</returns>
    /// <remarks>
    /// SQLite uses LIMIT at the end of queries, unlike SQL Server's TOP at the beginning.
    /// </remarks>
    public override TopXResponse HowDoWeAchieveTopX(int x) => new($"LIMIT {x}", QueryComponent.Postfix);

    /// <summary>
    /// Gets the SQL for declaring a parameter.
    /// </summary>
    /// <param name="proposedNewParameterName">The parameter name</param>
    /// <param name="sqlType">The SQL type</param>
    /// <returns>A comment (SQLite doesn't require parameter declarations)</returns>
    /// <remarks>
    /// SQLite doesn't require explicit parameter declarations like SQL Server's DECLARE.
    /// Parameters are automatically typed based on usage.
    /// </remarks>
    public override string GetParameterDeclaration(string proposedNewParameterName, string sqlType) =>
        // SQLite doesn't require parameter declaration like SQL Server
        $"/* {proposedNewParameterName} */";

    /// <summary>
    /// Escapes special characters in a SQL string literal.
    /// </summary>
    /// <param name="sql">The string to escape</param>
    /// <returns>The escaped string with single quotes doubled</returns>
    /// <remarks>
    /// SQLite uses single quotes for string literals and escapes them by doubling.
    /// </remarks>
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

    /// <summary>
    /// Gets the SQL for mandatory scalar functions.
    /// </summary>
    /// <param name="function">The function to get SQL for</param>
    /// <returns>The SQLite-specific function SQL</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if the function is not recognized</exception>
    /// <remarks>
    /// <para>SQLite function mappings:</para>
    /// <list type="bullet">
    /// <item><description>GetTodaysDate: date('now')</description></item>
    /// <item><description>GetGuid: lower(hex(randomblob(16))) - pseudo-UUID</description></item>
    /// <item><description>Len: LENGTH</description></item>
    /// </list>
    /// <para>Note: SQLite doesn't have native UUID/GUID support; randomblob(16) generates a random 16-byte value.</para>
    /// </remarks>
    public override string GetScalarFunctionSql(MandatoryScalarFunctions function) =>
        function switch
        {
            MandatoryScalarFunctions.GetTodaysDate => "date('now')",
            MandatoryScalarFunctions.GetGuid => "lower(hex(randomblob(16)))",  // SQLite doesn't have native UUID
            MandatoryScalarFunctions.Len => "LENGTH",
            _ => throw new ArgumentOutOfRangeException(nameof(function))
        };

    /// <summary>
    /// Gets the auto-increment keyword for SQLite.
    /// </summary>
    /// <returns>Empty string (SQLite uses INTEGER PRIMARY KEY for auto-increment)</returns>
    /// <remarks>
    /// SQLite automatically provides auto-increment behavior for INTEGER PRIMARY KEY columns.
    /// The AUTOINCREMENT keyword exists but is rarely needed and has performance implications.
    /// </remarks>
    public override string GetAutoIncrementKeywordIfAny() => ""; // SQLite uses INTEGER PRIMARY KEY for auto-increment

    /// <inheritdoc />
    public override Dictionary<string, string> GetSQLFunctionsDictionary() => Functions;

    /// <summary>
    /// Dictionary mapping common SQL function names to their SQLite equivalents with usage examples.
    /// </summary>
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

    /// <summary>
    /// Gets the SQL for computing MD5 hash of a value.
    /// </summary>
    /// <param name="selectSql">The SQL expression to hash</param>
    /// <returns>Never returns (always throws)</returns>
    /// <exception cref="NotSupportedException">
    /// SQLite does not have a built-in MD5 function. Use SQLite extensions or application-level hashing.
    /// </exception>
    /// <remarks>
    /// SQLite can be extended with crypto functions via extensions like SQLCipher or custom functions.
    /// </remarks>
    public override string HowDoWeAchieveMd5(string selectSql) =>
        throw new NotSupportedException("SQLite does not have a built-in MD5 function");
}
