using System.Text.RegularExpressions;
using FAnsi;
using FAnsi.Implementations.MicrosoftSQL;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

/// <summary>
///     Checks, without a database, of the names pivots give what they make on a connection besides their columns: on SQL
///     Server the temporary table of the pivot values.  A name is made from a hash of the query, so that it is not one a
///     caller would use and so that the same query always gives the same SQL, which callers that cache results by their
///     SQL (as RDMP does) rely on.
/// </summary>
internal sealed partial class PivotObjectNameSqlTests
{
    private const DatabaseType SqlServer = DatabaseType.MicrosoftSQLServer;

    /// <summary>
    ///     Loads the implementations whose SQL is checked here.  These tests use no database, so they have no base class
    ///     that loads them.
    /// </summary>
    [OneTimeSetUp]
    public void LoadImplementations() => MicrosoftSQLImplementation.EnsureLoaded();

    /// <summary>
    ///     SQL Server ranks the pivot values into a temporary table named #fansi_pivot_ and 16 hex digits, and uses that
    ///     one name throughout the batch: to drop a table an earlier run of the query left, to fill it, to read the values
    ///     from it, to join to it in the pivot itself and to drop it at the end
    /// </summary>
    [TestCase(false)]
    [TestCase(true)]
    public void MicrosoftSql_ValuesTableHasOneNameThroughoutTheBatch(bool withAxis)
    {
        var sql = PivotColumnNameSqlTests.BuildPivot(SqlServer, withAxis);
        var table = GetTemporaryTable(sql);

        Assert.Multiple(() =>
        {
            Assert.That(table, Does.Match("^#fansi_pivot_[0-9a-f]{16}$"));
            Assert.That(sql, Does.Contain($"IF OBJECT_ID('tempdb..{table}') IS NOT NULL DROP TABLE {table};"));
            Assert.That(sql, Does.Contain($"INTO {table}"));
            Assert.That(sql, Does.Contain($"(SELECT COUNT(*) FROM {table});"));
            Assert.That(sql, Does.Contain($"FROM {table} WHERE fansi_rank = @fansi_pivot_rank;"));
            Assert.That(sql, Does.Contain($"LEFT JOIN {table} fansi_pivot_values ON "));
            Assert.That(sql, Does.EndWith($"DROP TABLE {table};"));
        });
    }

    /// <summary>
    ///     The name of the temporary table comes from the query alone: the same query gets the same name each time, and
    ///     a query that differs in its table, in what comes before its SELECT or in its axis gets another
    /// </summary>
    [Test]
    public void MicrosoftSql_ValuesTableIsNamedAfterTheQuery()
    {
        var table = GetTemporaryTable(PivotColumnNameSqlTests.BuildPivot(SqlServer));
        var again = GetTemporaryTable(PivotColumnNameSqlTests.BuildPivot(SqlServer));

        string[] others =
        [
            GetTemporaryTable(PivotColumnNameSqlTests.BuildPivot(SqlServer, fromSql: "[fansi]..[Other]")),
            GetTemporaryTable(PivotColumnNameSqlTests.BuildPivot(SqlServer, preSelectSql: "DECLARE @fansi_test int = 1;")),
            GetTemporaryTable(PivotColumnNameSqlTests.BuildPivot(SqlServer, withAxis: true))
        ];

        Assert.Multiple(() =>
        {
            Assert.That(again, Is.EqualTo(table));
            Assert.That(others, Has.None.EqualTo(table));
            Assert.That(others, Is.Unique);
        });
    }

    /// <summary>
    ///     A pivot leaves alone a temporary table of the caller's, even one called #fansi_pivot_values as the table of a
    ///     pivot once was: here the table being pivoted.  The batch reads it, and neither drops it nor writes to it.
    /// </summary>
    [TestCase(false)]
    [TestCase(true)]
    public void MicrosoftSql_TemporaryTableOfTheCallerIsLeftAlone(bool withAxis)
    {
        const string callersTable = "#fansi_pivot_values";

        var sql = PivotColumnNameSqlTests.BuildPivot(SqlServer, withAxis, fromSql: callersTable);
        var tables = GetTemporaryTables(sql);

        Assert.Multiple(() =>
        {
            Assert.That(tables, Has.Count.EqualTo(2), "the table of the caller and the table of the pivot");
            Assert.That(sql, Does.Contain($"FROM {callersTable}"));
            Assert.That(sql, Does.Not.Contain($"DROP TABLE {callersTable}"));
            Assert.That(sql, Does.Not.Contain($"INTO {callersTable}"));
            Assert.That(sql, Does.Not.Contain($"OBJECT_ID('tempdb..{callersTable}')"));
        });
    }

    /// <summary>
    ///     The one temporary table that <paramref name="sql" /> uses
    /// </summary>
    private static string GetTemporaryTable(string sql)
    {
        var tables = GetTemporaryTables(sql);
        Assert.That(tables, Has.Count.EqualTo(1), "The batch should use one temporary table");
        return tables[0];
    }

    /// <summary>
    ///     The names of the temporary tables that <paramref name="sql" /> uses
    /// </summary>
    private static List<string> GetTemporaryTables(string sql) =>
        TemporaryTable().Matches(sql).Select(static m => m.Value).Distinct().ToList();

    [GeneratedRegex(@"#\w+", RegexOptions.CultureInvariant)]
    private static partial Regex TemporaryTable();
}
