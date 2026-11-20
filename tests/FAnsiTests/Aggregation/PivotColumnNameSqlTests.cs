using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using FAnsi.Implementations.Oracle.Aggregation;
using FAnsi.Implementations.PostgreSql;
using FAnsi.Implementations.Sqlite;
using NUnit.Framework;

namespace FAnsiTests.Aggregation;

/// <summary>
///     Checks, without a database, of the SQL with which the Oracle and PostgreSQL pivots name their columns.  The names
///     are worked out by that SQL when it runs, which the Test_Pivot_ColumnNamesAreValidAndUnique and
///     Test_Pivot_DateColumnNames tests of <see cref="PivotAggregationTestsBase" /> check on the databases.
/// </summary>
internal sealed class PivotColumnNameSqlTests
{
    /// <summary>
    ///     Loads every implementation, not only the two tested here: the first <see cref="DiscoveredServer" /> made sets up
    ///     the connection string keywords of just the implementations loaded at the time, so a DBMS loaded after that
    ///     cannot be used
    /// </summary>
    [OneTimeSetUp]
    public void LoadImplementations()
    {
        MicrosoftSQLImplementation.EnsureLoaded();
        MySqlImplementation.EnsureLoaded();
        OracleImplementation.EnsureLoaded();
        PostgreSqlImplementation.EnsureLoaded();
        SqliteImplementation.EnsureLoaded();
    }

    /// <summary>
    ///     Oracle names a date or timestamp column after the whole date and time, in a format chosen by the type DBMS_SQL
    ///     describes, rather than after TO_CHAR alone, which follows the session's NLS formats
    /// </summary>
    [Test]
    public void Oracle_DatesAreWrittenOutInFull()
    {
        var sql = BuildPivot(DatabaseType.Oracle);

        Assert.Multiple(() =>
        {
            Assert.That(sql, Does.Contain("DBMS_SQL.DESCRIBE_COLUMNS2("));
            Assert.That(sql, Does.Contain($"WHEN 12 THEN 'TO_CHAR(' || fansi_value_sql || ', ''{OraclePivotBlock.DateFormat}'')'"));
            Assert.That(sql, Does.Contain($"WHEN 180 THEN 'RTRIM(RTRIM(TO_CHAR(' || fansi_value_sql || ', ''{OraclePivotBlock.TimestampFormat}''), ''0''), ''.'')'"));
            Assert.That(sql, Does.Contain("WHEN 181 THEN 'RTRIM(RTRIM(TO_CHAR(SYS_EXTRACT_UTC("));
            Assert.That(OraclePivotBlock.DateFormat, Does.Contain("YYYY").And.Contain("HH24:MI:SS"));
        });
    }

    /// <summary>
    ///     Oracle names a column after its value without the double quotes and NUL characters a name cannot hold, within
    ///     128 bytes, and gives a value left with no name or the name of an earlier column a suffix
    /// </summary>
    [Test]
    public void Oracle_EachColumnGetsAValidNameOfItsOwn()
    {
        var sql = BuildPivot(DatabaseType.Oracle);

        Assert.Multiple(() =>
        {
            Assert.That(OraclePivotBlock.MaxNameBytes, Is.EqualTo(128));
            Assert.That(sql, Does.Contain("SUBSTR(REPLACE(REPLACE(fansi_text, '\"'), CHR(0)), 1, 128)"));
            Assert.That(sql, Does.Contain("LENGTHB(fansi_name) + NVL(LENGTHB(fansi_suffix), 0) > 128"));
            Assert.That(sql,
                Does.Contain("EXIT WHEN fansi_pivot_name IS NOT NULL AND NOT fansi_pivot_names.EXISTS(fansi_pivot_name);"));
            Assert.That(sql, Does.Contain("AS \"' || fansi_pivot_name || '\"'"));
        });
    }

    /// <summary>
    ///     PostgreSQL cuts a name to max_identifier_length bytes itself, so the pivot cuts each name first and gives a value
    ///     whose name an earlier column has a suffix
    /// </summary>
    [Test]
    public void PostgreSql_EachColumnGetsANameOfItsOwn()
    {
        var sql = BuildPivot(DatabaseType.PostgreSql);

        Assert.Multiple(() =>
        {
            Assert.That(sql, Does.Contain("current_setting('max_identifier_length')"));
            Assert.That(sql,
                Does.Contain("EXIT WHEN fansi_pivot_name <> '' AND NOT (fansi_pivot_name = ANY (fansi_pivot_names));"));
            Assert.That(sql, Does.Contain("AS %I', fansi_pivot_value, fansi_pivot_name)"));
        });
    }

    /// <summary>
    ///     Every string literal of the Oracle block is closed, as a quote doubled to escape it comes in a pair
    /// </summary>
    [Test]
    public void Oracle_StringLiteralsAreClosed() =>
        Assert.That(BuildPivot(DatabaseType.Oracle).Count(static c => c == '\'') % 2, Is.Zero);

    /// <summary>
    ///     The same query gives the same SQL each time it is built, which callers that cache results by their SQL (as RDMP
    ///     does) rely on
    /// </summary>
    [TestCase(DatabaseType.Oracle)]
    [TestCase(DatabaseType.PostgreSql)]
    public void Pivot_IsTheSameEachTime(DatabaseType type)
    {
        var first = BuildPivot(type);
        Assert.That(BuildPivot(type), Is.EqualTo(first));
    }

    /// <summary>
    ///     The SQL of a pivot of the number of records for each Val, in a row for each Grp, of a table that need not exist
    /// </summary>
    private static string BuildPivot(DatabaseType type)
    {
        var server = type == DatabaseType.Oracle
            ? new DiscoveredServer("Data Source=localhost:1521/XEPDB1;User Id=fansi", type)
            : new DiscoveredServer("Host=localhost;Database=fansi;Username=fansi", type);
        var tbl = server.ExpectDatabase("fansi").ExpectTable("PivotColumnNames");
        var syntax = tbl.GetQuerySyntaxHelper();
        var grp = syntax.EnsureWrapped("Grp");
        var val = syntax.EnsureWrapped("Val");

        List<CustomLine> lines =
        [
            new("SELECT", QueryComponent.SELECT),
            new("count(*) as MyCount,", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.CountFunction },
            new($"{grp} as Grp,", QueryComponent.QueryTimeColumn),
            new($"{val} as Val", QueryComponent.QueryTimeColumn) { Role = CustomLineRole.Pivot },
            new($"FROM {tbl.GetFullyQualifiedName()}", QueryComponent.FROM),
            new("GROUP BY", QueryComponent.GroupBy),
            new($"{grp},", QueryComponent.GroupBy),
            new(val, QueryComponent.GroupBy) { Role = CustomLineRole.Pivot }
        ];

        return syntax.AggregateHelper.BuildAggregate(lines, null);
    }
}
