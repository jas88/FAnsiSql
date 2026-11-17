using System.Data;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using FAnsi.Implementations.PostgreSql;
using FAnsi.Implementations.Sqlite;
using NUnit.Framework;

namespace FAnsiTests.TypeTranslation;

internal sealed class TypeTranslaterUnitTests
{
    [OneTimeSetUp]
    public void LoadImplementations()
    {
        // A DiscoveredColumn needs the implementation of its DBMS
#pragma warning disable CS0618 // Type or member is obsolete
        ImplementationManager.Load<MicrosoftSQLImplementation>();
        ImplementationManager.Load<MySqlImplementation>();
        ImplementationManager.Load<OracleImplementation>();
        ImplementationManager.Load<PostgreSqlImplementation>();
        ImplementationManager.Load<SqliteImplementation>();
#pragma warning restore CS0618 // Type or member is obsolete
    }

    /// <summary>
    ///     IsSupportedType is a support check for FAnsi not the DBMS.  This test shows that FAnsi's view of 'what is a string'
    ///     is pretty
    ///     broad.  We don't want to bind to DBMS / API since that would be too brittle.
    /// </summary>
    /// <param name="dbType"></param>
    /// <param name="sqlDbType"></param>
    /// <param name="expectedOutcome"></param>
    [TestCase(DatabaseType.MicrosoftSQLServer, "varchar2(10)", true)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "monkeychar7", true)]
    [TestCase(DatabaseType.MySql, "varchar2(10)", true)]
    [TestCase(DatabaseType.MySql, "monkeychar7", true)]
    [TestCase(DatabaseType.Oracle, "varchar2(10)", true)]
    [TestCase(DatabaseType.Oracle, "monkeychar7", true)]
    [TestCase(DatabaseType.PostgreSql, "varchar2(10)", true)]
    [TestCase(DatabaseType.PostgreSql, "monkeychar7", true)]
    [TestCase(DatabaseType.Sqlite, "varchar2(10)", true)]
    [TestCase(DatabaseType.Sqlite, "monkeychar7", true)]
    public void Test_IsSupportedType(DatabaseType dbType, string sqlDbType, bool expectedOutcome)
    {
        var tt = GetTypeTranslater(dbType);
        Assert.That(tt.IsSupportedSQLDBType(sqlDbType), Is.EqualTo(expectedOutcome),
            $"Unexpected result for IsSupportedSQLDBType with {dbType}.  Input was '{sqlDbType}' expected {expectedOutcome}");
    }

    /// <summary>
    ///     Each ADO.NET provider has its own idea of what a <see cref="DbType" /> means.  Npgsql sends
    ///     <see cref="DbType.DateTime" /> as 'timestamp with time zone' and <see cref="DbType.DateTime2" /> as 'timestamp
    ///     without time zone', the other providers expect <see cref="DbType.DateTime" /> for all of their date types.
    /// </summary>
    [TestCase(DatabaseType.PostgreSql, "timestamp without time zone", DbType.DateTime2)]
    [TestCase(DatabaseType.PostgreSql, "timestamp", DbType.DateTime2)]
    [TestCase(DatabaseType.PostgreSql, "timestamp with time zone", DbType.DateTime)]
    [TestCase(DatabaseType.PostgreSql, "time without time zone", DbType.Time)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "datetime2", DbType.DateTime)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "datetime", DbType.DateTime)]
    [TestCase(DatabaseType.MySql, "datetime", DbType.DateTime)]
    [TestCase(DatabaseType.Oracle, "DATE", DbType.DateTime)]
    [TestCase(DatabaseType.Oracle, "TIMESTAMP(6)", DbType.DateTime)]
    [TestCase(DatabaseType.Sqlite, "datetime", DbType.DateTime)]
    public void Test_GetDbTypeForSQLDBType_Dates(DatabaseType dbType, string sqlDbType, DbType expected) =>
        Assert.That(GetTypeTranslater(dbType).GetDbTypeForSQLDBType(sqlDbType), Is.EqualTo(expected));

    /// <summary>
    ///     The <see cref="TypeGuesser.Guesser" /> for a column starts out guessing a string type that is Unicode if the
    ///     column is, so that a type computed from it for wider data stays Unicode
    /// </summary>
    [TestCase(DatabaseType.MicrosoftSQLServer, "nvarchar(10)", true)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "nchar(10)", true)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "ntext", true)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "nvarchar(max)", true)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "varchar(10)", false)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "char(10)", false)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "text", false)]
    [TestCase(DatabaseType.MicrosoftSQLServer, "varchar(max)", false)]
    [TestCase(DatabaseType.Oracle, "nvarchar2(10)", true)]
    [TestCase(DatabaseType.Oracle, "nchar(10)", true)]
    [TestCase(DatabaseType.Oracle, "nclob", true)]
    [TestCase(DatabaseType.Oracle, "varchar2(10)", false)]
    [TestCase(DatabaseType.Oracle, "char(10)", false)]
    [TestCase(DatabaseType.Oracle, "clob", false)]
    [TestCase(DatabaseType.MySql, "varchar(10)", false)]
    [TestCase(DatabaseType.MySql, "text", false)]
    [TestCase(DatabaseType.PostgreSql, "character varying(10)", false)]
    [TestCase(DatabaseType.PostgreSql, "text", false)]
    [TestCase(DatabaseType.Sqlite, "character varying(10)", false)]
    [TestCase(DatabaseType.Sqlite, "TEXT", false)]
    public void GetGuesser_StringColumn_KeepsUnicode(DatabaseType dbType, string sqlType, bool expectUnicode)
    {
        var guess = GetColumn(dbType, sqlType).GetGuesser().Guess;

        Assert.Multiple(() =>
        {
            Assert.That(guess.CSharpType, Is.EqualTo(typeof(string)));
            Assert.That(guess.Unicode, Is.EqualTo(expectUnicode));
        });
    }

    /// <summary>
    ///     The <see cref="TypeGuesser.Guesser" /> for a string column starts out as wide as the column (on Oracle, a
    ///     non-ASCII character counts as more than one character)
    /// </summary>
    [TestCase(DatabaseType.MicrosoftSQLServer, "nvarchar(10)")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "nchar(10)")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "varchar(10)")]
    [TestCase(DatabaseType.Oracle, "nvarchar2(10)")]
    [TestCase(DatabaseType.Oracle, "varchar2(10)")]
    [TestCase(DatabaseType.MySql, "varchar(10)")]
    [TestCase(DatabaseType.PostgreSql, "character varying(10)")]
    [TestCase(DatabaseType.Sqlite, "character varying(10)")]
    public void GetGuesser_StringColumn_KeepsWidth(DatabaseType dbType, string sqlType) =>
        Assert.That(GetColumn(dbType, sqlType).GetGuesser().Guess.Width, Is.EqualTo(10));

    /// <summary>
    ///     The <see cref="TypeGuesser.Guesser" /> for an unlimited string column starts out guessing the unlimited string type
    /// </summary>
    [TestCase(DatabaseType.MicrosoftSQLServer, "nvarchar(max)", "nvarchar(max)")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "ntext", "nvarchar(max)")]
    [TestCase(DatabaseType.MicrosoftSQLServer, "varchar(max)", "varchar(max)")]
    [TestCase(DatabaseType.Oracle, "nclob", "NCLOB")]
    [TestCase(DatabaseType.Oracle, "clob", "CLOB")]
    [TestCase(DatabaseType.MySql, "longtext", "longtext")]
    [TestCase(DatabaseType.PostgreSql, "text", "text")]
    public void GetGuesser_UnlimitedStringColumn_GuessesUnlimitedType(DatabaseType dbType, string sqlType,
        string expectedSqlType)
    {
        var column = GetColumn(dbType, sqlType);
        Assert.That(GetTypeTranslater(dbType).GetSQLDBTypeForCSharpType(column.GetGuesser().Guess),
            Is.EqualTo(expectedSqlType));
    }

    /// <summary>
    ///     Creates a column of the given <paramref name="sqlType" />, no database connection is required.
    /// </summary>
    private static DiscoveredColumn GetColumn(DatabaseType dbType, string sqlType)
    {
        var helper = ImplementationManager.GetImplementation(dbType).GetServerHelper();
        var table = new DiscoveredServer(helper.GetConnectionStringBuilder("localhost", "db", "frank", "kangaro"))
            .ExpectDatabase("db").ExpectTable("tbl");

        using var noProprietaryFields = new DataTable();
        using var reader = noProprietaryFields.CreateDataReader();

        var column = new DiscoveredColumn(table, "MyCol", true);
        column.DataType = new DiscoveredDataType(reader, sqlType, column);
        return column;
    }

    private static ITypeTranslater GetTypeTranslater(DatabaseType dbType) =>
        dbType switch
        {
            DatabaseType.MicrosoftSQLServer => MicrosoftSQLTypeTranslater.Instance,
            DatabaseType.MySql => MySqlTypeTranslater.Instance,
            DatabaseType.Oracle => OracleTypeTranslater.Instance,
            DatabaseType.PostgreSql => PostgreSqlTypeTranslater.Instance,
            DatabaseType.Sqlite => SqliteTypeTranslater.Instance,
            _ => throw new ArgumentOutOfRangeException(nameof(dbType))
        };
}
