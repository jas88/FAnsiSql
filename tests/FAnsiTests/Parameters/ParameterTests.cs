using System.Data;
using System.Globalization;
using System.Text;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Implementation;
using FAnsiTests.TestGeneration;
using Npgsql;
using NpgsqlTypes;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Parameters;

internal sealed class ParameterTests : DatabaseTests
{
    /// <summary>
    ///     Oracle, PostgreSQL and SQLite SQL has no variables to declare and set, so <c>GetParameterDeclaration</c> throws
    ///     for them (see <see cref="Test_SupportsEmbeddedParameters_DeclarationOrThrow" />)
    /// </summary>
    private const string EmbeddedParametersOnly =
        "Only SQL Server and MySQL SQL can declare and set parameters (SupportsEmbeddedParameters)";

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_SupportsEmbeddedParameters_DeclarationOrThrow(DatabaseType type)
    {
        var syntax = ImplementationManager.GetImplementation(type).GetQuerySyntaxHelper();

        if (syntax.SupportsEmbeddedParameters())
            Assert.That(syntax.GetParameterDeclaration("@bob", new DatabaseTypeRequest(typeof(string), 10)),
                Is.Not.Empty);
        else
            Assert.Throws<NotSupportedException>(() =>
                syntax.GetParameterDeclaration("@bob", new DatabaseTypeRequest(typeof(string), 10)));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    [OnlyDatabase(DatabaseType.MicrosoftSQLServer, DatabaseType.MySql, EmbeddedParametersOnly)]
    public void CreateParameter(DatabaseType type)
    {
        var syntax = ImplementationManager.GetImplementation(type).GetQuerySyntaxHelper();

        var declaration = syntax.GetParameterDeclaration("@bob", new DatabaseTypeRequest(typeof(string), 10));

        Assert.That(declaration, Does.Contain("@bob"));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    [OnlyDatabase(DatabaseType.MicrosoftSQLServer, DatabaseType.MySql, EmbeddedParametersOnly)]
    public void CreateParameter_AndUse(DatabaseType type)
    {
        var db = GetTestDatabase(type);

        var dt = new DataTable();

        dt.Columns.Add("FF");
        dt.Rows.Add("armag");
        dt.Rows.Add("geddon");

        var tbl = db.CreateTable("ParameterUseTest", dt);

        var sb = new StringBuilder();

        //declare the variable
        sb.AppendLine(tbl.GetQuerySyntaxHelper()
            .GetParameterDeclaration("@bob", new DatabaseTypeRequest(typeof(string), 10)));

        sb.AppendLine("SET @bob='armag';");
        //set the variable

        sb.Append(CultureInfo.InvariantCulture, $"SELECT FF from {tbl.GetFullyQualifiedName()} WHERE FF = @bob;");
        sb.AppendLine();

        using var con = db.Server.GetConnection();
        con.Open();
        using var r = db.Server.GetCommand(sb.ToString(), con).ExecuteReader();

        Assert.That(r.Read());
        Assert.That(r.Read(), Is.False);
    }

    /// <summary>
    ///     Npgsql treats <see cref="DbType.DateTime" /> as 'timestamp with time zone' (UTC values only) and
    ///     <see cref="DbType.DateTime2" /> as 'timestamp without time zone', which stores the value exactly as given.
    /// </summary>
    [TestCase(DateTimeKind.Unspecified)]
    [TestCase(DateTimeKind.Local)]
    [TestCase(DateTimeKind.Utc)]
    public void GetParameter_PostgreSqlTimestamp_KeepsWallClockTime(DateTimeKind kind)
    {
        var value = new DateTime(2001, 1, 2, 3, 4, 5, kind);

        var p = GetPostgreSqlParameter("timestamp without time zone", value);

        Assert.Multiple(() =>
        {
            Assert.That(p.DbType, Is.EqualTo(DbType.DateTime2));
            Assert.That(p.NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Timestamp));
            Assert.That(p.Value, Is.EqualTo(new DateTime(2001, 1, 2, 3, 4, 5)));
            //Npgsql refuses to write a UTC DateTime to 'timestamp without time zone'
            Assert.That(p.Value,
                Is.TypeOf<DateTime>().With.Property(nameof(DateTime.Kind)).Not.EqualTo(DateTimeKind.Utc));
        });
    }

    [Test]
    public void GetParameter_PostgreSqlTimestamp_StringValueIsNotConvertedToUtc()
    {
        var p = GetPostgreSqlParameter("timestamp without time zone", "2001-01-02 03:04:05");

        Assert.Multiple(() =>
        {
            Assert.That(p.NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Timestamp));
            Assert.That(p.Value, Is.EqualTo(new DateTime(2001, 1, 2, 3, 4, 5)));
            Assert.That(p.Value,
                Is.TypeOf<DateTime>().With.Property(nameof(DateTime.Kind)).Not.EqualTo(DateTimeKind.Utc));
        });
    }

    /// <summary>
    ///     A 'timestamp with time zone' stores an instant.  A Local value is converted to UTC, an Unspecified one is taken
    ///     to be UTC already, which is how values read from such a column come back out of a DataTable.
    /// </summary>
    [TestCase(DateTimeKind.Unspecified)]
    [TestCase(DateTimeKind.Local)]
    [TestCase(DateTimeKind.Utc)]
    public void GetParameter_PostgreSqlTimestampWithTimeZone_IsUtc(DateTimeKind kind)
    {
        var value = new DateTime(2001, 1, 2, 3, 4, 5, kind);
        var expected = kind == DateTimeKind.Local ? value.ToUniversalTime() : new DateTime(2001, 1, 2, 3, 4, 5);

        var p = GetPostgreSqlParameter("timestamp with time zone", value);

        Assert.Multiple(() =>
        {
            Assert.That(p.NpgsqlDbType, Is.EqualTo(NpgsqlDbType.TimestampTz));
            Assert.That(p.Value, Is.EqualTo(expected));
            //Npgsql refuses to write anything except a UTC DateTime to 'timestamp with time zone'
            Assert.That(p.Value, Is.TypeOf<DateTime>().With.Property(nameof(DateTime.Kind)).EqualTo(DateTimeKind.Utc));
        });
    }

    [Test]
    public void GetParameter_PostgreSqlTimestampWithTimeZone_StringValueIsTakenAsUtc()
    {
        var p = GetPostgreSqlParameter("timestamp with time zone", "2001-01-02 03:04:05");

        Assert.Multiple(() =>
        {
            Assert.That(p.Value, Is.EqualTo(new DateTime(2001, 1, 2, 3, 4, 5)));
            Assert.That(p.Value, Is.TypeOf<DateTime>().With.Property(nameof(DateTime.Kind)).EqualTo(DateTimeKind.Utc));
        });
    }

    /// <summary>
    ///     A 'date' has no time of day and no time zone, it stores the date of the value exactly as given whatever the
    ///     <see cref="DateTimeKind" />, as the other DBMS do
    /// </summary>
    [TestCase(DateTimeKind.Unspecified)]
    [TestCase(DateTimeKind.Local)]
    [TestCase(DateTimeKind.Utc)]
    public void GetParameter_PostgreSqlDate_IsTheDateAsGiven(DateTimeKind kind)
    {
        var p = GetPostgreSqlParameter("date", new DateTime(2001, 1, 2, 23, 4, 5, kind));

        Assert.Multiple(() =>
        {
            Assert.That(p.DbType, Is.EqualTo(DbType.Date));
            Assert.That(p.NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Date));
            Assert.That(p.Value, Is.EqualTo(new DateOnly(2001, 1, 2)));
        });
    }

    [TestCase("2001-01-02")]
    [TestCase("2001-01-02 23:04:05")]
    public void GetParameter_PostgreSqlDate_StringValue(string value)
    {
        var p = GetPostgreSqlParameter("date", value);

        Assert.Multiple(() =>
        {
            Assert.That(p.NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Date));
            Assert.That(p.Value, Is.EqualTo(new DateOnly(2001, 1, 2)));
        });
    }

    [Test]
    public void GetParameter_PostgreSqlDate_DateOnlyValue()
    {
        var p = GetPostgreSqlParameter("date", new DateOnly(2001, 1, 2));

        Assert.Multiple(() =>
        {
            Assert.That(p.NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Date));
            Assert.That(p.Value, Is.EqualTo(new DateOnly(2001, 1, 2)));
        });
    }

    /// <summary>
    ///     Npgsql writes both a <see cref="TimeSpan" /> and a <see cref="TimeOnly" /> to a 'time'.  A 'time with time zone'
    ///     is sent a 'time' too, which PostgreSQL gives the offset of the time zone of the session, as it does a time
    ///     written in the SQL.
    /// </summary>
    [TestCase("time without time zone")]
    [TestCase("time with time zone")]
    public void GetParameter_PostgreSqlTime(string sqlType)
    {
        var fromTimeSpan = GetPostgreSqlParameter(sqlType, new TimeSpan(13, 11, 10));
        var fromTimeOnly = GetPostgreSqlParameter(sqlType, new TimeOnly(13, 11, 10));
        var fromString = GetPostgreSqlParameter(sqlType, "13:11:10");

        Assert.Multiple(() =>
        {
            Assert.That(fromTimeSpan.NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Time));
            Assert.That(fromTimeSpan.Value, Is.EqualTo(new TimeSpan(13, 11, 10)));
            Assert.That(fromTimeOnly.NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Time));
            Assert.That(fromTimeOnly.Value, Is.EqualTo(new TimeOnly(13, 11, 10)));
            Assert.That(fromString.NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Time));
            Assert.That(fromString.Value, Is.EqualTo(new TimeSpan(13, 11, 10)));
        });
    }

    /// <summary>
    ///     An 'interval' is not limited to a day, so it must not be sent as a 'time'.  <see cref="DbType.Object" /> lets
    ///     Npgsql send a <see cref="TimeSpan" /> as the 'interval' it maps one to.
    /// </summary>
    [Test]
    public void GetParameter_PostgreSqlInterval()
    {
        var p = GetPostgreSqlParameter("interval", new TimeSpan(1, 2, 3, 4));

        Assert.Multiple(() =>
        {
            Assert.That(p.DbType, Is.EqualTo(DbType.Object));
            Assert.That(p.Value, Is.EqualTo(new TimeSpan(1, 2, 3, 4)));
        });
    }

    /// <summary>
    ///     Builds the parameter FAnsi would use to insert <paramref name="value" /> into a column of the given
    ///     <paramref name="sqlType" />, no database connection is required.
    /// </summary>
    private static NpgsqlParameter GetPostgreSqlParameter(string sqlType, object value)
    {
        var table = new DiscoveredServer("Host=localhost;Database=FAnsiTests", DatabaseType.PostgreSql)
            .ExpectDatabase("FAnsiTests").ExpectTable("ParameterTable");

        using var noProprietaryFields = new DataTable();
        using var reader = noProprietaryFields.CreateDataReader();

        var column = new DiscoveredColumn(table, "MyCol", true);
        column.DataType = new DiscoveredDataType(reader, sqlType, column);

        var syntax = table.GetQuerySyntaxHelper();
        return (NpgsqlParameter)syntax.GetParameter(table.Database.Server.Helper.GetParameter("@MyCol"), column, value,
            CultureInfo.InvariantCulture);
    }
}
