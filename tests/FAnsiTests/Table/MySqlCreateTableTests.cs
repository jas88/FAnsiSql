using System.Data;
using System.Globalization;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

/// <summary>
///     Table creation tests that only concern <see cref="DatabaseType.MySql" />, which covers both MySQL and MariaDB
/// </summary>
internal sealed class MySqlCreateTableTests : DatabaseTests
{
    private const DatabaseType DbType = DatabaseType.MySql;

    private static readonly string[] BitTypes = ["bit", "bit(8)", "bit(64)"];

    /// <summary>
    ///     Only character types take a CHARACTER SET.  MySQL and MariaDB both reject one on any other type as a syntax
    ///     error, so the Unicode flag of a request for another type must be ignored.
    /// </summary>
    [Test]
    public void CreateTable_UnicodeRequestForTypeWithoutCharacterSet_IsIgnored()
    {
        var db = GetTestDatabase(DbType);

        var tbl = db.CreateTable("UnicodeRequests",
        [
            new DatabaseColumnRequest("MyDate", new DatabaseTypeRequest(typeof(DateTime)) { Unicode = true }),
            new DatabaseColumnRequest("MyTime", new DatabaseTypeRequest(typeof(TimeSpan)) { Unicode = true }),
            new DatabaseColumnRequest("MyInt", new DatabaseTypeRequest(typeof(int)) { Unicode = true }),
            new DatabaseColumnRequest("MyDecimal",
                new DatabaseTypeRequest(typeof(decimal), null, new DecimalSize(3, 2)) { Unicode = true }),
            new DatabaseColumnRequest("MyBool", new DatabaseTypeRequest(typeof(bool)) { Unicode = true }),
            new DatabaseColumnRequest("MyBytes", new DatabaseTypeRequest(typeof(byte[])) { Unicode = true }),
            new DatabaseColumnRequest("MyString", new DatabaseTypeRequest(typeof(string), 10) { Unicode = true })
        ]);

        Assert.Multiple(() =>
        {
            Assert.That(tbl.DiscoverColumn("MyDate").DataType?.SQLType, Is.EqualTo("datetime"));
            Assert.That(tbl.DiscoverColumn("MyTime").DataType?.SQLType, Is.EqualTo("time"));
            Assert.That(tbl.DiscoverColumn("MyInt").DataType?.SQLType, Is.EqualTo("int"));
            Assert.That(tbl.DiscoverColumn("MyDecimal").DataType?.SQLType, Is.EqualTo("decimal(5,2)"));
            Assert.That(tbl.DiscoverColumn("MyBool").DataType?.SQLType, Is.EqualTo("tinyint(1)"));
            Assert.That(tbl.DiscoverColumn("MyBytes").DataType?.SQLType, Is.EqualTo("longblob"));

            // The string column still gets the utf8mb4 character set
            Assert.That(tbl.DiscoverColumn("MyString").DataType?.SQLType, Is.EqualTo("varchar(10)"));
            Assert.That(tbl.DiscoverColumn("MyString").Collation, Is.EqualTo("utf8mb4_bin"));
        });
    }

    /// <summary>
    ///     TypeGuesser flags a column Unicode when the text of any of its values has a non-ASCII character, even when it
    ///     guesses a date.  In en-US .NET puts U+202F before AM/PM when ICU is version 72 or later (as on macOS and
    ///     Windows 11), so a table of dates created there got <c>datetime CHARACTER SET utf8mb4</c> and failed with a
    ///     syntax error.  CI's Linux runners use the invariant culture and ICU 70, where en-US dates are ASCII, so the
    ///     ko-KR case, whose AM/PM designators are Korean whatever the ICU version, is the one that fails there.
    /// </summary>
    [Test]
    [SetCulture("en-US")]
    public void CreateTable_DatesInCultureWithNonAsciiDates_EnUs() => CreateTableOfDates();

    /// <inheritdoc cref="CreateTable_DatesInCultureWithNonAsciiDates_EnUs" />
    [Test]
    [SetCulture("ko-KR")]
    public void CreateTable_DatesInCultureWithNonAsciiDates_KoKr() => CreateTableOfDates();

    private void CreateTableOfDates()
    {
        var db = GetTestDatabase(DbType);
        var when = new DateTime(2024, 1, 1, 13, 30, 0);

        using var dt = new DataTable();
        dt.Columns.Add("Typed", typeof(DateTime));
        dt.Columns.Add("Formatted");
        dt.Rows.Add(when, when.ToString(CultureInfo.CurrentCulture));

        var tbl = db.CreateTable("CultureDates", dt);

        using var result = tbl.GetDataTable();

        Assert.Multiple(() =>
        {
            Assert.That(tbl.DiscoverColumn("Typed").DataType?.SQLType, Is.EqualTo("datetime"));
            Assert.That(tbl.DiscoverColumn("Formatted").DataType?.SQLType, Is.EqualTo("datetime"));
            Assert.That(result.Rows[0]["Typed"], Is.EqualTo(when));
            Assert.That(result.Rows[0]["Formatted"], Is.EqualTo(when));
        });
    }

    /// <summary>
    ///     MariaDB reports a display width on every integer column, e.g. bigint(20), where MySQL 8.0.19 and later report
    ///     none except on tinyint(1) and ZEROFILL columns.  Columns must be discovered with the same type on both.
    /// </summary>
    [Test]
    public void DiscoverColumns_IntegerTypes_HaveNoDisplayWidth()
    {
        var db = GetTestDatabase(DbType);

        var tbl = db.CreateTable("IntegerTypes",
        [
            new DatabaseColumnRequest("MyByte", new DatabaseTypeRequest(typeof(byte))),
            new DatabaseColumnRequest("MyShort", new DatabaseTypeRequest(typeof(short))),
            new DatabaseColumnRequest("MyInt", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("MyLong", new DatabaseTypeRequest(typeof(long))),
            new DatabaseColumnRequest("MyBool", new DatabaseTypeRequest(typeof(bool))),
            new DatabaseColumnRequest("MyMediumInt", "mediumint"),
            new DatabaseColumnRequest("MyUnsignedLong", "bigint unsigned")
        ]);

        Assert.Multiple(() =>
        {
            Assert.That(tbl.DiscoverColumn("MyByte").DataType?.SQLType, Is.EqualTo("tinyint"));
            Assert.That(tbl.DiscoverColumn("MyShort").DataType?.SQLType, Is.EqualTo("smallint"));
            Assert.That(tbl.DiscoverColumn("MyInt").DataType?.SQLType, Is.EqualTo("int"));
            Assert.That(tbl.DiscoverColumn("MyLong").DataType?.SQLType, Is.EqualTo("bigint"));
            Assert.That(tbl.DiscoverColumn("MyBool").DataType?.SQLType, Is.EqualTo("tinyint(1)"));
            Assert.That(tbl.DiscoverColumn("MyMediumInt").DataType?.SQLType, Is.EqualTo("mediumint"));
            Assert.That(tbl.DiscoverColumn("MyUnsignedLong").DataType?.SQLType, Is.EqualTo("bigint unsigned"));
        });
    }

    /// <summary>
    ///     The display width of a ZEROFILL column is the number of digits its values are padded to with zeros, and MySQL
    ///     and MariaDB both report it, so it is kept on every integer type.  The type must still be understood.
    /// </summary>
    [Test]
    public void DiscoverColumns_ZerofillIntegerTypes_KeepDisplayWidth()
    {
        var db = GetTestDatabase(DbType);

        var tbl = db.CreateTable("ZerofillTypes",
        [
            new DatabaseColumnRequest("MyTinyInt", "tinyint(2) zerofill"),
            new DatabaseColumnRequest("MySmallInt", "smallint(4) zerofill"),
            new DatabaseColumnRequest("MyMediumInt", "mediumint(6) zerofill"),
            new DatabaseColumnRequest("MyInt", "int(8) zerofill"),
            new DatabaseColumnRequest("MyBigInt", "bigint(12) zerofill")
        ]);

        Assert.Multiple(() =>
        {
            AssertDiscoveredType(tbl, "MyTinyInt", "tinyint(2) unsigned zerofill", typeof(byte));
            AssertDiscoveredType(tbl, "MySmallInt", "smallint(4) unsigned zerofill", typeof(short));
            AssertDiscoveredType(tbl, "MyMediumInt", "mediumint(6) unsigned zerofill", typeof(int));
            AssertDiscoveredType(tbl, "MyInt", "int(8) unsigned zerofill", typeof(int));
            AssertDiscoveredType(tbl, "MyBigInt", "bigint(12) unsigned zerofill", typeof(long));
        });
    }

    private static void AssertDiscoveredType(DiscoveredTable tbl, string column, string sqlType, Type cSharpType)
    {
        var dataType = tbl.DiscoverColumn(column).DataType;
        Assert.That(dataType?.SQLType, Is.EqualTo(sqlType), column);
        Assert.That(dataType?.GetCSharpDataType(), Is.EqualTo(cSharpType), column);
    }

    /// <summary>
    ///     The n of bit(n) is the number of bits the column holds, so a column created from the discovered type must
    ///     hold as many.  bit(1) is the same type as bit, and both servers report a bit column as bit(1).
    /// </summary>
    [Test]
    public void DiscoverColumns_BitTypes_KeepSize()
    {
        var db = GetTestDatabase(DbType);

        var tbl = db.CreateTable("BitTypes",
        [
            new DatabaseColumnRequest("MyBit", "bit"),
            new DatabaseColumnRequest("MyBit8", "bit(8)"),
            new DatabaseColumnRequest("MyBit64", "bit(64)")
        ]);

        var copy = db.CreateTable("BitTypesCopy",
            tbl.DiscoverColumns().Select(static c => new DatabaseColumnRequest(c.GetRuntimeName(), SqlTypeOf(c)))
                .ToArray());

        Assert.Multiple(() =>
        {
            Assert.That(tbl.DiscoverColumns().Select(SqlTypeOf), Is.EqualTo(BitTypes).AsCollection);
            Assert.That(copy.DiscoverColumns().Select(SqlTypeOf), Is.EqualTo(BitTypes).AsCollection);
        });
    }

    /// <summary>
    ///     MariaDB reports a year column as year(4), and MySQL 8.0.19 and later as year.  It must be discovered as year
    ///     on both.
    /// </summary>
    [Test]
    public void DiscoverColumns_Year_HasNoDisplayWidth()
    {
        var db = GetTestDatabase(DbType);

        var tbl = db.CreateTable("YearTypes", [new DatabaseColumnRequest("MyYear", "year")]);

        Assert.That(tbl.DiscoverColumn("MyYear").DataType?.SQLType, Is.EqualTo("year"));
    }

    private static string SqlTypeOf(DiscoveredColumn column) =>
        column.DataType?.SQLType ?? throw new InvalidOperationException($"No data type was discovered for {column}");
}
