using FAnsi.Implementations.MySql;
using NUnit.Framework;

namespace FAnsiTests.Table;

/// <summary>
///     Which types reported by MySQL and MariaDB lose the number in brackets when columns are discovered: integer
///     types lose their display width, but bit keeps its size.  The servers report types in lower case, so the other
///     cases are only covered here.
/// </summary>
internal sealed class MySqlIntegerDisplayWidthTests
{
    [TestCase("tinyint(4)", "tinyint")]
    [TestCase("smallint(6)", "smallint")]
    [TestCase("mediumint(9)", "mediumint")]
    [TestCase("int(11)", "int")]
    [TestCase("bigint(20)", "bigint")]
    [TestCase("bigint(20) unsigned", "bigint unsigned")]
    [TestCase("BIGINT(20) UNSIGNED", "BIGINT UNSIGNED")]
    public void TrimIntDisplayValues_IntegerType_RemovesWidth(string type, string expected) =>
        Assert.That(MySqlTableHelper.TrimIntDisplayValues(type), Is.EqualTo(expected));

    /// <summary>
    ///     tinyint(1) is how both servers report a boolean column, and the width of a ZEROFILL column is the number of
    ///     digits its values are padded to with zeros
    /// </summary>
    [TestCase("tinyint(1)")]
    [TestCase("tinyint(3) unsigned zerofill")]
    [TestCase("smallint(5) unsigned zerofill")]
    [TestCase("mediumint(8) unsigned zerofill")]
    [TestCase("int(10) unsigned zerofill")]
    [TestCase("bigint(20) unsigned zerofill")]
    [TestCase("INT(10) UNSIGNED ZEROFILL")]
    [TestCase("BigInt(20) Unsigned ZeroFill")]
    [TestCase("decimal(10,2) unsigned zerofill")]
    public void TrimIntDisplayValues_BooleanOrZerofill_KeepsWidth(string type) =>
        Assert.That(MySqlTableHelper.TrimIntDisplayValues(type), Is.EqualTo(type));

    /// <summary>
    ///     The n of bit(n) is the number of bits the column holds, so it is kept, except in bit(1), which is the same
    ///     type as bit
    /// </summary>
    [TestCase("bit(1)", "bit")]
    [TestCase("BIT(1)", "bit")]
    [TestCase("bit(2)", "bit(2)")]
    [TestCase("bit(8)", "bit(8)")]
    [TestCase("bit(10)", "bit(10)")]
    [TestCase("bit(64)", "bit(64)")]
    [TestCase("BIT(8)", "BIT(8)")]
    public void TrimIntDisplayValues_Bit_KeepsSizeUnlessOne(string type, string expected) =>
        Assert.That(MySqlTableHelper.TrimIntDisplayValues(type), Is.EqualTo(expected));

    /// <summary>
    ///     In the other types with a number in brackets it is a length, a precision and scale, or a number of digits of
    ///     fractional seconds, and none of them is touched
    /// </summary>
    [TestCase("char(5)")]
    [TestCase("varchar(10)")]
    [TestCase("binary(3)")]
    [TestCase("varbinary(5)")]
    [TestCase("decimal(10,2)")]
    [TestCase("float(7,4)")]
    [TestCase("double(10,2)")]
    [TestCase("datetime(3)")]
    [TestCase("time(2)")]
    [TestCase("timestamp(6)")]
    public void TrimIntDisplayValues_OtherTypes_Unchanged(string type) =>
        Assert.That(MySqlTableHelper.TrimIntDisplayValues(type), Is.EqualTo(type));
}
