using FAnsi.Implementations.MySql;
using NUnit.Framework;

namespace FAnsiTests.Table;

/// <summary>
///     Which integer types reported by MySQL and MariaDB lose their display width when columns are discovered.  The
///     servers report types in lower case, so the other cases are only covered here.
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
}
