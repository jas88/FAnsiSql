using System.Globalization;
using FAnsi.Discovery;
using NUnit.Framework;

namespace FAnsiTests.Table;

/// <summary>
///     Tests the range of a decimal(p,s) column that bulk insert checks values against
/// </summary>
internal sealed class DecimalColumnRangeTests
{
    [TestCase("decimal(5,2)", 5, 2)]
    [TestCase("numeric(3,5)", 3, 5)]
    [TestCase("NUMBER(3,5)", 3, 5)]
    [TestCase("decimal(29,1)", 29, 1)]
    [TestCase("decimal(10, 2) unsigned", 10, 2)]
    public void TryGetPrecisionAndScale_Declared(string sqlType, int expectedPrecision, int expectedScale)
    {
        var found = DecimalColumnRange.TryGetPrecisionAndScale(sqlType, out var precision, out var scale);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.True);
            Assert.That(precision, Is.EqualTo(expectedPrecision));
            Assert.That(scale, Is.EqualTo(expectedScale));
        });
    }

    [TestCase("numeric(5,-2)")] // a negative scale
    [TestCase("numeric(,)")] // how PostgreSQL's discovery describes numeric without a precision
    [TestCase("numeric")]
    [TestCase("varchar(10)")]
    [TestCase(null)]
    public void TryGetPrecisionAndScale_NotDeclared(string? sqlType)
    {
        var found = DecimalColumnRange.TryGetPrecisionAndScale(sqlType, out var precision, out var scale);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.False);
            Assert.That(precision, Is.Zero);
            Assert.That(scale, Is.Zero);
        });
    }

    [TestCase(3, 1, "100")]
    [TestCase(5, 0, "100000")]
    [TestCase(3, 3, "1")]
    [TestCase(3, 5, "0.01")]
    [TestCase(28, 0, "10000000000000000000000000000")]
    [TestCase(29, 1, "10000000000000000000000000000")]
    [TestCase(38, 10, "10000000000000000000000000000")]
    [TestCase(1, 29, "0.0000000000000000000000000001")]
    [TestCase(1, 30, "0.0000000000000000000000000001")] // 10^-29, for which no decimal but 0 is small enough
    public void GetLimit_IsTenToTheDigitsBeforeThePoint(int precision, int scale, string expected) =>
        Assert.That(DecimalColumnRange.GetLimit(precision, scale),
            Is.EqualTo(decimal.Parse(expected, CultureInfo.InvariantCulture)));

    [TestCase(29, 0)]
    [TestCase(38, 2)]
    [TestCase(65, 30)]
    [TestCase(0, 0)] // the precision is not known
    public void GetLimit_EveryDecimalFits(int precision, int scale) =>
        Assert.That(DecimalColumnRange.GetLimit(precision, scale), Is.Null);

    [TestCase(3, 1, "99.9")]
    [TestCase(5, 0, "99999")]
    [TestCase(3, 3, "0.999")]
    [TestCase(3, 5, "0.00999")]
    [TestCase(29, 1, "9999999999999999999999999999.9")]
    [TestCase(38, 0, "99999999999999999999999999999999999999")]
    public void FormatMaxValue_IsAllNines(int precision, int scale, string expected) =>
        Assert.That(DecimalColumnRange.FormatMaxValue(precision, scale), Is.EqualTo(expected));

    /// <summary>
    ///     Where a decimal holds the largest value of the column, adding one in its last place makes the limit
    /// </summary>
    [Test]
    public void FormatMaxValue_IsOneBelowTheLimitInTheLastPlace()
    {
        for (var precision = 1; precision <= 28; precision++)
        {
            for (var scale = 0; scale <= 28; scale++)
            {
                var max = decimal.Parse(DecimalColumnRange.FormatMaxValue(precision, scale),
                    CultureInfo.InvariantCulture);
                var lastPlace = new decimal(1, 0, 0, false, (byte)scale);

                Assert.That(max + lastPlace, Is.EqualTo(DecimalColumnRange.GetLimit(precision, scale)),
                    $"decimal({precision},{scale})");
            }
        }
    }
}
