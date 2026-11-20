using System.Globalization;
using System.Numerics;
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

    /// <summary>
    ///     A double is compared with 10^(p-s) for any number of digits before the decimal point. The value tested is
    ///     <paramref name="near" /> moved by <paramref name="ulps" /> doubles, so that the doubles just below and at
    ///     the limit are tested.
    /// </summary>
    [TestCase(1e30, 0, 29, 0, true)]
    [TestCase(-1e30, 0, 29, 0, true)]
    [TestCase(1e30, 0, 38, 0, false)]
    [TestCase(1e29, 0, 29, 0, true)]
    [TestCase(1e29, 1, 29, 0, true)]
    [TestCase(1e29, -1, 29, 0, false)]
    [TestCase(1e23, 0, 23, 0, true)]
    [TestCase(1e23, -1, 23, 0, false)]
    [TestCase(1e22, 0, 22, 0, true)] // 10^22 is a double
    [TestCase(1e22, -1, 22, 0, false)]
    [TestCase(1000.0, 0, 5, 2, true)]
    [TestCase(1000.0, -1, 5, 2, false)]
    [TestCase(0.01, 0, 3, 5, true)]
    [TestCase(0.01, -1, 3, 5, false)]
    [TestCase(double.MaxValue, 0, 400, 0, false)] // no double reaches 10^400
    [TestCase(double.Epsilon, 0, 1, 400, true)] // every double but 0 reaches 10^-399
    [TestCase(0.0, 0, 1, 400, false)]
    [TestCase(-0.0, 0, 1, 400, false)]
    [TestCase(0.0, 0, 5, 2, false)]
    [TestCase(-0.0, 0, 5, 2, false)]
    [TestCase(0.0, 0, 1, 1000, false)] // numeric(1,1000) of PostgreSQL 15 or later: Math.Pow(10, -999) is 0
    [TestCase(-0.0, 0, 1, 1000, false)]
    [TestCase(double.Epsilon, 0, 1, 1000, true)]
    [TestCase(1e-323, 0, 1, 324, true)] // 10^-323, the smallest power of ten that Math.Pow does not make 0
    [TestCase(1e-323, -1, 1, 324, false)] // double.Epsilon
    public void IsTooLarge_Double(double near, int ulps, int precision, int scale, bool expected)
    {
        var value = near;
        for (var i = 0; i < Math.Abs(ulps); i++)
            value = ulps < 0 ? Math.BitDecrement(value) : Math.BitIncrement(value);

        Assert.That(DecimalColumnRange.IsTooLarge(value, precision, scale), Is.EqualTo(expected),
            value.ToString("R", CultureInfo.InvariantCulture));
    }

    [TestCase(1e30f, 0, 29, 0, true)]
    [TestCase(1e30f, 0, 38, 0, false)]
    [TestCase(1e29f, 0, 29, 0, true)]
    [TestCase(1e29f, -1, 29, 0, false)]
    [TestCase(1000f, 0, 5, 2, true)]
    [TestCase(1000f, -1, 5, 2, false)]
    [TestCase(0f, 0, 5, 2, false)]
    [TestCase(-0f, 0, 5, 2, false)]
    [TestCase(0f, 0, 1, 1000, false)]
    [TestCase(-0f, 0, 1, 1000, false)]
    [TestCase(float.Epsilon, 0, 1, 1000, true)]
    public void IsTooLarge_Float(float near, int ulps, int precision, int scale, bool expected)
    {
        var value = near;
        for (var i = 0; i < Math.Abs(ulps); i++)
            value = ulps < 0 ? MathF.BitDecrement(value) : MathF.BitIncrement(value);

        Assert.That(DecimalColumnRange.IsTooLarge(value, precision, scale), Is.EqualTo(expected),
            value.ToString("R", CultureInfo.InvariantCulture));
    }

    /// <summary>
    ///     The double nearest 10^29 is just below it, but its shortest text, 1E+29, is what MySQL is sent and FAnsi's
    ///     PostgreSQL writer sends, so it is too large for decimal(29,0)
    /// </summary>
    [Test]
    public void IsTooLarge_DoubleJustBelowThePowerOfTenItIsWrittenAs_IsTooLarge()
    {
        Assert.Multiple(static () =>
        {
            Assert.That(new BigInteger(1e29), Is.LessThan(BigInteger.Pow(10, 29)));
            Assert.That(1e29.ToString("R", CultureInfo.InvariantCulture), Is.EqualTo("1E+29"));
            Assert.That(DecimalColumnRange.IsTooLarge(1e29, 29, 0), Is.True);
        });
    }
}
