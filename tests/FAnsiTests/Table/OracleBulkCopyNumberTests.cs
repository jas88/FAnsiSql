using System.Reflection;
using FAnsi.Implementations.Oracle;
using NUnit.Framework;
using Oracle.ManagedDataAccess.Types;

namespace FAnsiTests.Table;

/// <summary>
///     How Oracle bulk insert sends a float or double to a NUMBER(p,s) column. These need no Oracle server: an
///     OracleDecimal is made on the client.
/// </summary>
internal sealed class OracleBulkCopyNumberTests
{
    /// <summary>
    ///     What ODP.NET does with a double bound to a NUMBER column: it keeps 15 significant digits, so the double just
    ///     below 10^29 becomes 10^29, which is too large for NUMBER(29,0), and 0.12499999999999999 becomes 0.125
    /// </summary>
    [Test]
    public void OdpNet_ConvertsDoubleToFifteenDigits()
    {
        Assert.Multiple(static () =>
        {
            Assert.That(new OracleDecimal(Math.BitDecrement(1e29)),
                Is.EqualTo(new OracleDecimal("100000000000000000000000000000")));
            Assert.That(new OracleDecimal(0.12499999999999999), Is.EqualTo(new OracleDecimal("0.125")));
        });
    }

    /// <summary>
    ///     What ODP.NET does with a float bound to a NUMBER column: it makes the OracleDecimal of the float's shortest
    ///     text, then rounds that half up to 7 significant digits with its internal OracleDecimal.SetPrecisionNoRound,
    ///     which leaves out only the rounding to 7 decimal places that the public SetPrecision adds. So the float just
    ///     below 10^21, 9.9999995E+20, becomes 10^21, which is too large for NUMBER(21,0), and the float just below
    ///     0.125, 0.12499999, becomes 0.125
    /// </summary>
    [Test]
    public void OdpNet_ConvertsFloatToSevenDigits()
    {
        var method = typeof(OracleDecimal).GetMethod("SetPrecisionNoRound",
                         BindingFlags.NonPublic | BindingFlags.Static, [typeof(OracleDecimal), typeof(int)]) ??
                     throw new MissingMethodException(nameof(OracleDecimal), "SetPrecisionNoRound");
        var setPrecisionNoRound = method.CreateDelegate<Func<OracleDecimal, int, OracleDecimal>>();

        OracleDecimal ToOdpNetNumber(float value)
        {
            return setPrecisionNoRound(new OracleDecimal(value), 7);
        }

        Assert.Multiple(() =>
        {
            Assert.That(new OracleDecimal(MathF.BitDecrement(0.125f)), Is.EqualTo(new OracleDecimal("0.12499999")));
            Assert.That(ToOdpNetNumber(MathF.BitDecrement(1e21f)),
                Is.EqualTo(new OracleDecimal("1000000000000000000000")));
            Assert.That(ToOdpNetNumber(MathF.BitDecrement(0.125f)), Is.EqualTo(new OracleDecimal("0.125")));
            Assert.That(ToOdpNetNumber(0.100000024f), Is.EqualTo(new OracleDecimal("0.1")));
            // A tie, rounded up rather than to even
            Assert.That(ToOdpNetNumber(1.0000005f), Is.EqualTo(new OracleDecimal("1.000001")));
        });
    }

    [TestCase(9.999999999999997E+28, "99999999999999970000000000000")] // the double just below 10^29
    [TestCase(1e30, "1000000000000000000000000000000")]
    [TestCase(-1e30, "-1000000000000000000000000000000")]
    [TestCase(0.30000000000000004, "0.30000000000000004")]
    [TestCase(0.12499999999999999, "0.12499999999999999")]
    [TestCase(9.99, "9.99")]
    [TestCase(0.0001, "0.0001")]
    [TestCase(1.5E-07, "0.00000015")]
    [TestCase(123456789012345.67, "123456789012345.67")]
    public void ToNumber_Double_IsTheNumberOfItsShortestText(double value, string expected) =>
        Assert.That(OracleBulkCopy.ToNumber(value), Is.EqualTo(new OracleDecimal(expected)));

    [TestCase(9.999999E+28f, "99999990000000000000000000000")] // the float just below 10^29
    [TestCase(9.9999995E+20f, "999999950000000000000")] // the float just below 10^21
    [TestCase(0.12499999f, "0.12499999")] // the float just below 0.125
    [TestCase(0.100000024f, "0.100000024")] // 9 significant digits
    [TestCase(1e30f, "1000000000000000000000000000000")]
    [TestCase(9.99f, "9.99")]
    public void ToNumber_Float_IsTheNumberOfItsShortestText(float value, string expected) =>
        Assert.That(OracleBulkCopy.ToNumber(value), Is.EqualTo(new OracleDecimal(expected)));

    /// <summary>
    ///     These are left for ODP.NET to convert, as before
    /// </summary>
    [TestCase(double.NaN)]
    [TestCase(double.PositiveInfinity)]
    [TestCase(double.NegativeInfinity)]
    [TestCase(0.0)]
    [TestCase(double.Epsilon)] // smaller than any NUMBER
    [TestCase(1e200)] // larger than any NUMBER
    public void ToNumber_NotFiniteOrOutsideTheRangeOfNumber_IsLeftAsItIs(double value) =>
        Assert.That(OracleBulkCopy.ToNumber(value), Is.EqualTo(value));
}
