using System.Globalization;
using System.Text.RegularExpressions;

namespace FAnsi.Discovery;

/// <summary>
///     The range of the values of a decimal(p,s) column, worked out from its precision p and scale s. The scale can be
///     larger than the precision, as in numeric(3,5) on PostgreSQL 15 or later and NUMBER(3,5) on Oracle, which hold
///     up to 0.00999.
/// </summary>
internal static partial class DecimalColumnRange
{
    /// <summary>
    ///     The most digits that a decimal holds after the decimal point, and the most 9s that it holds before it
    /// </summary>
    private const int MaxDecimalDigits = 28;

    /// <summary>
    ///     Gets the precision and scale that <paramref name="sqlType" /> declares, such as 3 and 5 for numeric(3,5)
    /// </summary>
    /// <returns>False if <paramref name="sqlType" /> declares no precision and scale, or a negative scale</returns>
    public static bool TryGetPrecisionAndScale(string? sqlType, out int precision, out int scale)
    {
        if (sqlType != null && PrecisionAndScaleRegex().Match(sqlType) is { Success: true } match &&
            int.TryParse(match.Groups[1].ValueSpan, NumberStyles.None, CultureInfo.InvariantCulture, out precision) &&
            int.TryParse(match.Groups[2].ValueSpan, NumberStyles.None, CultureInfo.InvariantCulture, out scale))
            return true;

        precision = scale = 0;
        return false;
    }

    /// <summary>
    ///     Returns 10^(<paramref name="precision" /> - <paramref name="scale" />), the smallest number too large for a
    ///     decimal(precision,scale) column: 100 for decimal(3,1), which holds up to 99.9, and 0.01 for numeric(3,5).
    /// </summary>
    /// <remarks>
    ///     The limit is exact, where 10^(p-s) - 10^-s, the largest value of the column, cannot always be held by a
    ///     decimal: 9999999999999999999999999999.9 for decimal(29,1) rounds to 10^28. Values with too many places after
    ///     the decimal point have to be rejected separately.
    /// </remarks>
    /// <returns>
    ///     Null if the precision is not known (0) or every decimal fits in the column, because it has 29 or more digits
    ///     before the decimal point. 10^-28 if the column has more than 28 zeros after the decimal point, because no
    ///     decimal but 0 is smaller than 10^-28.
    /// </returns>
    public static decimal? GetLimit(int precision, int scale)
    {
        if (precision <= 0)
            return null;

        var digitsBeforePoint = precision - scale;
        if (digitsBeforePoint > MaxDecimalDigits)
            return null;

        if (digitsBeforePoint < 0)
            return new decimal(1, 0, 0, false, (byte)Math.Min(-digitsBeforePoint, MaxDecimalDigits));

        var limit = 1m;
        for (var i = 0; i < digitsBeforePoint; i++)
            limit *= 10;

        return limit;
    }

    /// <summary>
    ///     Returns the largest value of a decimal(<paramref name="precision" />,<paramref name="scale" />) column, as
    ///     text because a decimal cannot always hold it: 99.9 for decimal(3,1), 9999999999999999999999999999.9 for
    ///     decimal(29,1) and 0.00999 for numeric(3,5).
    /// </summary>
    /// <param name="precision">The precision of the column, from 1</param>
    /// <param name="scale">The scale of the column, from 0</param>
    public static string FormatMaxValue(int precision, int scale)
    {
        if (scale == 0)
            return new string('9', precision);

        var digitsBeforePoint = precision - scale;
        return digitsBeforePoint > 0
            ? $"{new string('9', digitsBeforePoint)}.{new string('9', scale)}"
            : $"0.{new string('0', -digitsBeforePoint)}{new string('9', precision)}";
    }

    [GeneratedRegex(@"\(\s*([0-9]+)\s*,\s*([0-9]+)\s*\)", RegexOptions.CultureInvariant)]
    private static partial Regex PrecisionAndScaleRegex();
}
