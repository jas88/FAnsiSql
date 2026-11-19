using System.Data;
using System.Numerics;

namespace FAnsi.Discovery;

/// <summary>
///     Pre-computed validation rules for a column. Avoids repeated lookups during row iteration.
///     Used by <see cref="BulkCopy.PreProcessAndValidate" /> for single-pass data validation.
/// </summary>
public readonly struct ColumnValidationRule(
    DataColumn dataColumn,
    DiscoveredColumn discoveredColumn,
    int ordinal,
    bool isString,
    int maxStringLength,
    bool isDecimal,
    int decimalPrecision,
    int decimalScale,
    bool hasIntegerRange,
    BigInteger integerMin,
    BigInteger integerMax,
    bool requiresNotNull)
{
    /// <summary>
    ///     Creates a rule whose decimal values are checked against the range of
    ///     decimal(<paramref name="decimalPrecision" />,<paramref name="decimalScale" />), as the other constructor
    ///     does. <paramref name="maxDecimalValue" /> is only kept in <see cref="MaxDecimalValue" />.
    /// </summary>
    [Obsolete("The range of a decimal column is worked out from its precision and scale: use the constructor " +
              "without maxDecimalValue")]
    public ColumnValidationRule(DataColumn dataColumn, DiscoveredColumn discoveredColumn, int ordinal, bool isString,
        int maxStringLength, bool isDecimal, int decimalPrecision, int decimalScale, decimal maxDecimalValue,
        bool hasIntegerRange, BigInteger integerMin, BigInteger integerMax, bool requiresNotNull)
        : this(dataColumn, discoveredColumn, ordinal, isString, maxStringLength, isDecimal, decimalPrecision,
            decimalScale, hasIntegerRange, integerMin, integerMax, requiresNotNull)
    {
        MaxDecimalValue = maxDecimalValue;
    }

    public readonly int Ordinal = ordinal;
    public readonly string SourceName = dataColumn.ColumnName;
    public readonly string DestName = discoveredColumn.GetRuntimeName();
    public readonly string? SqlType = discoveredColumn.DataType?.SQLType;
    public readonly bool IsString = isString;
    public readonly int MaxStringLength = maxStringLength;
    public readonly bool IsDecimal = isDecimal;

    /// <summary>
    ///     The precision of the decimal column, or 0 if it is not known
    /// </summary>
    public readonly int DecimalPrecision = decimalPrecision;

    /// <summary>
    ///     The scale of the decimal column, which can be larger than its precision, as in numeric(3,5)
    /// </summary>
    public readonly int DecimalScale = decimalScale;

    /// <summary>
    ///     The value given to the obsolete constructor, otherwise 0. A decimal cannot always hold the largest value of
    ///     a decimal column, such as 9999999999999999999999999999.9 for decimal(29,1).
    /// </summary>
    [Obsolete("A decimal cannot hold the largest value of every decimal column: use DecimalLimit and " +
              "MaxDecimalValueText")]
    public readonly decimal MaxDecimalValue;

    /// <summary>
    ///     The smallest number too large for the column, 10^(<see cref="DecimalPrecision" /> -
    ///     <see cref="DecimalScale" />), or null if the column is not decimal, its precision is not known or every
    ///     decimal fits in it.
    /// </summary>
    public readonly decimal? DecimalLimit =
        isDecimal ? DecimalColumnRange.GetLimit(decimalPrecision, decimalScale) : null;

    public readonly bool HasIntegerRange = hasIntegerRange;
    public readonly BigInteger IntegerMin = integerMin;
    public readonly BigInteger IntegerMax = integerMax;
    public readonly bool RequiresNotNull = requiresNotNull;

    /// <summary>
    ///     The largest value of the column as text, such as 99.9 for decimal(3,1), or null if the column is not decimal
    ///     or its precision is not known.
    /// </summary>
    public string? MaxDecimalValueText => IsDecimal && DecimalPrecision > 0
        ? DecimalColumnRange.FormatMaxValue(DecimalPrecision, DecimalScale)
        : null;
}
