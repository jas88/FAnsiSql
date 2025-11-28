using System;
using System.Text.RegularExpressions;
using FAnsi.Discovery.Helpers;
using FAnsi.Discovery.TypeTranslation;
using TypeGuesser;

namespace FAnsi.Implementations.Sqlite;

/// <summary>
/// SQLite-specific implementation of type translation. Handles mapping between C# types,
/// database types, and SQL type names according to SQLite's type affinity system.
/// </summary>
/// <remarks>
/// <para>SQLite uses a type affinity system rather than strict typing:</para>
/// <list type="bullet">
/// <item><description>TEXT: String data</description></item>
/// <item><description>INTEGER: Signed integers (1, 2, 3, 4, 6, or 8 bytes)</description></item>
/// <item><description>REAL: Floating point numbers (8 bytes)</description></item>
/// <item><description>BLOB: Binary data</description></item>
/// <item><description>NULL: Null value</description></item>
/// </list>
/// <para>SQLite is very flexible - any data type can be stored in any column regardless of declared type.</para>
/// </remarks>
public sealed partial class SqliteTypeTranslater : TypeTranslater
{
    /// <summary>
    /// Gets the singleton instance of <see cref="SqliteTypeTranslater"/>.
    /// </summary>
    public static readonly SqliteTypeTranslater Instance = new();

    private SqliteTypeTranslater() : base(DateRe(), 1000000, 1000000)  // SQLite is quite flexible with string lengths
    {
        // SQLite is very flexible with types, but we'll follow some conventions
        ByteRegex = ByteRe();
        SmallIntRegex = SmallIntRe();
        IntRegex = IntRe();
        LongRegex = LongRe();
    }

    /// <inheritdoc />
    public override int GetLengthIfString(string sqlType) => base.GetLengthIfString(sqlType);

    /// <summary>
    /// Gets the SQL type for strings with unlimited width.
    /// </summary>
    /// <returns>"TEXT" (SQLite's unlimited-length string type)</returns>
    public override string GetStringDataTypeWithUnlimitedWidth() => "TEXT";

    /// <summary>
    /// Gets the SQL type for Unicode strings with unlimited width.
    /// </summary>
    /// <returns>"TEXT" (SQLite natively supports Unicode)</returns>
    public override string GetUnicodeStringDataTypeWithUnlimitedWidth() => "TEXT";

    /// <summary>
    /// Gets the SQL type for Unicode strings with specified maximum width.
    /// </summary>
    /// <param name="maxExpectedStringWidth">The maximum expected string width (ignored for SQLite)</param>
    /// <returns>"TEXT" (SQLite TEXT has no length limit)</returns>
    protected override string GetUnicodeStringDataTypeImpl(int maxExpectedStringWidth) =>
        maxExpectedStringWidth > 0 ? $"TEXT" : "TEXT";  // SQLite TEXT can handle any length

    /// <inheritdoc />
    protected override bool IsBool(string sqlType) =>
        base.IsBool(sqlType) ||
        StringComparisonHelper.SqlTypesEqual(sqlType, "BOOLEAN");

    /// <inheritdoc />
    protected override bool IsInt(string sqlType) =>
        base.IsInt(sqlType) ||
        StringComparisonHelper.SqlTypesEqual(sqlType, "INTEGER");

    /// <inheritdoc />
    protected override bool IsString(string sqlType) =>
        base.IsString(sqlType) ||
        StringComparisonHelper.SqlTypesEqual(sqlType, "TEXT") ||
        StringComparisonHelper.SqlTypesEqual(sqlType, "CLOB");

    /// <inheritdoc />
    protected override bool IsFloatingPoint(string sqlType) =>
        base.IsFloatingPoint(sqlType) ||
        StringComparisonHelper.SqlTypesEqual(sqlType, "REAL");

    /// <inheritdoc />
    protected override bool IsBit(string sqlType) => base.IsBit(sqlType);

    /// <summary>
    /// Gets the SQL type for boolean values.
    /// </summary>
    /// <returns>"INTEGER" (SQLite stores booleans as 0/1)</returns>
    protected override string GetBoolDataType() => "INTEGER";  // SQLite uses INTEGER for boolean (0/1)

    /// <summary>
    /// Gets the SQL type for small integers.
    /// </summary>
    /// <returns>"INTEGER" (all integer types use INTEGER in SQLite)</returns>
    protected override string GetSmallIntDataType() => "INTEGER";

    /// <summary>
    /// Gets the SQL type for integers.
    /// </summary>
    /// <returns>"INTEGER"</returns>
    protected override string GetIntDataType() => "INTEGER";

    /// <summary>
    /// Gets the SQL type for big integers.
    /// </summary>
    /// <returns>"INTEGER" (SQLite INTEGER can store up to 8 bytes)</returns>
    protected override string GetBigIntDataType() => "INTEGER";

    /// <summary>
    /// Gets the SQL type for time values.
    /// </summary>
    /// <returns>"TEXT" (SQLite stores times as text strings)</returns>
    protected override string GetTimeDataType() => "TEXT";

    /// <summary>
    /// Gets the SQL type for date/datetime values.
    /// </summary>
    /// <returns>"TEXT" (SQLite stores dates as text, though numeric and real are also supported)</returns>
    /// <remarks>
    /// SQLite doesn't have dedicated date/time types. Dates can be stored as:
    /// <list type="bullet">
    /// <item><description>TEXT as ISO8601 strings ("YYYY-MM-DD HH:MM:SS.SSS")</description></item>
    /// <item><description>REAL as Julian day numbers</description></item>
    /// <item><description>INTEGER as Unix Time (seconds since 1970-01-01 00:00:00 UTC)</description></item>
    /// </list>
    /// </remarks>
    protected override string GetDateDateTimeDataType() => "TEXT";  // SQLite stores dates as text, numeric, or real

    /// <summary>
    /// Gets the SQL type for byte array values.
    /// </summary>
    /// <returns>"BLOB" (SQLite's binary data type)</returns>
    protected override string GetByteArrayDataType() => "BLOB";

    [GeneratedRegex(@"^(tinyint)|(int1)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex ByteRe();
    [GeneratedRegex(@"^(smallint)|(int2)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex SmallIntRe();
    [GeneratedRegex(@"^(int)|(integer)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex IntRe();
    [GeneratedRegex(@"^(bigint)|(int8)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex LongRe();
    [GeneratedRegex(@"(date)|(datetime)|(timestamp)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex DateRe();
}
