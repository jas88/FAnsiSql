using System.Text.RegularExpressions;
using FAnsi.Discovery.TypeTranslation;

namespace FAnsi.Implementations.MySql;

public sealed partial class MySqlTypeTranslater : TypeTranslater
{
    public static readonly MySqlTypeTranslater Instance = new();

    //yup that's right!, long is string (MEDIUMTEXT)
    //https://dev.mysql.com/doc/refman/8.0/en/other-vendor-data-types.html
    private static readonly Regex AlsoBitRegex = AlsoBitRe();
    private static readonly Regex AlsoStringRegex = AlsoStringRe();
    private static readonly Regex AlsoFloatingPoint = AlsoFloatingPointRe();

    private MySqlTypeTranslater() : base(DateRe(), 4000, 4000)
    {
        //match bigint and bigint(20) etc
        ByteRegex = ByteRe();
        SmallIntRegex = SmallIntRe();
        IntRegex = IntRe();
        LongRegex = LongRe();
    }

    public override int GetLengthIfString(string sqlType)
    {
        if (string.IsNullOrWhiteSpace(sqlType))
            return -1;

        // Use span-based comparison for better performance - no string allocation
        var sqlTypeSpan = sqlType.AsSpan();

        // Check for text types using ordinal comparison for performance
        if (sqlTypeSpan.Equals("TINYTEXT", StringComparison.OrdinalIgnoreCase))
            return 1 << 8;

        if (sqlTypeSpan.Equals("TEXT", StringComparison.OrdinalIgnoreCase))
            return 1 << 16;

        if (sqlTypeSpan.Equals("MEDIUMTEXT", StringComparison.OrdinalIgnoreCase))
            return 1 << 24;

        if (sqlTypeSpan.Equals("LONGTEXT", StringComparison.OrdinalIgnoreCase))
            return int.MaxValue; // Should be 1<<32 but that overflows...

        return AlsoStringRegex.IsMatch(sqlType) ? int.MaxValue : base.GetLengthIfString(sqlType);
    }

    public override string GetStringDataTypeWithUnlimitedWidth() => "longtext";

    public override string GetUnicodeStringDataTypeWithUnlimitedWidth() => "longtext";

    protected override string GetStringDataTypeImpl(int maxExpectedStringWidth) => $"varchar({maxExpectedStringWidth})";

    protected override string GetUnicodeStringDataTypeImpl(int maxExpectedStringWidth) =>
        $"varchar({maxExpectedStringWidth})";

    protected override bool IsBool(string sqlType)
    {
        // Use span-based comparison for better performance
        if (base.IsBool(sqlType))
            return true;

        var sqlTypeSpan = sqlType.AsSpan();
        return sqlTypeSpan.StartsWith("tinyint(1)", StringComparison.OrdinalIgnoreCase);
    }

    protected override bool IsInt(string sqlType)
    {
        //not an int - use span-based comparison
        var sqlTypeSpan = sqlType.AsSpan();
        if (sqlTypeSpan.StartsWith("int8", StringComparison.OrdinalIgnoreCase))
            return false;

        return base.IsInt(sqlType);
    }

    protected override bool IsString(string sqlType)
    {
        // Use span-based comparison for better performance
        var sqlTypeSpan = sqlType.AsSpan();

        // Exclude binary types (binary, varbinary) and blob types (blob, tinyblob, mediumblob, longblob)
        if (sqlTypeSpan.Contains("binary", StringComparison.OrdinalIgnoreCase) ||
            sqlTypeSpan.Contains("blob", StringComparison.OrdinalIgnoreCase))
            return false;

        return base.IsString(sqlType) || AlsoStringRegex.IsMatch(sqlType);
    }

    protected override bool IsFloatingPoint(string sqlType) =>
        base.IsFloatingPoint(sqlType) || AlsoFloatingPoint.IsMatch(sqlType);

    protected override bool IsBit(string sqlType) => base.IsBit(sqlType) || AlsoBitRegex.IsMatch(sqlType);

    protected override string GetByteArrayDataType() => "longblob";

    protected override string GetGuidDataType() => "char(36)"; // MySQL stores UUIDs as char(36) for compatibility

    protected override string GetDateDateTimeDataType() =>
        "datetime"; // Use datetime instead of timestamp to support full date range (1000-01-01 to 9999-12-31)

    // Optimized regex patterns with ReadOnlySpan<char> compatibility and better performance
    [GeneratedRegex(@"tinyint\(1\)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex AlsoBitRe();

    [GeneratedRegex("(long)|(enum)|(set)|(text)|(mediumtext)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex AlsoStringRe();

    [GeneratedRegex(@"^(tinyint)|(int1)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex ByteRe();

    [GeneratedRegex("^(dec)|(fixed)", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex AlsoFloatingPointRe();

    [GeneratedRegex(@"^(smallint)|(int2)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex SmallIntRe();

    [GeneratedRegex(@"^(int)|(mediumint)|(middleint)|(int3)|(int4)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex IntRe();

    [GeneratedRegex(@"^(bigint)|(int8)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex LongRe();

    [GeneratedRegex(@"(date)|(timestamp)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex DateRe();
}
