using System;
using System.Text.RegularExpressions;
using FAnsi.Discovery.TypeTranslation;
using TypeGuesser;

namespace FAnsi.Implementations.Sqlite;

public sealed partial class SqliteTypeTranslater : TypeTranslater
{
    public static readonly SqliteTypeTranslater Instance = new();

    private SqliteTypeTranslater() : base(DateRe(), 1000000, 1000000)  // SQLite is quite flexible with string lengths
    {
        // SQLite is very flexible with types, but we'll follow some conventions
        ByteRegex = ByteRe();
        SmallIntRegex = SmallIntRe();
        IntRegex = IntRe();
        LongRegex = LongRe();
    }

    public override int GetLengthIfString(string sqlType) => base.GetLengthIfString(sqlType);

    public override string GetStringDataTypeWithUnlimitedWidth() => "TEXT";

    public override string GetUnicodeStringDataTypeWithUnlimitedWidth() => "TEXT";

    protected override string GetUnicodeStringDataTypeImpl(int maxExpectedStringWidth) => 
        maxExpectedStringWidth > 0 ? $"TEXT" : "TEXT";  // SQLite TEXT can handle any length

    protected override bool IsBool(string sqlType) => 
        base.IsBool(sqlType) || 
        sqlType.Equals("BOOLEAN", StringComparison.InvariantCultureIgnoreCase);

    protected override bool IsInt(string sqlType) =>
        base.IsInt(sqlType) || 
        sqlType.Equals("INTEGER", StringComparison.InvariantCultureIgnoreCase);

    protected override bool IsString(string sqlType) =>
        base.IsString(sqlType) || 
        sqlType.Equals("TEXT", StringComparison.InvariantCultureIgnoreCase) ||
        sqlType.Equals("CLOB", StringComparison.InvariantCultureIgnoreCase);

    protected override bool IsFloatingPoint(string sqlType) => 
        base.IsFloatingPoint(sqlType) || 
        sqlType.Equals("REAL", StringComparison.InvariantCultureIgnoreCase);

    protected override bool IsBit(string sqlType) => base.IsBit(sqlType);

    // SQLite specific data type methods
    protected override string GetBoolDataType() => "INTEGER";  // SQLite uses INTEGER for boolean (0/1)
    protected override string GetSmallIntDataType() => "INTEGER";
    protected override string GetIntDataType() => "INTEGER";
    protected override string GetBigIntDataType() => "INTEGER";
    protected override string GetTimeDataType() => "TEXT";
    protected override string GetDateDateTimeDataType() => "TEXT";  // SQLite stores dates as text, numeric, or real

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