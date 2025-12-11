using System.Text.RegularExpressions;
using FAnsi.Discovery.TypeTranslation;

namespace FAnsi.Implementations.MicrosoftSQL;

public sealed partial class MicrosoftSQLTypeTranslater : TypeTranslater
{
    public static readonly MicrosoftSQLTypeTranslater Instance = new();

    private static readonly Regex AlsoBinaryRegex = AlsoBinaryRe();

    private MicrosoftSQLTypeTranslater() : base(DateRe(), 8000, 4000)
    {
    }

    /// <summary>
    ///     Microsoft SQL lacks any sane boolean support, so use a 'BIT' instead
    /// </summary>
    /// <returns></returns>
    protected override string GetBoolDataType() => "bit";

    protected override string GetDateDateTimeDataType() => "datetime2";

    public override string GetStringDataTypeWithUnlimitedWidth() => "varchar(max)";

    public override string GetUnicodeStringDataTypeWithUnlimitedWidth() => "nvarchar(max)";

    protected override bool IsByteArray(string sqlType) =>
        base.IsByteArray(sqlType) || AlsoBinaryRegex.IsMatch(sqlType);

    protected override string GetByteArrayDataType() => "varbinary(max)";

    protected override string GetStringDataTypeImpl(int maxExpectedStringWidth) => $"varchar({maxExpectedStringWidth})";

    protected override string GetUnicodeStringDataTypeImpl(int maxExpectedStringWidth) =>
        $"nvarchar({maxExpectedStringWidth})";

    protected override string GetGuidDataType() => "uniqueidentifier";

    [GeneratedRegex("date", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex DateRe();

    [GeneratedRegex("(image)|(timestamp)|(rowversion)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex AlsoBinaryRe();
}
