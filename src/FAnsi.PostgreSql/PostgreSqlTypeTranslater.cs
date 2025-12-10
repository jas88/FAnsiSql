using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using FAnsi.Discovery.TypeTranslation;
using NpgsqlTypes;

namespace FAnsi.Implementations.PostgreSql;

public sealed partial class PostgreSqlTypeTranslater : TypeTranslater
{
    public static readonly PostgreSqlTypeTranslater Instance = new();

    private static readonly CompositeFormat TypeNotMappedExceptionFormat =
        CompositeFormat.Parse(FAnsiStrings.TypeTranslater_GetSQLDBTypeForCSharpType_Unsure_what_SQL_type_to_use_for_CSharp_Type___0_____TypeTranslater_was___1__);

    private static readonly FrozenDictionary<Type, NpgsqlDbType> TypeMappings =
        new Dictionary<Type, NpgsqlDbType>
        {
            [typeof(bool)] = NpgsqlDbType.Boolean,
            [typeof(bool?)] = NpgsqlDbType.Boolean,
            [typeof(byte)] = NpgsqlDbType.Smallint,
            [typeof(byte[])] = NpgsqlDbType.Bytea,
            [typeof(short)] = NpgsqlDbType.Smallint,
            [typeof(short?)] = NpgsqlDbType.Smallint,
            [typeof(ushort)] = NpgsqlDbType.Smallint,
            [typeof(ushort?)] = NpgsqlDbType.Smallint,
            [typeof(int)] = NpgsqlDbType.Integer,
            [typeof(int?)] = NpgsqlDbType.Integer,
            [typeof(uint)] = NpgsqlDbType.Integer,
            [typeof(uint?)] = NpgsqlDbType.Integer,
            [typeof(long)] = NpgsqlDbType.Bigint,
            [typeof(long?)] = NpgsqlDbType.Bigint,
            [typeof(ulong)] = NpgsqlDbType.Bigint,
            [typeof(ulong?)] = NpgsqlDbType.Bigint,
            [typeof(float)] = NpgsqlDbType.Double,
            [typeof(float?)] = NpgsqlDbType.Double,
            [typeof(double)] = NpgsqlDbType.Double,
            [typeof(double?)] = NpgsqlDbType.Double,
            [typeof(decimal)] = NpgsqlDbType.Numeric,
            [typeof(decimal?)] = NpgsqlDbType.Numeric,
            [typeof(string)] = NpgsqlDbType.Text,
            [typeof(DateTime)] = NpgsqlDbType.Timestamp,
            [typeof(DateTime?)] = NpgsqlDbType.Timestamp,
            [typeof(DateOnly)] = NpgsqlDbType.Date,
            [typeof(DateOnly?)] = NpgsqlDbType.Date,
            [typeof(TimeSpan)] = NpgsqlDbType.Time,
            [typeof(TimeSpan?)] = NpgsqlDbType.Time,
            [typeof(TimeOnly)] = NpgsqlDbType.Time,
            [typeof(TimeOnly?)] = NpgsqlDbType.Time,
            [typeof(Guid)] = NpgsqlDbType.Uuid
        }.ToFrozenDictionary();

    private PostgreSqlTypeTranslater() : base(DateRegexImpl(), 8000, 4000)
    {
        TimeRegex = TimeRegexImpl(); //space is important
    }

    public override string GetStringDataTypeWithUnlimitedWidth() => "text";

    protected override string GetUnicodeStringDataTypeImpl(int maxExpectedStringWidth) => GetStringDataType(maxExpectedStringWidth);

    protected override string GetStringDataTypeImpl(int maxExpectedStringWidth) => $"varchar({maxExpectedStringWidth})";

    public override string GetUnicodeStringDataTypeWithUnlimitedWidth() => "text";

    protected override string GetDateDateTimeDataType() => "timestamp";

    protected override string GetByteArrayDataType() => "bytea";

    public NpgsqlDbType GetNpgsqlDbTypeForCSharpType(Type t) =>
        TypeMappings.TryGetValue(t, out var npgsqlType)
            ? npgsqlType
            : throw new TypeNotMappedException(string.Format(CultureInfo.InvariantCulture, TypeNotMappedExceptionFormat, t.Name, GetType().Name));

    protected override bool IsByteArray(string sqlType) =>
        sqlType?.StartsWith("bytea", StringComparison.OrdinalIgnoreCase) ?? false;

    [GeneratedRegex("timestamp", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex DateRegexImpl();
    [GeneratedRegex("^time ", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex TimeRegexImpl();
}
