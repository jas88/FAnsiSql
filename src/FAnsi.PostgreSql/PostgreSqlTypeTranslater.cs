using System.Collections.Frozen;
using System.Data;
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
        CompositeFormat.Parse(FAnsiStrings
            .TypeTranslater_GetSQLDBTypeForCSharpType_Unsure_what_SQL_type_to_use_for_CSharp_Type___0_____TypeTranslater_was___1__);

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

    /// <summary>
    ///     The column data types (as named by information_schema) that take values of more than one C# type, with the
    ///     type Npgsql writes them as
    /// </summary>
    private static readonly FrozenDictionary<string, NpgsqlDbType> SqlTypeMappings =
        new Dictionary<string, NpgsqlDbType>
        {
            ["smallint"] = NpgsqlDbType.Smallint,
            ["integer"] = NpgsqlDbType.Integer,
            ["bigint"] = NpgsqlDbType.Bigint,
            ["numeric"] = NpgsqlDbType.Numeric,
            ["real"] = NpgsqlDbType.Real,
            ["double precision"] = NpgsqlDbType.Double,
            ["character"] = NpgsqlDbType.Char,
            ["character varying"] = NpgsqlDbType.Varchar,
            ["text"] = NpgsqlDbType.Text,
            ["timestamp without time zone"] = NpgsqlDbType.Timestamp,
            ["timestamp with time zone"] = NpgsqlDbType.TimestampTz
        }.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);

    private PostgreSqlTypeTranslater() : base(DateRegexImpl(), 8000, 4000)
    {
        TimeRegex = TimeRegexImpl(); //space is important
    }

    public override string GetStringDataTypeWithUnlimitedWidth() => "text";

    protected override string GetUnicodeStringDataTypeImpl(int maxExpectedStringWidth) =>
        GetStringDataType(maxExpectedStringWidth);

    protected override string GetStringDataTypeImpl(int maxExpectedStringWidth) => $"varchar({maxExpectedStringWidth})";

    public override string GetUnicodeStringDataTypeWithUnlimitedWidth() => "text";

    protected override string GetDateDateTimeDataType() => "timestamp";

    protected override string GetByteArrayDataType() => "bytea";

    public NpgsqlDbType GetNpgsqlDbTypeForCSharpType(Type t) =>
        TypeMappings.TryGetValue(t, out var npgsqlType)
            ? npgsqlType
            : throw new TypeNotMappedException(string.Format(CultureInfo.InvariantCulture, TypeNotMappedExceptionFormat,
                t.Name, GetType().Name));

    /// <summary>
    ///     Returns the type Npgsql must write values as for them to be accepted by a column of the given
    ///     <paramref name="sqlType" /> (as named by information_schema e.g. "character varying(10)") or null if it is a
    ///     type that only takes values of the one C# type.
    /// </summary>
    internal NpgsqlDbType? GetNpgsqlDbTypeForSQLDBType(string sqlType)
    {
        //strip any length / precision e.g. "numeric(10,2)"
        var bracket = sqlType.IndexOf('(', StringComparison.Ordinal);
        var typeName = bracket == -1 ? sqlType : sqlType[..bracket];

        return SqlTypeMappings.TryGetValue(typeName.Trim(), out var npgsqlType) ? npgsqlType : null;
    }

    /// <inheritdoc />
    /// <remarks>
    ///     Npgsql sends <see cref="DbType.DateTime" /> as 'timestamp with time zone' and only accepts UTC values for it.  The
    ///     'timestamp' (without time zone) columns that FAnsi creates for dates need <see cref="DbType.DateTime2" />.
    /// </remarks>
    public override DbType GetDbTypeForSQLDBType(string sqlType)
    {
        var dbType = base.GetDbTypeForSQLDBType(sqlType);

        return dbType == DbType.DateTime && !IsTimestampWithTimeZone(sqlType) ? DbType.DateTime2 : dbType;
    }

    /// <summary>
    ///     Returns <paramref name="dateTime" /> as Npgsql must be given it for a 'timestamp' column, or for a 'timestamp
    ///     with time zone' column when <paramref name="withTimeZone" /> is true.  What is stored for each
    ///     <see cref="DateTimeKind" /> is decided here and nowhere else, so that it does not depend on how the value is
    ///     inserted:
    ///     <list type="bullet">
    ///         <item>
    ///             A 'timestamp' has no time zone.  It stores the date and time exactly as given whatever the Kind, as the
    ///             other DBMS do (Npgsql will not write a Utc value to one, so the Kind is removed).
    ///         </item>
    ///         <item>
    ///             A 'timestamp with time zone' stores an instant and Npgsql will only write it a Utc value.  A Local
    ///             value is converted to UTC.  An Unspecified value is taken to be UTC already: that does not depend on
    ///             the time zone of the machine and it is what a value read from such a column is once it has been put
    ///             in a DataTable, so data read from one table is stored unchanged in another.
    ///         </item>
    ///     </list>
    /// </summary>
    internal static DateTime ToTimestamp(DateTime dateTime, bool withTimeZone)
    {
        if (!withTimeZone)
            return DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified);

        return dateTime.Kind == DateTimeKind.Local
            ? dateTime.ToUniversalTime()
            : DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
    }

    private static bool IsTimestampWithTimeZone(string sqlType) =>
        sqlType.Contains("with time zone", StringComparison.OrdinalIgnoreCase) ||
        sqlType.Contains("timestamptz", StringComparison.OrdinalIgnoreCase);

    protected override bool IsByteArray(string sqlType) =>
        sqlType?.StartsWith("bytea", StringComparison.OrdinalIgnoreCase) ?? false;

    [GeneratedRegex("timestamp", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex DateRegexImpl();

    [GeneratedRegex("^time ", RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex TimeRegexImpl();
}
