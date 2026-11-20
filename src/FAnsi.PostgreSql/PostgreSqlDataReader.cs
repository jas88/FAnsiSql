using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace FAnsi.Implementations.PostgreSql;

/// <summary>
///     Reads the result of a PostgreSQL query with the C# types <see cref="PostgreSqlTypeTranslater" /> gives its columns,
///     which are the types the other DBMS read their dates and times as.  Npgsql 10 reads a 'date' as a
///     <see cref="DateOnly" />, which is read here as the <see cref="DateTime" /> at midnight at the start of that day,
///     and a 'time' as a <see cref="TimeOnly" />, read here as a <see cref="TimeSpan" />.  It reads a 'time with time
///     zone' as a <see cref="DateTimeOffset" />, which is read here as the <see cref="TimeSpan" /> of its time of day as
///     written, without the offset.  Every other value is read as Npgsql reads it.
/// </summary>
/// <remarks>
///     <see cref="PostgreSqlDataAdapter" /> fills a <see cref="DataTable" /> through this, so that the
///     <see cref="DataTable" /> FAnsi reads from PostgreSQL has the same column types as one read from another DBMS.
/// </remarks>
internal sealed class PostgreSqlDataReader(IDataReader reader) : IDataReader
{
    private Conversion[]? _conversions;

    /// <summary>
    ///     How the values of each column of the current result are converted, decided once per result
    /// </summary>
    private Conversion[] Conversions => _conversions ??= GetConversions();

    public object this[int i] => GetValue(i);

    public object this[string name] => GetValue(reader.GetOrdinal(name));

    public int Depth => reader.Depth;

    public bool IsClosed => reader.IsClosed;

    public int RecordsAffected => reader.RecordsAffected;

    public int FieldCount => reader.FieldCount;

    public void Close() => reader.Close();

    public void Dispose() => reader.Dispose();

    public DataTable? GetSchemaTable()
    {
        var schema = reader.GetSchemaTable();
        var ordinal = schema?.Columns[SchemaTableColumn.ColumnOrdinal];
        var dataType = schema?.Columns[SchemaTableColumn.DataType];

        if (schema == null || ordinal == null || dataType == null)
            return schema;

        foreach (DataRow row in schema.Rows)
            if (row[ordinal] is int i && i >= 0 && i < Conversions.Length && Conversions[i] != Conversion.None)
                row[dataType] = GetFieldType(i);

        return schema;
    }

    public bool NextResult()
    {
        _conversions = null;
        return reader.NextResult();
    }

    public bool Read() => reader.Read();

    public bool GetBoolean(int i) => reader.GetBoolean(i);

    public byte GetByte(int i) => reader.GetByte(i);

    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) =>
        reader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);

    public char GetChar(int i) => reader.GetChar(i);

    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) =>
        reader.GetChars(i, fieldoffset, buffer, bufferoffset, length);

    public IDataReader GetData(int i) => reader.GetData(i);

    public string GetDataTypeName(int i) => reader.GetDataTypeName(i);

    public DateTime GetDateTime(int i) => reader.GetDateTime(i);

    public decimal GetDecimal(int i) => reader.GetDecimal(i);

    public double GetDouble(int i) => reader.GetDouble(i);

    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields |
                                        DynamicallyAccessedMemberTypes.PublicProperties)]
    public Type GetFieldType(int i) =>
        Conversions[i] switch
        {
            Conversion.DateOnlyToDateTime => typeof(DateTime),
            Conversion.TimeOnlyToTimeSpan or Conversion.DateTimeOffsetToTimeOfDay => typeof(TimeSpan),
            _ => reader.GetFieldType(i)
        };

    public float GetFloat(int i) => reader.GetFloat(i);

    public Guid GetGuid(int i) => reader.GetGuid(i);

    public short GetInt16(int i) => reader.GetInt16(i);

    public int GetInt32(int i) => reader.GetInt32(i);

    public long GetInt64(int i) => reader.GetInt64(i);

    public string GetName(int i) => reader.GetName(i);

    public int GetOrdinal(string name) => reader.GetOrdinal(name);

    public string GetString(int i) => reader.GetString(i);

    public object GetValue(int i) => Convert(Conversions[i], reader.GetValue(i));

    public int GetValues(object[] values)
    {
        var count = reader.GetValues(values);

        var conversions = Conversions;
        for (var i = 0; i < count; i++)
            if (conversions[i] != Conversion.None)
                values[i] = Convert(conversions[i], values[i]);

        return count;
    }

    public bool IsDBNull(int i) => reader.IsDBNull(i);

    private Conversion[] GetConversions()
    {
        var conversions = new Conversion[reader.FieldCount];

        for (var i = 0; i < conversions.Length; i++)
        {
            var fieldType = reader.GetFieldType(i);

            if (fieldType == typeof(DateOnly))
                conversions[i] = Conversion.DateOnlyToDateTime;
            else if (fieldType == typeof(TimeOnly))
                conversions[i] = Conversion.TimeOnlyToTimeSpan;
            //Npgsql reads only a 'time with time zone' as a DateTimeOffset, but make sure it is one
            else if (fieldType == typeof(DateTimeOffset) &&
                     PostgreSqlTypeTranslater.Instance.TryGetCSharpTypeForSQLDBType(reader.GetDataTypeName(i)) ==
                     typeof(TimeSpan))
                conversions[i] = Conversion.DateTimeOffsetToTimeOfDay;
        }

        return conversions;
    }

    private static object Convert(Conversion conversion, object value) =>
        (conversion, value) switch
        {
            (Conversion.DateOnlyToDateTime, DateOnly date) => date.ToDateTime(TimeOnly.MinValue),
            (Conversion.TimeOnlyToTimeSpan, TimeOnly time) => time.ToTimeSpan(),
            (Conversion.DateTimeOffsetToTimeOfDay, DateTimeOffset time) => time.TimeOfDay,
            _ => value
        };

    private enum Conversion
    {
        None,
        DateOnlyToDateTime,
        TimeOnlyToTimeSpan,
        DateTimeOffsetToTimeOfDay
    }
}
