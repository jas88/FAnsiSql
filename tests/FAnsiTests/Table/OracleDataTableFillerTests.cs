using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using FAnsi.Implementations.Oracle;
using NUnit.Framework;

namespace FAnsiTests.Table;

/// <summary>
///     Tests the general part of the <see cref="OracleDataTableFiller" />, which reads Oracle NUMBER columns as
///     decimals
/// </summary>
internal sealed class OracleDataTableFillerTests
{
    [Test]
    public void Fill_ColumnReadAsDecimal_GetsTheDecimal()
    {
        using var source = CreateNumbers();
        using var reader = new NumberAsFloatReader(source, 1);
        using var dt = new DataTable();

        var rows = OracleDataTableFiller.Fill(reader, dt, static (r, i) => r.GetFieldType(i) == typeof(float),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(rows, Is.EqualTo(3));
            Assert.That(dt.Columns["Sanity"]?.DataType, Is.EqualTo(typeof(decimal)));
            Assert.That(ColumnValues(dt, "Sanity"), Is.EqualTo(new object[] { 0.50m, DBNull.Value, 9.99m }));
            Assert.That(ColumnValues(dt, "Name"), Is.EqualTo(new object[] { "Frank", "Tony", "Jez" }));
        });
    }

    [Test]
    public void Fill_ColumnReadAsDecimal_GoesIntoExistingColumn()
    {
        using var source = CreateNumbers();
        using var reader = new NumberAsFloatReader(source, 1);
        using var dt = new DataTable();
        dt.Columns.Add("SANITY", typeof(decimal));

        OracleDataTableFiller.Fill(reader, dt, static (r, i) => r.GetFieldType(i) == typeof(float),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(DescribeColumns(dt), Is.EqualTo(new object[] { "SANITY:Decimal", "Name:String" }));
            Assert.That(ColumnValues(dt, "SANITY"), Is.EqualTo(new object[] { 0.50m, DBNull.Value, 9.99m }));
        });
    }

    [Test]
    public void Fill_ColumnNotReadAsDecimal_GetsTheValue()
    {
        using var source = CreateNumbers();
        using var reader = new NumberAsFloatReader(source, 1);
        using var dt = new DataTable();

        OracleDataTableFiller.Fill(reader, dt, static (_, _) => false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(dt.Columns["Sanity"]?.DataType, Is.EqualTo(typeof(float)));
            Assert.That(ColumnValues(dt, "Sanity"), Is.EqualTo(new object[] { 0.5f, DBNull.Value, 9.99f }));
        });
    }

    /// <summary>
    ///     Columns not read as decimals are filled as <see cref="DbDataAdapter" /> fills them: into the column with
    ///     the same name, or the same name ignoring case, or a new column, with repeated names numbered, and the rows
    ///     unchanged
    /// </summary>
    [TestCase(false)]
    [TestCase(true)]
    public void Fill_OtherColumns_AsDataAdapterFillsThem(bool withExistingColumns)
    {
        using var source = new DataTable();
        source.Columns.Add("Name", typeof(string));
        source.Columns.Add("NAME", typeof(string));
        source.Columns.Add("Age", typeof(int));
        source.Columns.Add("Height", typeof(double));
        source.Rows.Add("Frank", "Frankie", 50, 1.8);
        source.Rows.Add("Tony", DBNull.Value, DBNull.Value, 1.75);

        using var expected = CreateTarget(withExistingColumns);
        using var actual = CreateTarget(withExistingColumns);

        using (var adapter = new ReaderAdapter())
        using (var reader = source.CreateDataReader())
            adapter.FillFrom(expected, reader);

        using (var reader = source.CreateDataReader())
            OracleDataTableFiller.Fill(reader, actual, static (_, _) => false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(DescribeColumns(actual), Is.EqualTo(DescribeColumns(expected)));
            Assert.That(actual.Rows.Cast<DataRow>().Select(static r => r.ItemArray),
                Is.EqualTo(expected.Rows.Cast<DataRow>().Select(static r => r.ItemArray)));
            Assert.That(actual.Rows.Cast<DataRow>().Select(static r => r.RowState),
                Is.EqualTo(expected.Rows.Cast<DataRow>().Select(static r => r.RowState)));
        });
    }

    [Test]
    public void Fill_Cancelled_Throws()
    {
        using var source = CreateNumbers();
        using var reader = source.CreateDataReader();
        using var dt = new DataTable();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.Throws<OperationCanceledException>(() =>
            OracleDataTableFiller.Fill(reader, dt, static (_, _) => false, cancellation.Token));
        Assert.That(dt.Rows, Is.Empty);
    }

    private static DataTable CreateNumbers()
    {
        var dt = new DataTable();
        dt.Columns.Add("Name", typeof(string));
        dt.Columns.Add("Sanity", typeof(decimal));
        dt.Rows.Add("Frank", 0.50m);
        dt.Rows.Add("Tony", DBNull.Value);
        dt.Rows.Add("Jez", 9.99m);
        return dt;
    }

    private static DataTable CreateTarget(bool withExistingColumns)
    {
        var dt = new DataTable();
        if (!withExistingColumns)
            return dt;

        dt.Columns.Add("Extra", typeof(int));
        dt.Columns.Add("age", typeof(decimal));
        dt.Columns.Add("NAME1", typeof(string));
        return dt;
    }

    private static object[] ColumnValues(DataTable dt, string column) =>
        dt.Rows.Cast<DataRow>().Select(r => r[column]).ToArray();

    private static string[] DescribeColumns(DataTable dt) =>
        dt.Columns.Cast<DataColumn>().Select(static c => $"{c.ColumnName}:{c.DataType.Name}").ToArray();

    /// <summary>
    ///     Exposes the Fill from a data reader that <see cref="DbDataAdapter.Fill(DataTable)" /> uses
    /// </summary>
    private sealed class ReaderAdapter : DataAdapter
    {
        public void FillFrom(DataTable dt, IDataReader reader) => Fill(dt, reader);
    }

    /// <summary>
    ///     Reads a table as ODP.NET reads NUMBER(5,2) values: the decimal column <paramref name="ordinal" /> has the
    ///     field type float, and GetValue gives its value as a float, while GetDecimal gives the decimal
    /// </summary>
    private sealed class NumberAsFloatReader(DataTable source, int ordinal) : DbDataReader
    {
        private readonly DataTableReader _inner = source.CreateDataReader();

        public override object this[int i] => GetValue(i);
        public override object this[string name] => GetValue(GetOrdinal(name));
        public override int Depth => _inner.Depth;
        public override int FieldCount => _inner.FieldCount;
        public override bool HasRows => _inner.HasRows;
        public override bool IsClosed => _inner.IsClosed;
        public override int RecordsAffected => _inner.RecordsAffected;

        public override bool GetBoolean(int i) => _inner.GetBoolean(i);
        public override byte GetByte(int i) => _inner.GetByte(i);

        public override long GetBytes(int i, long dataOffset, byte[]? buffer, int bufferOffset, int length) =>
            _inner.GetBytes(i, dataOffset, buffer, bufferOffset, length);

        public override char GetChar(int i) => _inner.GetChar(i);

        public override long GetChars(int i, long dataOffset, char[]? buffer, int bufferOffset, int length) =>
            _inner.GetChars(i, dataOffset, buffer, bufferOffset, length);

        public override string GetDataTypeName(int i) => GetFieldType(i).Name;
        public override DateTime GetDateTime(int i) => _inner.GetDateTime(i);
        public override decimal GetDecimal(int i) => _inner.GetDecimal(i);
        public override double GetDouble(int i) => _inner.GetDouble(i);
        public override IEnumerator GetEnumerator() => new DbEnumerator(this);

        [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields |
                                            DynamicallyAccessedMemberTypes.PublicProperties)]
        public override Type GetFieldType(int i) => i == ordinal ? typeof(float) : _inner.GetFieldType(i);

        public override float GetFloat(int i) => i == ordinal ? (float)_inner.GetDecimal(i) : _inner.GetFloat(i);
        public override Guid GetGuid(int i) => _inner.GetGuid(i);
        public override short GetInt16(int i) => _inner.GetInt16(i);
        public override int GetInt32(int i) => _inner.GetInt32(i);
        public override long GetInt64(int i) => _inner.GetInt64(i);
        public override string GetName(int i) => _inner.GetName(i);
        public override int GetOrdinal(string name) => _inner.GetOrdinal(name);
        public override string GetString(int i) => _inner.GetString(i);

        public override object GetValue(int i) =>
            i == ordinal && !_inner.IsDBNull(i) ? GetFloat(i) : _inner.GetValue(i);

        public override int GetValues(object[] values)
        {
            var count = Math.Min(values.Length, FieldCount);
            for (var i = 0; i < count; i++)
                values[i] = GetValue(i);
            return count;
        }

        public override bool IsDBNull(int i) => _inner.IsDBNull(i);
        public override bool NextResult() => _inner.NextResult();
        public override bool Read() => _inner.Read();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
