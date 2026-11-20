using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Npgsql;

namespace FAnsi.Implementations.PostgreSql;

/// <summary>
///     The <see cref="DbDataAdapter" /> that FAnsi reads PostgreSQL results into a <see cref="DataTable" /> or
///     <see cref="DataSet" /> with.  It reads them through a <see cref="PostgreSqlDataReader" />, so that dates and times
///     have the same C# types as they do on the other DBMS.  It is not an <see cref="NpgsqlDataAdapter" /> because that is
///     sealed.
/// </summary>
internal sealed class PostgreSqlDataAdapter : DbDataAdapter
{
    private const string SchemaTableTypesCannotBeAnalyzed =
        "dataReader's schema table types cannot be statically analyzed.";

    public PostgreSqlDataAdapter()
    {
    }

    public PostgreSqlDataAdapter(NpgsqlCommand selectCommand)
    {
        SelectCommand = selectCommand;
    }

    protected override int Fill(DataTable[] dataTables, IDataReader dataReader, int startRecord, int maxRecords) =>
        base.Fill(dataTables, Read(dataReader), startRecord, maxRecords);

    protected override int Fill(DataSet dataSet, string srcTable, IDataReader dataReader, int startRecord,
        int maxRecords) =>
        base.Fill(dataSet, srcTable, Read(dataReader), startRecord, maxRecords);

    [RequiresUnreferencedCode(SchemaTableTypesCannotBeAnalyzed)]
    protected override DataTable? FillSchema(DataTable dataTable, SchemaType schemaType, IDataReader dataReader) =>
        base.FillSchema(dataTable, schemaType, Read(dataReader));

    [RequiresUnreferencedCode(SchemaTableTypesCannotBeAnalyzed)]
    protected override DataTable[] FillSchema(DataSet dataSet, SchemaType schemaType, string srcTable,
        IDataReader dataReader) =>
        base.FillSchema(dataSet, schemaType, srcTable, Read(dataReader));

    /// <summary>
    ///     Returns <paramref name="dataReader" /> read with the C# types FAnsi gives PostgreSQL columns.  The reader is still
    ///     owned (and disposed) by whoever opened it.
    /// </summary>
    private static PostgreSqlDataReader Read(IDataReader dataReader) =>
        dataReader as PostgreSqlDataReader ?? new PostgreSqlDataReader(dataReader);
}
