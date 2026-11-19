using System.Data;
using System.Data.Common;
using System.Globalization;
using FAnsi.Connections;
using FAnsi.Discovery;
using Oracle.ManagedDataAccess.Client;

namespace FAnsi.Implementations.Oracle;

/// <summary>
///     Fills a <see cref="DataTable" /> from Oracle as <see cref="OracleDataAdapter" /> does, except that the
///     NUMBER(p,s) columns that ODP.NET reads as float or double are read as decimals.
/// </summary>
internal static class OracleDataTableFiller
{
    /// <summary>
    ///     The most digits that a decimal holds after the decimal point
    /// </summary>
    private const int MaxDecimalScale = 28;

    /// <summary>
    ///     Fills <paramref name="dt" /> with the first <paramref name="topX" /> rows of <paramref name="table" />, as
    ///     <see cref="DatabaseOperationArgs.Fill" /> would with an <see cref="OracleDataAdapter" />, except that the
    ///     columns for which <see cref="ReadAsDecimal" /> is true are read as decimals.
    /// </summary>
    public static void FillWithTopX(DatabaseOperationArgs args, DiscoveredTable table, int topX, DataTable dt)
    {
        using var con = args.GetManagedConnection(table);
        ((OracleConnection)con.Connection).PurgeStatementCache();

        var cols = table.DiscoverColumns();

        // The columns are listed rather than selected with *, which also makes column i of the reader cols[i]
        var sql =
            $"SELECT {string.Join(",", cols.Select(static c => c.GetFullyQualifiedName()).ToArray())} FROM {table.GetFullyQualifiedName()} OFFSET 0 ROWS FETCH NEXT {topX} ROWS ONLY";

        using var cmd = (OracleCommand)table.Database.Server.GetCommand(sql, con);
        cmd.CommandTimeout = args.TimeoutInSeconds;
        // Fetches CLOB and BLOB values with their rows, as OracleDataAdapter.Fill does, rather than one at a time
        cmd.InitialLOBFetchSize = -1;
        args.CancellationToken.ThrowIfCancellationRequested();

        using (var reader = cmd.ExecuteReader())
            Fill(reader, dt, (r, i) => ReadAsDecimal(cols[i], r.GetFieldType(i)), args.CancellationToken);

        args.CancellationToken.ThrowIfCancellationRequested();
    }

    /// <summary>
    ///     Adds the rows of <paramref name="reader" /> to <paramref name="dt" /> as <see cref="DbDataAdapter" /> does,
    ///     except that the columns for which <paramref name="readAsDecimal" /> returns true are read with
    ///     <see cref="DbDataReader.GetDecimal" /> and added to <paramref name="dt" /> as decimal columns. This works
    ///     with any <see cref="DbDataReader" />.
    /// </summary>
    /// <remarks>
    ///     As with <see cref="DbDataAdapter" />, each column of the reader goes into the column of
    ///     <paramref name="dt" /> with its name, or else the only one with its name ignoring case, and is added to
    ///     <paramref name="dt" /> with the field type of the reader if there is neither. Names are numbered as
    ///     <see cref="GetColumnNames" /> says, null values are <see cref="DBNull" />, and the rows are added unchanged.
    /// </remarks>
    /// <param name="reader">The reader, before its first row</param>
    /// <param name="dt">The table to add the rows to</param>
    /// <param name="readAsDecimal">Whether to read the column of the reader with the ordinal given as a decimal</param>
    /// <param name="cancellationToken">Checked for each row</param>
    /// <returns>The number of rows added</returns>
    public static int Fill(DbDataReader reader, DataTable dt, Func<DbDataReader, int, bool> readAsDecimal,
        CancellationToken cancellationToken)
    {
        var names = GetColumnNames(reader);
        var asDecimal = new bool[names.Length];
        var ordinals = new int[names.Length];

        for (var i = 0; i < names.Length; i++)
        {
            asDecimal[i] = readAsDecimal(reader, i);

            var ordinal = dt.Columns.IndexOf(names[i]);
            ordinals[i] = ordinal >= 0
                ? ordinal
                : dt.Columns.Add(names[i], asDecimal[i] ? typeof(decimal) : reader.GetFieldType(i)).Ordinal;
        }

        var values = new object?[dt.Columns.Count];
        var rows = 0;

        dt.BeginLoadData();
        try
        {
            while (reader.Read())
            {
                cancellationToken.ThrowIfCancellationRequested();

                for (var i = 0; i < names.Length; i++)
                    values[ordinals[i]] =
                        asDecimal[i] && !reader.IsDBNull(i) ? reader.GetDecimal(i) : reader.GetValue(i);

                // Accepting the changes leaves the row unchanged, as DbDataAdapter.AcceptChangesDuringFill does
                dt.LoadDataRow(values, true);
                rows++;
            }
        }
        finally
        {
            dt.EndLoadData();
        }

        return rows;
    }

    /// <summary>
    ///     Returns whether to read <paramref name="column" />, which ODP.NET reads as <paramref name="fieldType" />,
    ///     with GetDecimal: true for a NUMBER(p,s) column that ODP.NET reads as a float or double.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ODP.NET reads a NUMBER(p,s) column as the first of short, int, long, float, double and decimal that
    ///         holds every value of the column, so NUMBER(5,2) as a float and NUMBER(10,2) as a double. .NET 11
    ///         converts a float or double to decimal exactly, so the 9.99 of a NUMBER(5,2) column became
    ///         9.9899997711181640625 in the decimal DataColumn that
    ///         <see cref="DiscoveredTable.GetDataTable(int, bool, IManagedTransaction)" /> gives it. GetDecimal reads
    ///         the number that the column holds.
    ///     </para>
    ///     <para>
    ///         The columns read this way are those discovered as decimal(p,s) with a scale of 28 or less, all of whose
    ///         values fit in a decimal, since ODP.NET only reads columns of fewer than 16 digits as float or double.
    ///         FLOAT(b) columns, which are discovered as double, can hold numbers too large or too small for a
    ///         decimal, and ODP.NET's GetDecimal throws InvalidCastException for BINARY_FLOAT and BINARY_DOUBLE
    ///         columns, so these are read as ODP.NET reads them.
    ///     </para>
    /// </remarks>
    private static bool ReadAsDecimal(DiscoveredColumn column, Type fieldType) =>
        (fieldType == typeof(float) || fieldType == typeof(double)) &&
        column.DataType?.GetDecimalSize() is { Scale: <= MaxDecimalScale };

    /// <summary>
    ///     Returns the names of the columns of <paramref name="reader" />, numbered as <see cref="DbDataAdapter" />
    ///     numbers them: a name that repeats an earlier one, ignoring case, gets the first number from 1 that makes it
    ///     unique, so Name, NAME and name become Name, NAME1 and name2.
    /// </summary>
    private static string[] GetColumnNames(DbDataReader reader)
    {
        var names = new string[reader.FieldCount];
        for (var i = 0; i < names.Length; i++)
            names[i] = reader.GetName(i);

        var taken = new HashSet<string>(names, StringComparer.OrdinalIgnoreCase);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < names.Length; i++)
        {
            // An empty name is left for the DataTable to make up, as Column1, Column2 and so on
            if (names[i].Length == 0 || seen.Add(names[i]))
                continue;

            string numbered;
            var number = 0;
            do
            {
                number++;
                numbered = names[i] + number.ToString(CultureInfo.InvariantCulture);
            } while (!taken.Add(numbered));

            names[i] = numbered;
        }

        return names;
    }
}
