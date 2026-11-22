using System.Data;
using System.Data.Common;
using System.Globalization;

namespace FAnsi.Implementations.Sqlite;

/// <summary>
///     Fills a <see cref="DataTable" /> from a SQLite reader, converting each value to the type of the column it goes
///     into.  SQLite reports each value as the storage class it is stored as, so the dates and times in DATETIME and TIME
///     columns are read as strings, booleans as integers and the values of decimal columns as REALs.
/// </summary>
internal static class SqliteDataTableFiller
{
    /// <summary>
    ///     Adds the rows of <paramref name="reader" /> to <paramref name="dt" />, converting the values of the columns
    ///     that <paramref name="dt" /> has to their types (see <see cref="ConvertToColumnType" />).  If
    ///     <paramref name="dt" /> has no columns, a column is added for each field of the reader first, with the field
    ///     type that the reader reports.  The rows are added without reflection, which keeps this AOT-compatible.
    /// </summary>
    /// <param name="reader">The reader to read the rows from, positioned before the first row</param>
    /// <param name="dt">The table to add the rows to</param>
    public static void Fill(DbDataReader reader, DataTable dt)
    {
        if (dt.Columns.Count == 0)
            for (var i = 0; i < reader.FieldCount; i++)
                dt.Columns.Add(reader.GetName(i), reader.GetFieldType(i));

        while (reader.Read())
        {
            var row = dt.NewRow();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var value = reader.GetValue(i);

                if (i < dt.Columns.Count)
                    value = ConvertToColumnType(value, dt.Columns[i].DataType);

                row[i] = value;
            }

            dt.Rows.Add(row);
        }
    }

    /// <summary>
    ///     Converts a value read from SQLite to the type of the <see cref="DataColumn" /> it is going into.
    /// </summary>
    /// <param name="value">The value read from the database</param>
    /// <param name="columnType">The <see cref="DataColumn.DataType" /> of the column the value is going into</param>
    /// <returns>The value converted to <paramref name="columnType" /> if it needed converting and could be</returns>
    /// <exception cref="OverflowException">A REAL in a decimal column is outside the range of decimal</exception>
    private static object ConvertToColumnType(object value, Type columnType)
    {
        switch (value)
        {
            case string s when columnType == typeof(DateTime):
                return DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateTime)
                    ? dateTime
                    : value;
            case string s when columnType == typeof(TimeSpan):
                return TimeSpan.TryParse(s, CultureInfo.InvariantCulture, out var timeSpan) ? timeSpan : value;
            case long l when columnType == typeof(bool):
                return l != 0;
            case double real when columnType == typeof(decimal) && double.IsFinite(real):
                return RealToDecimal(real);
            default:
                return value;
        }
    }

    /// <summary>
    ///     Converts the REAL value of a decimal column to a decimal rounded to 15 significant digits, as
    ///     Convert.ToDecimal(double) did before .NET 11, so that the REAL 9.99 is read as 9.99.
    /// </summary>
    /// <remarks>
    ///     .NET 11 converts a double to decimal exactly, which reads the REAL 9.99 as 9.990000000000000213162820728: more
    ///     decimal places than the column allows, and not equal to the value that was stored.
    /// </remarks>
    /// <param name="real">A finite REAL value</param>
    /// <exception cref="OverflowException">The value is outside the range of decimal</exception>
    private static decimal RealToDecimal(double real) =>
        decimal.Parse(real.ToString("G15", CultureInfo.InvariantCulture), NumberStyles.Float,
            CultureInfo.InvariantCulture);
}
