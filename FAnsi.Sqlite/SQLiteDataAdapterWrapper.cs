using System;
using System.Data;
using System.Data.Common;
using Microsoft.Data.Sqlite;

namespace FAnsi.Implementations.Sqlite;

/// <summary>
/// SQLite-compatible implementation of DbDataAdapter for filling DataTables.
/// </summary>
/// <remarks>
/// SQLite doesn't have a built-in DataAdapter, so this implementation
/// manually executes commands and fills DataTables.
/// </remarks>
internal sealed class SQLiteDataAdapterWrapper : DbDataAdapter
{
    private readonly SqliteCommand _selectCommand;

    /// <summary>
    /// Initializes a new instance of the SQLiteDataAdapterWrapper.
    /// </summary>
    /// <param name="selectCommand">The SELECT command to execute</param>
    public SQLiteDataAdapterWrapper(SqliteCommand selectCommand)
    {
        _selectCommand = selectCommand ?? throw new ArgumentNullException(nameof(selectCommand));
        SelectCommand = _selectCommand;
    }

    /// <summary>
    /// Fills a DataTable with the results of the SELECT command.
    /// </summary>
    /// <param name="dataTable">The DataTable to fill with data</param>
    /// <returns>The number of rows successfully added to or refreshed in the DataTable</returns>
    public new int Fill(DataTable dataTable)
    {
        if (dataTable == null)
            throw new ArgumentNullException(nameof(dataTable));

        dataTable.Clear();

        using var reader = _selectCommand.ExecuteReader();

        // Get schema information
        var schemaTable = reader.GetSchemaTable();
        if (schemaTable != null)
        {
            foreach (DataRow row in schemaTable.Rows)
            {
                var columnName = row["ColumnName"].ToString();
                var dataType = (Type)row["DataType"];
                var column = new DataColumn(columnName, dataType);
                dataTable.Columns.Add(column);
            }
        }

        // Fill data
        while (reader.Read())
        {
            var dataRow = dataTable.NewRow();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (reader.IsDBNull(i))
                {
                    dataRow[i] = DBNull.Value;
                }
                else
                {
                    var readerValue = reader.GetValue(i);
                    var dataColumn = dataTable.Columns[i];

                    // Convert string value to the expected column type if needed
                    if (dataColumn != null && dataColumn.DataType != typeof(string) && readerValue is string stringValue)
                    {
                        dataRow[i] = SqliteTableHelper.ConvertStringToTypedValue(stringValue, dataColumn.DataType);
                    }
                    else
                    {
                        dataRow[i] = readerValue;
                    }
                }
            }
            dataTable.Rows.Add(dataRow);
        }

        return dataTable.Rows.Count;
    }
}
