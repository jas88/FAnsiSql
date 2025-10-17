using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using FAnsi.Connections;
using FAnsi.Discovery;
using Microsoft.Data.Sqlite;

namespace FAnsi.Implementations.Sqlite;

/// <summary>
/// Inserts rows into SQLite table using batch INSERT commands with parameters for better performance and safety.
/// </summary>
public sealed class SqliteBulkCopy(DiscoveredTable targetTable, IManagedConnection connection, CultureInfo culture)
    : BulkCopy(targetTable, connection, culture)
{
    public static int BulkInsertBatchTimeoutInSeconds { get; set; } = 30;
    public static int BatchSize { get; set; } = 1000; // SQLite can handle larger batches than some databases

    public override int UploadImpl(DataTable dt)
    {
        var ourTrans = Connection.Transaction == null ? Connection.Connection.BeginTransaction(IsolationLevel.ReadUncommitted) : null;
        var matchedColumns = GetMapping(dt.Columns.Cast<DataColumn>());
        var affected = 0;

        using var cmd = new SqliteCommand("", (SqliteConnection)Connection.Connection);
        cmd.Transaction = (SqliteTransaction?)(Connection.Transaction ?? ourTrans);
        
        if (BulkInsertBatchTimeoutInSeconds != 0)
            cmd.CommandTimeout = BulkInsertBatchTimeoutInSeconds;

        // Build the parameterized INSERT statement
        var columnNames = string.Join(",", matchedColumns.Values.Select(c => $"[{c.GetRuntimeName()}]"));
        var paramNames = string.Join(",", matchedColumns.Keys.Select((_, i) => $"@p{i}"));
        var baseCommand = $"INSERT INTO {TargetTable.GetFullyQualifiedName()}({columnNames}) VALUES ";

        var batchRows = 0;
        var valueClauses = new StringBuilder();
        var parameterIndex = 0;

        foreach (DataRow dr in dt.Rows)
        {
            if (batchRows > 0)
                valueClauses.Append(',');

            valueClauses.Append('(');
            var columnIndex = 0;
            foreach (var kvp in matchedColumns)
            {
                if (columnIndex > 0)
                    valueClauses.Append(',');

                var paramName = $"@p{parameterIndex}";
                valueClauses.Append(paramName);

                var parameter = new SqliteParameter(paramName, ConvertValueForSQLite(dr[kvp.Key.Ordinal]));
                cmd.Parameters.Add(parameter);

                parameterIndex++;
                columnIndex++;
            }
            valueClauses.Append(')');

            batchRows++;

            // Execute batch when we reach batch size or it's the last row
            if (batchRows >= BatchSize || dr == dt.Rows.Cast<DataRow>().Last())
            {
                cmd.CommandText = baseCommand + valueClauses.ToString();
                affected += cmd.ExecuteNonQuery();

                // Reset for next batch
                cmd.Parameters.Clear();
                valueClauses.Clear();
                batchRows = 0;
                parameterIndex = 0;
            }
        }

        ourTrans?.Commit();
        return affected;
    }

    private static object ConvertValueForSQLite(object value)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // SQLite handles most types natively, but we'll handle some special cases
        return value switch
        {
            DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss.fff"),
            DateOnly dateOnly => dateOnly.ToString("yyyy-MM-dd"),
            TimeOnly timeOnly => timeOnly.ToString("HH:mm:ss.fff"),
            bool b => b ? 1 : 0,  // SQLite stores booleans as integers
            Guid g => g.ToString(),  // Store GUIDs as text
            _ => value
        };
    }

    public override int Upload(DataTable dt)
    {
        if (dt.Rows.Count == 0)
            return 0;

        return UploadImpl(dt);
    }

    // BulkCopy base class handles disposal
}