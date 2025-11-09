using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using FAnsi.Connections;
using FAnsi.Discovery;
using FAnsi.Discovery.Helpers;
using Microsoft.Data.SqlClient;

namespace FAnsi.Implementations.MicrosoftSQL;

public sealed partial class MicrosoftSQLBulkCopy : BulkCopy
{
    private readonly SqlBulkCopy _bulkCopy;
    private static readonly Regex ColumnLevelComplaint = ColumnLevelComplaintRe();
    private Dictionary<int, ColumnMappingMetadata>? _columnMetadataCache;

    /// <summary>
    /// Metadata for a column mapping, cached to avoid reflection when enhancing error messages.
    /// </summary>
    private sealed class ColumnMappingMetadata
    {
        public required string SourceColumn { get; init; }
        public required string DestinationColumn { get; init; }
        public int? MaxLength { get; init; }
        public string? DataType { get; init; }
    }


    public MicrosoftSQLBulkCopy(DiscoveredTable targetTable, IManagedConnection connection, CultureInfo culture) : base(targetTable, connection,
        culture)
    {
        var options = SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock;
        if (connection.Transaction == null)
            options |= SqlBulkCopyOptions.UseInternalTransaction;
        _bulkCopy = new SqlBulkCopy((SqlConnection)connection.Connection, options, (SqlTransaction?)connection.Transaction)
        {
            BulkCopyTimeout = 50000,
            DestinationTableName = targetTable.GetFullyQualifiedName()
        };

        // Configure DateTimeDecider for SQL Server compatibility
        // SQL Server expects dates in specific formats, including the format from the test
        DateTimeDecider.Settings.ExplicitDateFormats = [
            "MM/dd/yyyy HH:mm:ss",  // 01/01/2007 00:00:00
            "MM/dd/yyyy H:mm:ss",   // 01/01/2007 0:00:00
            "M/dd/yyyy HH:mm:ss",   // 1/01/2007 00:00:00
            "M/d/yyyy HH:mm:ss",    // 1/1/2007 00:00:00
            "MM/dd/yyyy hh:mm:ss tt", // 01/01/2007 12:00:00 AM
            "M/dd/yyyy hh:mm:ss tt",  // 1/01/2007 12:00:00 AM
            "yyyy-MM-dd HH:mm:ss",  // 2007-01-01 00:00:00
            "yyyy-MM-dd H:mm:ss",   // 2007-01-01 0:00:00
            "MM/dd/yyyy",           // 01/01/2007
            "M/dd/yyyy",            // 1/01/2007
            "yyyy-MM-dd",           // 2007-01-01
            "yyyyMMdd",             // 20070101
            "MM/dd/yy",             // 01/01/07
            "M/dd/yy"               // 1/01/07
        ];
    }

    public override int UploadImpl(DataTable dt)
    {
        _bulkCopy.BulkCopyTimeout = Timeout;

        _bulkCopy.ColumnMappings.Clear();

        // Simple column mapping setup (metadata cache built lazily on first error)
        foreach (var (key, value) in GetMapping(dt.Columns.Cast<DataColumn>()))
            _bulkCopy.ColumnMappings.Add(key.ColumnName, value.GetRuntimeName());

        return BulkInsertWithBetterErrorMessages(_bulkCopy, dt, TargetTable.Database.Server);
    }

    /// <summary>
    /// Builds the column metadata cache from current SqlBulkCopy column mappings.
    /// This is called lazily when error enhancement is needed (AOT-compatible approach).
    /// </summary>
    /// <param name="insert">The SqlBulkCopy instance with configured column mappings</param>
    /// <returns>Dictionary mapping colid (1-based) to column metadata</returns>
    private Dictionary<int, ColumnMappingMetadata> BuildColumnMetadataCache(SqlBulkCopy insert)
    {
        // Build mapping dictionary for fast lookup by destination column name
        var mappingByDestColumn = new Dictionary<string, SqlBulkCopyColumnMapping>(StringComparer.OrdinalIgnoreCase);
        foreach (SqlBulkCopyColumnMapping mapping in insert.ColumnMappings)
            mappingByDestColumn[mapping.DestinationColumn] = mapping;

        // Iterate through TargetTableColumns in physical table order
        // SQL Server assigns colid based on table column order (NOT alphabetical)
        var cache = new Dictionary<int, ColumnMappingMetadata>();
        var colid = 1; // 1-based

        foreach (var destColumn in TargetTableColumns)
        {
            var destColumnName = destColumn.GetRuntimeName();

            // Only include columns that are actually mapped
            if (mappingByDestColumn.TryGetValue(destColumnName, out var mapping))
            {
                cache[colid] = new ColumnMappingMetadata
                {
                    SourceColumn = mapping.SourceColumn,
                    DestinationColumn = destColumnName,
                    MaxLength = destColumn.DataType?.GetLengthIfString(),
                    DataType = destColumn.DataType?.SQLType
                };
                colid++;
            }
        }

        return cache;
    }

    private int BulkInsertWithBetterErrorMessages(SqlBulkCopy insert, DataTable dt, DiscoveredServer serverForLineByLineInvestigation)
    {
        var rowsWritten = 0;
        EmptyStringsToNulls(dt);
        InspectDataTableForFloats(dt);

        ConvertStringTypesToHardTypes(dt);

        try
        {
            //send data read to server
            insert.WriteToServer(dt);
            rowsWritten += dt.Rows.Count;

            return rowsWritten;
        }
        catch (Exception e)
        {
            //user does not want to replay the load one line at a time to get more specific error messages
            if (serverForLineByLineInvestigation != null)
            {
                Exception better;
                try
                {
                    //we can attempt line by line insert to find the bad row
                    better = AttemptLineByLineInsert(e, insert, dt, serverForLineByLineInvestigation);
                }
                catch (Exception exception)
                {
                    throw new AggregateException(
                        SR
                            .MicrosoftSQLBulkCopy_BulkInsertWithBetterErrorMessages_Failed_to_bulk_insert_batch__line_by_line_investigation_also_failed___InnerException_0__is_the_original_Exception__InnerException_1__is_the_line_by_line_failure,
                        e, exception);
                }
                throw better;
            }

            if (BcpColIdToString(insert, e as SqlException, out var result1, out _))
                throw new Exception(
                    string.Format(
                        SR.MicrosoftSQLBulkCopy_BulkInsertWithBetterErrorMessages_Failed_to_bulk_insert__0_,
                        result1), e); //but we can still give him a better message than "bcp colid 1 was bad"!

            throw;
        }
    }

    /// <summary>
    /// Creates a new transaction and does one line at a time bulk insertions of the <paramref name="insert"/> to determine which line (and value)
    /// is causing the problem.  Transaction is always rolled back.
    ///
    /// </summary>
    /// <param name="e"></param>
    /// <param name="insert"></param>
    /// <param name="dt"></param>
    /// <param name="serverForLineByLineInvestigation"></param>
    /// <returns></returns>
    private Exception AttemptLineByLineInsert(Exception e, SqlBulkCopy insert, DataTable dt, DiscoveredServer serverForLineByLineInvestigation)
    {
        var line = 1;
        var firstPass = ExceptionToListOfInnerMessages(e, true);
        firstPass = firstPass.Replace(Environment.NewLine, $"{Environment.NewLine}\t");
        firstPass = Environment.NewLine + SR.MicrosoftSQLBulkCopy_AttemptLineByLineInsert_First_Pass_Exception_ + Environment.NewLine + firstPass;

        //have to use a new object because current one could have a broken transaction associated with it
        using var con = (SqlConnection)serverForLineByLineInvestigation.GetConnection();
        con.Open();
        var investigationTransaction = con.BeginTransaction("Investigate BulkCopyFailure");
        using (var investigationOneLineAtATime = new SqlBulkCopy(con, SqlBulkCopyOptions.KeepIdentity, investigationTransaction))
        {
            investigationOneLineAtATime.DestinationTableName = insert.DestinationTableName;

            foreach (SqlBulkCopyColumnMapping m in insert.ColumnMappings)
                investigationOneLineAtATime.ColumnMappings.Add(m);

            //try a line at a time
            foreach (DataRow dr in dt.Rows)
                try
                {
                    investigationOneLineAtATime.WriteToServer(new[] { dr }); //try one line
                    line++;
                }
                catch (Exception exception)
                {
                    if (BcpColIdToString(investigationOneLineAtATime, exception as SqlException, out var result, out var badMapping))
                    {
                        if (badMapping is null || !dt.Columns.Contains(badMapping.SourceColumn))
                            return new Exception(
                                string.Format(
                                    SR
                                        .MicrosoftSQLBulkCopy_AttemptLineByLineInsert_BulkInsert_failed_on_data_row__0___1_,
                                    line, result), e);

                        var sourceValue = dr[badMapping.SourceColumn];
                        // Manual loop optimization to avoid LINQ SingleOrDefault allocation and use span comparisons
                        DiscoveredColumn? destColumn = null;
                        foreach (var column in TargetTableColumns)
                        {
                            if (StringComparisonHelper.ColumnNamesEqual(column.GetRuntimeName(), badMapping.DestinationColumn))
                            {
                                destColumn = column;
                                break;
                            }
                        }

                        if (destColumn != null)
                            return new FileLoadException(
                                string.Format(SR.MicrosoftSQLBulkCopy_AttemptLineByLineInsert_BulkInsert_failed_on_data_row__0__the_complaint_was_about_source_column____1____which_had_value____2____destination_data_type_was____3____4__5_, line, badMapping.SourceColumn, sourceValue, destColumn.DataType, Environment.NewLine, result), exception);

                        return new Exception(string.Format(SR.MicrosoftSQLBulkCopy_AttemptLineByLineInsert_BulkInsert_failed_on_data_row__0___1_, line, result), e);
                    }

                    // For constraint violations that don't have column-specific bcp errors,
                    // provide a clearer message by including the original exception details
                    var originalException = exception;
                    return new FileLoadException(
                        string.Format(SR.MicrosoftSQLBulkCopy_AttemptLineByLineInsert_Second_Pass_Exception__Failed_to_load_data_row__0__the_following_values_were_rejected_by_the_database___1__2__3_, line, Environment.NewLine, string.Join(Environment.NewLine, dr.ItemArray), firstPass),
                        originalException);
                }

            //it worked... how!?
            investigationTransaction.Rollback();
            con.Close();
        }

        return new Exception(SR.MicrosoftSQLBulkCopy_AttemptLineByLineInsert_Second_Pass_Exception__Bulk_insert_failed_but_when_we_tried_to_repeat_it_a_line_at_a_time_it_worked + firstPass, e);
    }

    /// <summary>
    /// Inspects exception message <paramref name="ex"/> for references to bcp client colid and displays the user recognizable name of the column.
    /// Uses cached metadata to avoid reflection (AOT-compatible). Cache is built lazily on first error.
    /// </summary>
    /// <param name="insert"></param>
    /// <param name="ex">The Exception you caught.  If null method returns false and output variables are null.</param>
    /// <param name="newMessage"></param>
    /// <param name="badMapping"></param>
    /// <returns></returns>
    private bool BcpColIdToString(SqlBulkCopy insert, SqlException? ex, out string? newMessage, out SqlBulkCopyColumnMapping? badMapping)
    {
        var match = ColumnLevelComplaint.Match(ex?.Message ?? "");
        if (ex == null || !match.Success)
        {
            newMessage = null;
            badMapping = null;
            return false;
        }

        // Lazy initialization: build cache on first error (instance is not shared across threads)
        _columnMetadataCache ??= BuildColumnMetadataCache(insert);

        // Get colid from error message (1-based)
        var colId = Convert.ToInt32(match.Groups[1].Value);

        // Look up in our cache
        if (!_columnMetadataCache.TryGetValue(colId, out var metadata))
        {
            newMessage = ex.Message;
            badMapping = null;
            return false;
        }

        // Find the actual SqlBulkCopyColumnMapping object
        badMapping = insert.ColumnMappings.Cast<SqlBulkCopyColumnMapping>()
            .FirstOrDefault(m => string.Equals(m.DestinationColumn, metadata.DestinationColumn,
                StringComparison.OrdinalIgnoreCase));

        // Build enhanced error message (matching original format)
        newMessage = ex.Message.Insert(match.Index + match.Length,
            $"(colid {colId}: Source Column <<{metadata.SourceColumn}>> Dest Column <<{metadata.DestinationColumn}>> which has MaxLength of {metadata.MaxLength?.ToString() ?? "unknown"})");

        return true;
    }

    private static void EmptyStringsToNulls(DataTable dt)
    {
        foreach (var col in dt.Columns.Cast<DataColumn>().Where(static c => c.DataType == typeof(string)))
            foreach (var row in dt.Rows.Cast<DataRow>()
                         .Select(row => new { row, o = row[col] })
                         .Where(static t => t.o != DBNull.Value && t.o != null && string.IsNullOrWhiteSpace(t.o.ToString()))
                         .Select(static t => t.row))
                row[col] = DBNull.Value;
    }

    [Pure]
    private static string ExceptionToListOfInnerMessages(Exception e, bool includeStackTrace = false)
    {
        var message = new StringBuilder(e.Message);
        if (includeStackTrace)
        {
            message.AppendLine();
            message.Append(e.StackTrace);
        }

        if (e is ReflectionTypeLoadException reflectionTypeLoadException)
            foreach (var loaderException in reflectionTypeLoadException.LoaderExceptions.OfType<Exception>())
            {
                message.AppendLine();
                message.Append(ExceptionToListOfInnerMessages(loaderException, includeStackTrace));
            }

        if (e.InnerException == null) return message.ToString();

        message.AppendLine();
        message.Append(ExceptionToListOfInnerMessages(e.InnerException, includeStackTrace));
        return message.ToString();
    }

    private static void InspectDataTableForFloats(DataTable dt)
    {
        //are there any float or float? columns
        var floatColumnNames = dt.Columns.Cast<DataColumn>().Where(static c => c.DataType == typeof(float) || c.DataType == typeof(float?)).Select(static c => c.ColumnName).ToArray();
        if (floatColumnNames.Length != 0)
            throw new NotSupportedException(
                $"Found float column(s) in data table, SQLServer does not support floats in bulk insert, instead you should use doubles otherwise you will end up with the value 0.85 turning into :0.850000023841858 in your database.  Float column(s) were:{string.Join(",", floatColumnNames)}");

        //are there any object columns
        var objectColumns = dt.Columns.Cast<DataColumn>().Where(static c => c.DataType == typeof(object)).Select(static col => col.Ordinal).ToArray();

        //do any of the object columns have floats or float? in them?
        for (var i = 0; i < Math.Min(100, dt.Rows.Count); i++)
        {
            var bad = objectColumns.Select(c => dt.Rows[i][c])
                .FirstOrDefault(static t => t is float);
            if (bad != null)
                throw new NotSupportedException(
                $"Found float value {bad} in data table, SQLServer does not support floats in bulk insert, instead you should use doubles otherwise you will end up with the value 0.85 turning into :0.850000023841858 in your database");
        }
    }


    [GeneratedRegex("bcp client for colid (\\d+)", RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex ColumnLevelComplaintRe();
}
