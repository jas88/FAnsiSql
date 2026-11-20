using System.Data;
using System.Globalization;
using System.Numerics;
using System.Text;
using FAnsi.Connections;
using FAnsi.Discovery.Helpers;
using TypeGuesser;
using TypeGuesser.Deciders;

namespace FAnsi.Discovery;

/// <inheritdoc />
public abstract class BulkCopy : IBulkCopy
{
    // Cached CompositeFormat for CA1863
    private static readonly CompositeFormat ColumnNotInDestinationFormat =
        CompositeFormat.Parse(FAnsiStrings.BulkCopy_ColumnNotInDestinationTable);

    /// <summary>
    ///     The database connection on which the bulk insert operation is underway
    /// </summary>
    protected readonly IManagedConnection Connection;

    /// <summary>
    ///     The target table on the database server to which records are being uploaded
    /// </summary>
    protected readonly DiscoveredTable TargetTable;

    private Lazy<DiscoveredColumn[]> _targetTableColumns;

    /// <summary>
    ///     Begins a new bulk copy operation in which one or more data tables are uploaded to the
    ///     <paramref name="targetTable" />.  The API entrypoint for this is
    ///     <see cref="DiscoveredTable.BeginBulkInsert(IManagedTransaction)" />.
    /// </summary>
    /// <param name="targetTable"></param>
    /// <param name="connection"></param>
    /// <param name="culture">For parsing string date expressions etc</param>
    protected BulkCopy(DiscoveredTable targetTable, IManagedConnection connection, CultureInfo culture)
    {
        Culture = culture;
        TargetTable = targetTable;
        Connection = connection;
        _targetTableColumns = new Lazy<DiscoveredColumn[]>(
            () => TargetTable.DiscoverColumns(Connection.ManagedTransaction),
            LazyThreadSafetyMode.ExecutionAndPublication);
        AllowUnmatchedInputColumns = false;
        DateTimeDecider = new DateTimeTypeDecider(culture);
    }

    public CultureInfo Culture { get; }

    /// <summary>
    ///     The cached columns found on the <see cref="TargetTable" />.  If you alter the table midway through a bulk insert
    ///     you must
    ///     call <see cref="InvalidateTableSchema" /> to refresh this.
    /// </summary>
    protected DiscoveredColumn[] TargetTableColumns => _targetTableColumns.Value;

    /// <summary>
    ///     When calling GetMapping if there are DataColumns in the input table that you are trying to bulk insert that are not
    ///     matched
    ///     in the destination table then the default behaviour is to throw a KeyNotFoundException.  Set this to false to
    ///     ignore that
    ///     behaviour.  This will result in loosing data from your DataTable.
    ///     <para>Defaults to false</para>
    /// </summary>
    public bool AllowUnmatchedInputColumns { get; }

    /// <inheritdoc />
    public DateTimeTypeDecider DateTimeDecider { get; protected set; }

    /// <inheritdoc />
    public virtual int Timeout { get; set; }

    /// <summary>
    ///     Updates <see cref="TargetTableColumns" />.  Call if you are making modifications to the <see cref="TargetTable" />
    ///     midway through a bulk insert.
    /// </summary>
    public void InvalidateTableSchema()
    {
        _targetTableColumns = new Lazy<DiscoveredColumn[]>(
            () => TargetTable.DiscoverColumns(Connection.ManagedTransaction),
            LazyThreadSafetyMode.ExecutionAndPublication);
    }

    /// <summary>
    ///     Closes the connection and completes the bulk insert operation (including committing the transaction).  If this
    ///     method is not called
    ///     then the records may not be committed.
    /// </summary>
    public virtual void Dispose()
    {
        GC.SuppressFinalize(this);
        Connection.Dispose();
    }

    /// <inheritdoc />
    public virtual int Upload(DataTable dt)
    {
        TargetTable.Database.Helper.ThrowIfObjectColumns(dt);

        ConvertStringTypesToHardTypes(dt);

        return UploadImpl(dt);
    }

    public abstract int UploadImpl(DataTable dt);

    /// <summary>
    ///     Replaces all string representations for data types that can be problematic/ambiguous (e.g. DateTime or TimeSpan)
    ///     into hard typed objects using appropriate decider e.g. <see cref="DateTimeDecider" />.
    /// </summary>
    /// <param name="dt"></param>
    protected void ConvertStringTypesToHardTypes(DataTable dt)
    {
        var dict = GetMapping(dt.Columns.Cast<DataColumn>(), out _);

        var factory = new TypeDeciderFactory(Culture);

        //These are the problematic Types
        var deciders = factory.Dictionary;

        //for each column in the destination
        foreach (var (dataColumn, discoveredColumn) in dict)
        {
            //if the destination column is a problematic type
            var dataType = discoveredColumn.DataType?.GetCSharpDataType();
            if (dataType == null || !deciders.TryGetValue(dataType, out var decider)) continue;
            //if it's already not a string then that's fine (hopefully it's a legit Type e.g. DateTime!)
            if (dataColumn.DataType != typeof(string))
                continue;

            //create a new column hard typed to DateTime
            var newColumn = dt.Columns.Add($"{dataColumn.ColumnName}_{Guid.NewGuid()}", dataType);

            //if it's a DateTime decider then guess DateTime culture based on values in the table
            if (decider is DateTimeTypeDecider)
            {
                //also use this one in case the user has set up explicit stuff on it e.g. Culture/Settings
                decider = DateTimeDecider;
                DateTimeDecider.GuessDateFormat(dt.Rows.Cast<DataRow>().Take(500).Select(r => r[dataColumn] as string)
                    .OfType<string>());
            }


            foreach (DataRow dr in dt.Rows)
                try
                {
                    //parse the value
                    dr[newColumn] = dr[dataColumn] is string v ? decider.Parse(v) ?? DBNull.Value : DBNull.Value;
                }
                catch (Exception ex) when (ex is FormatException or InvalidCastException)
                {
                    throw new FormatException($"Failed to parse value '{dr[dataColumn]}' in column '{dataColumn}'", ex);
                }

            //if the DataColumn is part of the Primary Key of the DataTable (in memory)
            //then we need to update the primary key to include the new column not the old one
            if (dt.PrimaryKey != null && dt.PrimaryKey.Contains(dataColumn))
                dt.PrimaryKey = dt.PrimaryKey.Except(new[] { dataColumn }).Union(new[] { newColumn }).ToArray();

            var oldOrdinal = dataColumn.Ordinal;

            //drop the original column
            dt.Columns.Remove(dataColumn);

            //rename the hard typed column to match the old column name
            newColumn.ColumnName = dataColumn.ColumnName;
            if (oldOrdinal != -1)
                newColumn.SetOrdinal(oldOrdinal);
        }
    }

    /// <summary>
    ///     Returns a case insensitive mapping between columns in your DataTable that you are trying to upload and the columns
    ///     that actually exist in the destination
    ///     table.
    ///     <para>
    ///         This overload gives you a list of all unmatched destination columns, these should be given null/default
    ///         automatically by your database API
    ///     </para>
    ///     <para>
    ///         Throws
    ///         <exception cref="KeyNotFoundException">
    ///             if there are unmatched input columns unless
    ///             <see cref="AllowUnmatchedInputColumns" /> is true.
    ///         </exception>
    ///     </para>
    /// </summary>
    /// <param name="inputColumns"></param>
    /// <param name="unmatchedColumnsInDestination"></param>
    /// <returns></returns>
    protected Dictionary<DataColumn, DiscoveredColumn> GetMapping(IEnumerable<DataColumn> inputColumns,
        out DiscoveredColumn[] unmatchedColumnsInDestination)
    {
        var mapping = new Dictionary<DataColumn, DiscoveredColumn>();

        foreach (var colInSource in inputColumns)
        {
            DiscoveredColumn? match = null;

            // Manual loop optimization to avoid LINQ allocations and use span comparisons
            // CodeQL[cs/linq/missed-where-opportunity]: Intentional - manual loop for performance (avoids LINQ allocations)
            foreach (var targetColumn in TargetTableColumns)
                if (StringComparisonHelper.RuntimeNamesEqual(targetColumn.GetRuntimeName(), colInSource.ColumnName))
                {
                    match = targetColumn;
                    break;
                }

            if (match == null)
            {
                if (!AllowUnmatchedInputColumns)
                    throw new ColumnMappingException(string.Format(CultureInfo.InvariantCulture,
                        ColumnNotInDestinationFormat, colInSource.ColumnName, TargetTable));

                //user is ignoring the fact there are unmatched items in DataTable!
            }
            else
            {
                mapping.Add(colInSource, match);
            }
        }

        //unmatched columns in the destination is fine, these usually get populated with the default column values or nulls
        unmatchedColumnsInDestination = TargetTableColumns.Except(mapping.Values).ToArray();

        return mapping;
    }

    /// <summary>
    ///     Returns a case insensitive mapping between columns in your DataTable that you are trying to upload and the columns
    ///     that actually exist in the destination
    ///     table.
    ///     <para>
    ///         Throws
    ///         <exception cref="KeyNotFoundException">
    ///             if there are unmatched input columns unless
    ///             <see cref="AllowUnmatchedInputColumns" /> is true.
    ///         </exception>
    ///     </para>
    /// </summary>
    /// <param name="inputColumns"></param>
    /// <returns></returns>
    protected Dictionary<DataColumn, DiscoveredColumn> GetMapping(IEnumerable<DataColumn> inputColumns) =>
        GetMapping(inputColumns, out _);

    /// <summary>
    ///     Pre-processes and validates all data in a single pass through the DataTable.
    ///     Performs: empty string to NULL conversion, string length validation, decimal validation,
    ///     integer range validation, and NOT NULL constraint validation.
    /// </summary>
    /// <param name="dt">The DataTable to validate</param>
    /// <param name="mapping">Column mapping from source to destination</param>
    /// <param name="validateNotNull">Whether to validate NOT NULL constraints (default true)</param>
    /// <param name="validateDecimalPrecision">
    ///     Whether to validate decimal precision/scale (default true). Set to false for
    ///     databases with dynamic typing like SQLite.
    /// </param>
    protected void PreProcessAndValidate(DataTable dt, Dictionary<DataColumn, DiscoveredColumn> mapping,
        bool validateNotNull = true, bool validateDecimalPrecision = true)
    {
        // Pre-compute validation rules for each column (once per table, not per row)
        var rules = new ColumnValidationRule[mapping.Count];
        var ruleIndex = 0;

        foreach (var (dataColumn, discoveredColumn) in mapping)
        {
            var isString = dataColumn.DataType == typeof(string);
            var isDecimal = validateDecimalPrecision &&
                            (dataColumn.DataType == typeof(decimal) || dataColumn.DataType == typeof(decimal?));
            var isFloatingPoint = validateDecimalPrecision &&
                                  (dataColumn.DataType == typeof(double) || dataColumn.DataType == typeof(float));
            var isInteger = IsIntegerType(dataColumn.DataType);

            var maxStringLength = 0;
            int decimalPrecision = 0, decimalScale = 0;
            BigInteger intMin = long.MinValue, intMax = long.MaxValue;
            var hasIntRange = false;

            if (isString)
            {
                var len = discoveredColumn.DataType?.GetLengthIfString();
                if (len.HasValue && len.Value > 0)
                    maxStringLength = len.Value;
            }

            if (isDecimal || isFloatingPoint)
                (decimalPrecision, decimalScale) = GetDecimalPrecisionAndScale(discoveredColumn);

            if (isInteger)
            {
                var sqlType = discoveredColumn.DataType?.SQLType?.ToUpperInvariant();
                if (!string.IsNullOrEmpty(sqlType))
                {
                    (intMin, intMax) = GetIntegerRange(sqlType);
                    hasIntRange = intMin != long.MinValue || intMax != long.MaxValue;
                }
            }

            rules[ruleIndex++] = new ColumnValidationRule(
                dataColumn, discoveredColumn, dataColumn.Ordinal,
                isString, maxStringLength,
                isDecimal, decimalPrecision, decimalScale,
                hasIntRange, intMin, intMax,
                validateNotNull && !discoveredColumn.AllowNulls);
        }

        // Allow subclasses to perform additional pre-validation checks (e.g., float column rejection)
        OnBeforeRowValidation(dt, mapping);

        // Single pass through all rows using pre-computed rules
        var rowCount = dt.Rows.Count;
        for (var rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            var row = dt.Rows[rowIndex];

            for (var i = 0; i < rules.Length; i++)
            {
                var rule = rules[i];
                var value = row[rule.Ordinal];
                var isNull = value == DBNull.Value || value == null;

                // String: convert empty to NULL, validate length
                if (rule.IsString && value is string s)
                {
                    if (string.IsNullOrWhiteSpace(s))
                    {
                        row[rule.Ordinal] = DBNull.Value;
                        isNull = true;
                    }
                    else if (rule.MaxStringLength > 0 && s.Length > rule.MaxStringLength)
                    {
                        throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture,
                            "Bulk insert failed on data row {0}: source column <<{1}>> has value <<{2}>> (length {3}) which exceeds maximum length {4} for destination column <<{5}>>.",
                            rowIndex + 1, rule.SourceName, s, s.Length, rule.MaxStringLength, rule.DestName));
                    }
                }

                // Decimal validation, if the precision and scale of the column are known
                if (!isNull && rule.DecimalPrecision > 0)
                    switch (value)
                    {
                        case decimal number:
                            ValidateDecimal(number, rule, rowIndex);
                            break;
                        case double or float:
                            ValidateFloatingPoint(value, rule, rowIndex);
                            break;
                    }

                // Integer range validation using BigInteger for full ulong support
                if (rule.HasIntegerRange && !isNull)
                {
                    var v = value switch
                    {
                        ulong ulVal => new BigInteger(ulVal),
                        long lVal => new BigInteger(lVal),
                        uint uiVal => new BigInteger(uiVal),
                        int iVal => new BigInteger(iVal),
                        ushort usVal => new BigInteger(usVal),
                        short sVal => new BigInteger(sVal),
                        byte bVal => new BigInteger(bVal),
                        sbyte sbVal => new BigInteger(sbVal),
                        _ => BigInteger.Parse(Convert.ToString(value, CultureInfo.InvariantCulture) ?? "0",
                            CultureInfo.InvariantCulture)
                    };
                    if (v < rule.IntegerMin || v > rule.IntegerMax)
                        throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture,
                            "Value {0} in column '{1}' (row {2}) is out of range for column '{3}' of type '{4}'.",
                            value, rule.SourceName, rowIndex + 1, rule.DestName, rule.SqlType));
                }

                // NOT NULL validation (after empty string conversion)
                if (rule.RequiresNotNull && isNull)
                    throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture,
                        "NULL value in column '{0}' (row {1}) violates NOT NULL constraint on column '{2}'.",
                        rule.SourceName, rowIndex + 1, rule.DestName));
            }
        }
    }

    /// <summary>
    ///     Checks a decimal against the limit of its column, then its decimal places against the scale of the column.
    /// </summary>
    private static void ValidateDecimal(decimal value, in ColumnValidationRule rule, int rowIndex)
    {
        var d = Math.Abs(value);
        if (rule.DecimalLimit is { } limit && d >= limit)
            throw ExceedsMaximum(value, rule, rowIndex);

        var vs = d.ToString(CultureInfo.InvariantCulture);
        if (!vs.Contains('.', StringComparison.Ordinal))
            return;

        var places = vs.Split('.')[1].TrimEnd('0').Length;
        if (places > rule.DecimalScale)
            throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture,
                "Value {0} in column '{1}' (row {2}) has {3} decimal places, but column is defined as decimal({4},{5}) which allows only {5} decimal places.",
                value, rule.SourceName, rowIndex + 1, places, rule.DecimalPrecision, rule.DecimalScale));
    }

    /// <summary>
    ///     Checks a float or double going into a decimal column against 10^(p-s) exactly, without converting it to a
    ///     decimal, which cannot hold one of 2^96 (about 7.9e28) or more. Unlike a decimal, a float or double can be
    ///     too large even for a column with 29 or more digits before the decimal point (see
    ///     <see cref="DecimalColumnRange.IsTooLarge(double, int, int)" />). Its decimal places are not checked: the
    ///     database rounds them.
    /// </summary>
    private static void ValidateFloatingPoint(object value, in ColumnValidationRule rule, int rowIndex)
    {
        var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
        if (double.IsNaN(number))
            throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture,
                "Value {0} in column '{1}' (row {2}) is not a number, so it cannot be inserted into decimal({3},{4}).",
                value, rule.SourceName, rowIndex + 1, rule.DecimalPrecision, rule.DecimalScale));

        // Infinity exceeds the largest value of every decimal column
        var tooLarge = double.IsInfinity(number) || (value is float single
            ? DecimalColumnRange.IsTooLarge(single, rule.DecimalPrecision, rule.DecimalScale)
            : DecimalColumnRange.IsTooLarge(number, rule.DecimalPrecision, rule.DecimalScale));
        if (tooLarge)
            throw ExceedsMaximum(value, rule, rowIndex);
    }

    private static InvalidOperationException ExceedsMaximum(object value, in ColumnValidationRule rule, int rowIndex) =>
        new(string.Format(CultureInfo.InvariantCulture,
            "Value {0} in column '{1}' (row {2}) exceeds the maximum allowed for decimal({3},{4}). Maximum value is {5}.",
            value, rule.SourceName, rowIndex + 1, rule.DecimalPrecision, rule.DecimalScale,
            rule.MaxDecimalValueText));

    /// <summary>
    ///     Called before row-by-row validation begins. Override in subclasses to perform
    ///     database-specific checks (e.g., SQL Server's float column rejection).
    /// </summary>
    /// <param name="dt">The DataTable being validated</param>
    /// <param name="mapping">Column mapping from source to destination</param>
    protected virtual void OnBeforeRowValidation(DataTable dt, Dictionary<DataColumn, DiscoveredColumn> mapping)
    {
        // Base implementation does nothing - subclasses can override
    }

    /// <summary>
    ///     Returns the valid integer range for a SQL type. Override in subclasses to handle
    ///     database-specific integer types (e.g., MySQL's MEDIUMINT, TINYINT UNSIGNED, BIGINT UNSIGNED).
    /// </summary>
    /// <param name="sqlType">The SQL type name (uppercase)</param>
    /// <returns>Tuple of (min, max) values for the type</returns>
    protected virtual (BigInteger min, BigInteger max) GetIntegerRange(string sqlType) =>
        sqlType switch
        {
            // Standard SQL types common across databases
            "TINYINT" => (0, 255), // ANSI SQL TINYINT is unsigned
            "SMALLINT" => (short.MinValue, short.MaxValue),
            "INT" or "INTEGER" => (int.MinValue, int.MaxValue),
            "BIGINT" => (long.MinValue, long.MaxValue),
            _ => (long.MinValue, long.MaxValue)
        };

    /// <summary>
    ///     Checks if a CLR type is an integer type that should have range validation.
    /// </summary>
    /// <param name="type">The CLR type to check</param>
    /// <returns>True if the type is an integer type</returns>
    protected static bool IsIntegerType(Type type) =>
        type == typeof(byte) || type == typeof(sbyte) ||
        type == typeof(short) || type == typeof(ushort) ||
        type == typeof(int) || type == typeof(uint) ||
        type == typeof(long) || type == typeof(ulong) ||
        (Nullable.GetUnderlyingType(type) is { } underlying && IsIntegerType(underlying));

    /// <summary>
    ///     Returns the precision and scale of a decimal column: those that its SQL type declares, or else those of its
    ///     <see cref="DiscoveredDataType.GetDecimalSize" />, or (0, 0) if neither gives them.
    /// </summary>
    /// <remarks>
    ///     A <see cref="DecimalSize" /> cannot have a scale larger than its precision, as in numeric(3,5) on
    ///     PostgreSQL 15 or later and NUMBER(3,5) on Oracle: it has no digits before the decimal point and 5 after it,
    ///     which makes 0.99999 rather than 0.00999 the largest value. A negative scale is not taken from the SQL type,
    ///     and gives no DecimalSize either.
    /// </remarks>
    private static (int Precision, int Scale) GetDecimalPrecisionAndScale(DiscoveredColumn column)
    {
        if (DecimalColumnRange.TryGetPrecisionAndScale(column.DataType?.SQLType, out var precision, out var scale))
            return (precision, scale);

        var size = column.DataType?.GetDecimalSize();
        return size == null ? (0, 0) : (size.Precision, size.Scale);
    }
}
