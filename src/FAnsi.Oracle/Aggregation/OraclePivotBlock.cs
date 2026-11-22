namespace FAnsi.Implementations.Oracle.Aggregation;

/// <summary>
///     Writes the anonymous PL/SQL block that <see cref="OracleAggregateHelper" /> builds a pivot with.  A query cannot
///     choose its own columns, and Oracle's PIVOT needs its values written into the query, so the block reads the pivot
///     values, adds a column for each to the query being pivoted and returns that query's rows as an implicit result
///     (DBMS_SQL.RETURN_RESULT, Oracle 12c and later), which ODP.NET reads like the result of a SELECT.
/// </summary>
internal static class OraclePivotBlock
{
    /// <summary>
    ///     The most bytes in an Oracle name with long identifiers: from Oracle 12.2, when COMPATIBLE is 12.2 or higher
    /// </summary>
    internal const int MaxNameBytes = 128;

    /// <summary>
    ///     The most bytes in an Oracle name without long identifiers: on Oracle 12.1, or when COMPATIBLE is lower than 12.2
    ///     (which Oracle 12.2, 18c and 19c allow)
    /// </summary>
    internal const int MaxShortNameBytes = 30;

    /// <summary>
    ///     The PL/SQL for the most bytes a name can have on the server the block runs on.  From Oracle 12.2 the function
    ///     ORA_MAX_NAME_LEN_SUPPORTED gives it, as COMPATIBLE allows (<see cref="MaxNameBytes" /> or
    ///     <see cref="MaxShortNameBytes" />).  Oracle 12.1 does not have the function, so conditional compilation leaves it
    ///     out there, where the most is <see cref="MaxShortNameBytes" />.  The limit is worked out where the block runs, so
    ///     the SQL of the block is the same whatever the server.
    /// </summary>
    internal static readonly string MaxNameBytesSql =
        $"$IF DBMS_DB_VERSION.VER_LE_12_1 $THEN {MaxShortNameBytes} $ELSE ORA_MAX_NAME_LEN_SUPPORTED $END";

    /// <summary>
    ///     How a DATE pivot value is written to name its column, whatever the session's NLS_DATE_FORMAT
    /// </summary>
    internal const string DateFormat = "YYYY-MM-DD HH24:MI:SS";

    /// <summary>
    ///     How a TIMESTAMP pivot value is written to name its column, before the trailing zeros and any decimal point are
    ///     taken off its fraction of a second.  A TIMESTAMP WITH TIME ZONE is written as its UTC time followed by " UTC".
    /// </summary>
    internal const string TimestampFormat = "YYYY-MM-DD HH24:MI:SS.FF9";

    // The column types DBMS_SQL.DESCRIBE_COLUMNS2 gives the date and timestamp types
    private const int DateTypeCode = 12;
    private const int TimestampTypeCode = 180;
    private const int TimestampWithTimeZoneTypeCode = 181;
    private const int TimestampWithLocalTimeZoneTypeCode = 231;

    /// <summary>
    ///     <para>
    ///         Returns the block for a pivot.  It reads the values listed by <paramref name="pivotValuesSql" /> (fansi_rank and
    ///         fansi_value), adds a column for each to the query <paramref name="selectSql" /> ... <paramref name="fromSql" />
    ///         and returns that query's rows.
    ///     </para>
    ///     <para>
    ///         The query joins the dataset to the pivot values on the pivot column's own type, so values are matched
    ///         exactly whatever the session's NLS settings.  Each column sums the aggregate over the rows of the dataset with
    ///         the value ranked at its position and is 0 where there are none (as ISNULL(...,0) gives on SQL Server).  It
    ///         also checks the text of the value, so that should the ranking change between the two runs of the values
    ///         query its counts are left out rather than shown under another value's name.
    ///     </para>
    ///     <para>
    ///         The text of a value is TO_CHAR of it, except that a date or timestamp, as DBMS_SQL describes the pivot column,
    ///         is written out in full (<see cref="DateFormat" />, <see cref="TimestampFormat" />) rather than in the
    ///         session's NLS format, which by default leaves out the century and the time and so can give two dates the same
    ///         text.  Each column is named after that text, without the double quotes and NUL characters an Oracle name
    ///         cannot hold and cut to whole characters within the most bytes a name can have on the server
    ///         (<see cref="MaxNameBytesSql" />).  A value left with no name (one made only of double quotes), or with the
    ///         name of an earlier pivot column, gets its position among the pivot columns as a suffix (e.g. _3, then _3_2
    ///         should that be taken too), so that every column has a valid name of its own.
    ///     </para>
    /// </summary>
    /// <param name="preSelectSql">The lines of the query that go before its SELECT</param>
    /// <param name="pivotValuesSql">The query of the values that get a column, ranked in the order of their columns</param>
    /// <param name="selectSql">The query being pivoted, up to the end of its SELECT list</param>
    /// <param name="fromSql">The rest of the query being pivoted, from its FROM</param>
    internal static string Build(string preSelectSql, string pivotValuesSql, string selectSql, string fromSql)
    {
        var block = $"""
                     {preSelectSql}
                     DECLARE
                         TYPE fansi_name_set IS TABLE OF BOOLEAN INDEX BY VARCHAR2({MaxNameBytes});
                         fansi_pivot_names fansi_name_set;
                         -- The most bytes a name can have on this server
                         fansi_pivot_max_bytes PLS_INTEGER := {MaxNameBytesSql};
                         fansi_pivot_sql CLOB;
                         fansi_pivot_columns CLOB;
                         fansi_pivot_type PLS_INTEGER;
                         fansi_pivot_rank NUMBER;
                         fansi_pivot_label VARCHAR2(32767);
                         fansi_pivot_name VARCHAR2(32767);
                         fansi_pivot_try PLS_INTEGER;
                         fansi_describe_cursor INTEGER;
                         fansi_describe_count INTEGER;
                         fansi_describe_columns DBMS_SQL.DESC_TAB2;
                         fansi_values_cursor SYS_REFCURSOR;
                         fansi_result_cursor SYS_REFCURSOR;

                         -- The SQL for the text of the pivot value fansi_value_sql, with dates and timestamps written out in full
                         FUNCTION fansi_pivot_text(fansi_value_sql VARCHAR2) RETURN VARCHAR2 IS
                         BEGIN
                             RETURN CASE fansi_pivot_type
                                 WHEN {DateTypeCode} THEN 'TO_CHAR(' || fansi_value_sql || ', ''{DateFormat}'')'
                                 WHEN {TimestampTypeCode} THEN 'RTRIM(RTRIM(TO_CHAR(' || fansi_value_sql || ', ''{TimestampFormat}''), ''0''), ''.'')'
                                 WHEN {TimestampWithLocalTimeZoneTypeCode} THEN 'RTRIM(RTRIM(TO_CHAR(' || fansi_value_sql || ', ''{TimestampFormat}''), ''0''), ''.'')'
                                 WHEN {TimestampWithTimeZoneTypeCode} THEN 'RTRIM(RTRIM(TO_CHAR(SYS_EXTRACT_UTC(' || fansi_value_sql || '), ''{TimestampFormat}''), ''0''), ''.'') || '' UTC'''
                                 ELSE 'TO_CHAR(' || fansi_value_sql || ')'
                             END;
                         END;

                         -- fansi_text as a name, without the characters a name cannot hold and cut so that with fansi_suffix it fits
                         FUNCTION fansi_pivot_column_name(fansi_text VARCHAR2, fansi_suffix VARCHAR2) RETURN VARCHAR2 IS
                             fansi_name VARCHAR2(32767) := SUBSTR(REPLACE(REPLACE(fansi_text, '"'), CHR(0)), 1, fansi_pivot_max_bytes);
                         BEGIN
                             WHILE fansi_name IS NOT NULL AND LENGTHB(fansi_name) + NVL(LENGTHB(fansi_suffix), 0) > fansi_pivot_max_bytes LOOP
                                 fansi_name := SUBSTR(fansi_name, 1, LENGTH(fansi_name) - 1);
                             END LOOP;
                             RETURN fansi_name || fansi_suffix;
                         END;
                     BEGIN
                         fansi_pivot_sql := {ToClob(pivotValuesSql)};

                         -- The type of the pivot column decides how its values are written as text
                         fansi_describe_cursor := DBMS_SQL.OPEN_CURSOR;
                         BEGIN
                             DBMS_SQL.PARSE(fansi_describe_cursor, fansi_pivot_sql, DBMS_SQL.NATIVE);
                             DBMS_SQL.DESCRIBE_COLUMNS2(fansi_describe_cursor, fansi_describe_count, fansi_describe_columns);
                         EXCEPTION
                             WHEN OTHERS THEN
                                 DBMS_SQL.CLOSE_CURSOR(fansi_describe_cursor);
                                 RAISE;
                         END;
                         DBMS_SQL.CLOSE_CURSOR(fansi_describe_cursor);
                         fansi_pivot_type := fansi_describe_columns(2).col_type;

                         OPEN fansi_values_cursor FOR 'SELECT fansi_rank, ' || fansi_pivot_text('fansi_value') || ' FROM ('
                             || CHR(10) || fansi_pivot_sql || CHR(10) || ') ORDER BY fansi_rank';
                         LOOP
                             FETCH fansi_values_cursor INTO fansi_pivot_rank, fansi_pivot_label;
                             EXIT WHEN fansi_values_cursor%NOTFOUND;

                             fansi_pivot_try := 0;
                             LOOP
                                 fansi_pivot_name := fansi_pivot_column_name(fansi_pivot_label,
                                     CASE WHEN fansi_pivot_try > 0 THEN '_' || fansi_pivot_rank END
                                     || CASE WHEN fansi_pivot_try > 1 THEN '_' || fansi_pivot_try END);
                                 EXIT WHEN fansi_pivot_name IS NOT NULL AND NOT fansi_pivot_names.EXISTS(fansi_pivot_name);
                                 fansi_pivot_try := fansi_pivot_try + 1;
                             END LOOP;
                             fansi_pivot_names(fansi_pivot_name) := TRUE;

                             fansi_pivot_columns := fansi_pivot_columns
                                 || ', COALESCE(SUM(CASE WHEN fansi_pivot_values.fansi_rank = ' || fansi_pivot_rank
                                 || ' AND ' || fansi_pivot_text('fansi_pivot_values.fansi_value') || ' = '''
                                 || REPLACE(fansi_pivot_label, '''', '''''')
                                 || ''' THEN fansi_pivot_dataset.fansi_aggregate END), 0) AS "' || fansi_pivot_name || '"';
                         END LOOP;
                         CLOSE fansi_values_cursor;

                         fansi_pivot_sql := {ToClob(selectSql)}
                             || fansi_pivot_columns
                             || {ToClob(Environment.NewLine + fromSql)};
                         OPEN fansi_result_cursor FOR fansi_pivot_sql;
                         DBMS_SQL.RETURN_RESULT(fansi_result_cursor);
                     END;
                     """;

        // PL/SQL sent with Windows line endings can fail to compile (PLS-00103)
        return block.ReplaceLineEndings("\n");
    }

    /// <summary>
    ///     Returns a PL/SQL expression for a CLOB holding <paramref name="sql" />.  It is built from several string literals
    ///     because a PL/SQL literal cannot be longer than 32767 bytes, which a long query (e.g. one with a big IN list) can be.
    /// </summary>
    internal static string ToClob(string sql)
    {
        // 4000 characters are at most 16000 bytes in any character set
        const int maxLiteralLength = 4000;

        var literals = new List<string>();

        for (var start = 0; start < sql.Length;)
        {
            var length = Math.Min(maxLiteralLength, sql.Length - start);

            // keep surrogate pairs together
            if (start + length < sql.Length && char.IsHighSurrogate(sql[start + length - 1]))
                length--;

            literals.Add($"TO_CLOB('{sql.Substring(start, length).Replace("'", "''", StringComparison.Ordinal)}')");
            start += length;
        }

        return literals.Count == 0 ? "TO_CLOB('')" : string.Join($"{Environment.NewLine}|| ", literals);
    }
}
