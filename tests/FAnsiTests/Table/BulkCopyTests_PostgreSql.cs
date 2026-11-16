using System.Data;
using System.Globalization;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Table;

internal sealed class BulkCopyTests_PostgreSql : BulkCopyTestsBase
{
    private const DatabaseType DbType = DatabaseType.PostgreSql;

    #region Timeout and Cancellation

    [Test]
    public void Upload_WithTimeout_RespectsSetting() => Upload_WithTimeout_RespectsSetting(DbType);

    #endregion

    #region Auto-Increment and Identity Columns

    [Test]
    public void Upload_WithAutoIncrementColumn_GeneratesValues() =>
        Upload_WithAutoIncrementColumn_GeneratesValues(DbType);

    #endregion

    #region Disposal and Resource Management

    [Test]
    public void Upload_AfterDispose_ThrowsException() => Upload_AfterDispose_ThrowsException(DbType);

    #endregion

    #region Basic Upload Operations

    [Test]
    public void Upload_EmptyDataTable_ReturnsZero() => Upload_EmptyDataTable_ReturnsZero(DbType);

    [Test]
    public void Upload_SingleRow_Success() => Upload_SingleRow_Success(DbType);

    [Test]
    public void Upload_MultipleRows_AllInserted() => Upload_MultipleRows_AllInserted(DbType);

    [Test]
    public void Upload_MultipleBatches_Success() => Upload_MultipleBatches_Success(DbType);

    #endregion

    #region NULL Value Handling

    [Test]
    public void Upload_NullValues_InsertedCorrectly() => Upload_NullValues_InsertedCorrectly(DbType);

    [Test]
    public void Upload_EmptyStrings_ConvertedToNull() => Upload_EmptyStrings_ConvertedToNull(DbType);

    #endregion

    #region Error Handling - Data Type Violations

    [Test]
    public void Upload_StringTooLong_ThrowsException() => Upload_StringTooLong_ThrowsException(DbType);

    [Test]
    public void Upload_DecimalOutOfRange_ThrowsException() => Upload_DecimalOutOfRange_ThrowsException(DbType);

    [Test]
    public void Upload_InvalidDecimalFormat_ThrowsFormatException() =>
        Upload_InvalidDecimalFormat_ThrowsFormatException(DbType);

    [Test]
    public void Upload_IntegerOverflow_ThrowsException() => Upload_IntegerOverflow_ThrowsException(DbType);

    #endregion

    #region Error Handling - Constraint Violations

    [Test]
    public void Upload_ViolateNotNullConstraint_ThrowsException() =>
        Upload_ViolateNotNullConstraint_ThrowsException(DbType);

    [Test]
    public void Upload_DuplicatePrimaryKey_ThrowsException() => Upload_DuplicatePrimaryKey_ThrowsException(DbType);

    #endregion

    #region Column Mapping

    [Test]
    public void Upload_ReorderedColumns_MapsCorrectly() => Upload_ReorderedColumns_MapsCorrectly(DbType);

    [Test]
    public void Upload_SubsetOfColumns_UsesDefaults() => Upload_SubsetOfColumns_UsesDefaults(DbType);

    [Test]
    public void Upload_ExtraColumnsInDataTable_ThrowsException() =>
        Upload_ExtraColumnsInDataTable_ThrowsException(DbType);

    [Test]
    public void Upload_CaseMismatchedColumns_MapsCorrectly() => Upload_CaseMismatchedColumns_MapsCorrectly(DbType);

    #endregion

    #region Transaction Behavior

    [Test]
    public void Upload_WithTransaction_CommitsProperly() => Upload_WithTransaction_CommitsProperly(DbType);

    [Test]
    public void Upload_WithTransaction_RollbackWorks() => Upload_WithTransaction_RollbackWorks(DbType);

    [Test]
    public void Upload_TransactionError_RollsBackAutomatically() =>
        Upload_TransactionError_RollsBackAutomatically(DbType);

    #endregion

    #region Special Data Types

    [Test]
    public void Upload_UnicodeStrings_PreservedCorrectly() => Upload_UnicodeStrings_PreservedCorrectly(DbType);

    [Test]
    public void Upload_DateTimeValues_PreservedCorrectly() => Upload_DateTimeValues_PreservedCorrectly(DbType);

    [Test]
    public void Upload_DateTimeStrings_ParsedCorrectly() => Upload_DateTimeStrings_ParsedCorrectly(DbType);

    [Test]
    public void Upload_BinaryData_PreservedCorrectly() => Upload_BinaryData_PreservedCorrectly(DbType);

    [Test]
    public void Upload_BooleanValues_ConvertedCorrectly() => Upload_BooleanValues_ConvertedCorrectly(DbType);

    [Test]
    public void Upload_DecimalPrecision_PreservedCorrectly() => Upload_DecimalPrecision_PreservedCorrectly(DbType);

    #endregion

    #region Performance and Large Datasets

    [Test]
    public void Upload_LargeDataset_CompletesSuccessfully() => Upload_LargeDataset_CompletesSuccessfully(DbType);

    [Test]
    public void Upload_WideTable_AllColumnsInserted() => Upload_WideTable_AllColumnsInserted(DbType);

    #endregion

    #region Decimal Precision and Scale Validation

    [Test]
    public void Upload_DecimalExceedsPrecision_ThrowsException() =>
        Upload_DecimalExceedsPrecision_ThrowsException(DbType);

    [Test]
    public void Upload_DecimalExceedsScale_ThrowsException() => Upload_DecimalExceedsScale_ThrowsException(DbType);

    [Test]
    public void Upload_DecimalMaxPrecisionAndScale_Success() => Upload_DecimalMaxPrecisionAndScale_Success(DbType);

    [Test]
    public void Upload_DecimalLargePrecisionViolation_ThrowsException() =>
        Upload_DecimalLargePrecisionViolation_ThrowsException(DbType);

    [Test]
    public void Upload_DecimalZeroScale_Success() => Upload_DecimalZeroScale_Success(DbType);

    [Test]
    public void Upload_DecimalZeroScale_WithDecimalPlaces_ThrowsException() =>
        Upload_DecimalZeroScale_WithDecimalPlaces_ThrowsException(DbType);

    [Test]
    public void Upload_DecimalNullValues_IgnoredInValidation() => Upload_DecimalNullValues_IgnoredInValidation(DbType);

    #endregion

    #region Destination Column Types

    // PostgreSQL does not convert values sent in the binary COPY format, each one has to be written as the type of
    // the destination column whatever the type of the DataColumn it came from

    private static readonly object[] DifferentlyTypedColumnCases =
    [
        new object[] { 5, "bigint", 5L },
        new object[] { 5L, "integer", 5 },
        new object[] { (short)5, "integer", 5 },
        new object[] { 5, "numeric(10,2)", 5m },
        new object[] { 5.5d, "numeric(10,2)", 5.5m },
        new object[] { 5.5m, "double precision", 5.5d },
        new object[] { 5, "double precision", 5d },
        new object[] { 5.5m, "real", 5.5f },
        new object[] { 5, "varchar(10)", "5" },
        new object[] { 5.5m, "text", "5.5" },
        new object[] { new DateTime(2001, 2, 3, 4, 5, 6), "varchar(100)", "2001-02-03 04:05:06" },
        new object[] { new DateTime(2001, 2, 3, 4, 5, 6, 789), "text", "2001-02-03 04:05:06.789" },
        new object[] { 5.5f, "double precision", 5.5d },
        new object[] { 5.5d, "real", 5.5f },
        new object[] { new DateOnly(2001, 2, 3), "timestamp", new DateTime(2001, 2, 3) },
        new object[] { new DateOnly(2001, 2, 3), "timestamp with time zone", new DateTime(2001, 2, 3) },
        new object[] { true, "varchar(10)", "true" }
    ];

    [TestCaseSource(nameof(DifferentlyTypedColumnCases))]
    public void Upload_ValueIntoDifferentlyTypedColumn_IsConverted(object value, string destinationType,
        object expected)
    {
        var db = GetTestDatabase(DbType);
        var tbl = db.CreateTable("BulkDifferentTypes",
        [
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 10)),
            new DatabaseColumnRequest("Value", destinationType)
        ]);

        try
        {
            using var dt = new DataTable();
            dt.Columns.Add("Name", typeof(string));
            dt.Columns.Add(CreateValueColumn(value));
            dt.Rows.Add("fff", value);
            dt.Rows.Add("rrr", DBNull.Value);

            using (var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture))
            {
                Assert.That(bulk.Upload(dt), Is.EqualTo(2));
            }

            using var result = tbl.GetDataTable();
            Assert.Multiple(() =>
            {
                Assert.That(GetValue(result, "fff"), Is.EqualTo(expected));
                Assert.That(GetValue(result, "rrr"), Is.EqualTo(DBNull.Value));
            });
        }
        finally
        {
            tbl.Drop();
        }
    }

    private static DataColumn CreateValueColumn(object value) =>
        value switch
        {
            short => new DataColumn("Value", typeof(short)),
            int => new DataColumn("Value", typeof(int)),
            long => new DataColumn("Value", typeof(long)),
            float => new DataColumn("Value", typeof(float)),
            double => new DataColumn("Value", typeof(double)),
            decimal => new DataColumn("Value", typeof(decimal)),
            DateTime => new DataColumn("Value", typeof(DateTime)),
            DateOnly => new DataColumn("Value", typeof(DateOnly)),
            bool => new DataColumn("Value", typeof(bool)),
            _ => throw new ArgumentOutOfRangeException(nameof(value))
        };

    private static object GetValue(DataTable dt, string name) =>
        dt.Rows.Cast<DataRow>().Single(r => Equals(r["Name"], name))["Value"];

    /// <summary>
    ///     Mirrors loading a table whose date column is a varchar from a table whose date column is a timestamp
    /// </summary>
    [Test]
    public void Upload_DataTableReadFromTimestampColumn_IntoStringColumn()
    {
        var db = GetTestDatabase(DbType);

        using var source = new DataTable();
        source.Columns.Add("Col1");
        source.Columns.Add("dateColumn");
        source.Rows.Add("fff", "2001-02-03 04:05:06");

        var from = db.CreateTable("BulkDateSource", source);
        var to = db.CreateTable("BulkDateDestination",
        [
            new DatabaseColumnRequest("Col1", new DatabaseTypeRequest(typeof(string), 5)),
            new DatabaseColumnRequest("dateColumn", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        try
        {
            using var read = from.GetDataTable();
            Assert.That(read.Columns["dateColumn"]!.DataType, Is.EqualTo(typeof(DateTime)));

            using (var bulk = to.BeginBulkInsert(CultureInfo.InvariantCulture))
            {
                Assert.That(bulk.Upload(read), Is.EqualTo(1));
            }

            using var result = to.GetDataTable();
            Assert.That(result.Rows[0]["dateColumn"], Is.EqualTo("2001-02-03 04:05:06"));
        }
        finally
        {
            to.Drop();
            from.Drop();
        }
    }

    /// <summary>
    ///     What is stored for a DateTime depends on the type of the column and on nothing else, in particular not on whether
    ///     it was inserted as a parameter or by bulk copy, nor on the time zones of the client and server:
    ///     <list type="bullet">
    ///         <item>a 'timestamp' has no time zone, it stores the date and time exactly as given whatever the Kind</item>
    ///         <item>
    ///             a 'timestamp with time zone' stores an instant, a Local value is converted to UTC and an Unspecified one
    ///             is taken to be UTC already
    ///         </item>
    ///     </list>
    /// </summary>
    [TestCase("timestamp", DateTimeKind.Unspecified)]
    [TestCase("timestamp", DateTimeKind.Local)]
    [TestCase("timestamp", DateTimeKind.Utc)]
    [TestCase("timestamp with time zone", DateTimeKind.Unspecified)]
    [TestCase("timestamp with time zone", DateTimeKind.Local)]
    [TestCase("timestamp with time zone", DateTimeKind.Utc)]
    public void DateTime_InsertAndBulkCopyStoreTheSameValue(string columnType, DateTimeKind kind)
    {
        var value = new DateTime(2001, 2, 3, 4, 5, 6, kind);
        var withTimeZone = columnType.EndsWith("with time zone", StringComparison.Ordinal);
        var expected = (withTimeZone && kind == DateTimeKind.Local ? value.ToUniversalTime() : value)
            .ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

        var db = GetTestDatabase(DbType);
        var tbl = db.CreateTable("DateKinds",
        [
            new DatabaseColumnRequest("How", new DatabaseTypeRequest(typeof(string), 10)),
            new DatabaseColumnRequest("MyDate", columnType)
        ]);

        try
        {
            tbl.Insert(new Dictionary<string, object>
            {
                { "How", "insert" },
                { "MyDate", value }
            });

            using var dt = new DataTable();
            dt.Columns.Add("How", typeof(string));
            //by default a DataTable makes every DateTime it is given Unspecified, these modes keep the Kind
            dt.Columns.Add(new DataColumn("MyDate", typeof(DateTime))
            {
                DateTimeMode = kind switch
                {
                    DateTimeKind.Local => DataSetDateTime.Local,
                    DateTimeKind.Utc => DataSetDateTime.Utc,
                    _ => DataSetDateTime.Unspecified
                }
            });
            dt.Rows.Add("bulk", value);

            using (var bulk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture))
            {
                Assert.That(bulk.Upload(dt), Is.EqualTo(1));
            }

            //the server formats what it stored so that the result does not depend on any client side conversion
            var stored = new Dictionary<string, string>();
            var storedDate = withTimeZone ? "\"MyDate\" AT TIME ZONE 'UTC'" : "\"MyDate\"";

            using (var con = db.Server.GetConnection())
            {
                con.Open();
                using var cmd = db.Server.GetCommand(
                    $"SELECT \"How\", to_char({storedDate}, 'YYYY-MM-DD HH24:MI:SS') FROM {tbl.GetFullyQualifiedName()}",
                    con);
                using var r = cmd.ExecuteReader();
                while (r.Read())
                    stored.Add(r.GetString(0), r.GetString(1));
            }

            Assert.Multiple(() =>
            {
                Assert.That(stored["insert"], Is.EqualTo(expected));
                Assert.That(stored["bulk"], Is.EqualTo(expected));
            });
        }
        finally
        {
            tbl.Drop();
        }
    }

    #endregion
}
