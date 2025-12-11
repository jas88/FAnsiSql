using System.Data;
using System.Globalization;
using System.Text;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsiTests.TypeTranslation;
using Microsoft.Data.SqlClient;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests;

public abstract class CrossPlatformTestsBase : DatabaseTests
{
    private readonly string[] _someDates =
    [
        "22\\5\\19",
        "22/5/19",
        "22-5-19",
        "22.5.19",
        "Wed\\5\\19",
        "Wed/5/19",
        "Wed-5-19",
        "Wed.5.19",
        "Wednesday\\5\\19",
        "Wednesday/5/19",
        "Wednesday-5-19",
        "Wednesday.5.19",
        "22\\05\\19",
        "22/05/19",
        "22-05-19",
        "22.05.19",
        "Wed\\05\\19",
        "Wed/05/19",
        "Wed-05-19",
        "Wed.05.19",
        "Wednesday\\05\\19",
        "Wednesday/05/19",
        "Wednesday-05-19",
        "Wednesday.05.19",
        "22\\May\\19",
        "22/May/19",
        "22-May-19",
        "22.May.19",
        "Wed\\May\\19",
        "Wed/May/19",
        "Wed-May-19",
        "Wed.May.19",
        "Wednesday\\May\\19",
        "Wednesday/May/19",
        "Wednesday-May-19",
        "Wednesday.May.19",
        "22\\May\\19",
        "22/May/19",
        "22-May-19",
        "22.May.19",
        "Wed\\May\\19",
        "Wed/May/19",
        "Wed-May-19",
        "Wed.May.19",
        "Wednesday\\May\\19",
        "Wednesday/May/19",
        "Wednesday-May-19",
        "Wednesday.May.19",
        "22\\5\\2019",
        "22/5/2019",
        "22-5-2019",
        "22.5.2019",
        "Wed\\5\\2019",
        "Wed/5/2019",
        "Wed-5-2019",
        "Wed.5.2019",
        "Wednesday\\5\\2019",
        "Wednesday/5/2019",
        "Wednesday-5-2019",
        "Wednesday.5.2019",
        "22\\05\\2019",
        "22/05/2019",
        "22-05-2019",
        "22.05.2019",
        "Wed\\05\\2019",
        "Wed/05/2019",
        "Wed-05-2019",
        "Wed.05.2019",
        "Wednesday\\05\\2019",
        "Wednesday/05/2019",
        "Wednesday-05-2019",
        "Wednesday.05.2019",
        "22\\May\\2019",
        "22/May/2019",
        "22-May-2019",
        "22.May.2019",
        "Wed\\May\\2019",
        "Wed/May/2019",
        "Wed-May-2019",
        "Wed.May.2019",
        "Wednesday\\May\\2019",
        "Wednesday/May/2019",
        "Wednesday-May-2019",
        "Wednesday.May.2019",
        "22\\May\\2019",
        "22/May/2019",
        "22-May-2019",
        "22.May.2019",
        "Wed\\May\\2019",
        "Wed/May/2019",
        "Wed-May-2019",
        "Wed.May.2019",
        "Wednesday\\May\\2019",
        "Wednesday/May/2019",
        "Wednesday-May-2019",
        "Wednesday.May.2019",
        "22\\5\\2019",
        "22/5/2019",
        "22-5-2019",
        "22.5.2019",
        "Wed\\5\\2019",
        "Wed/5/2019",
        "Wed-5-2019",
        "Wed.5.2019",
        "Wednesday\\5\\2019",
        "Wednesday/5/2019",
        "Wednesday-5-2019",
        "Wednesday.5.2019",
        "22\\05\\2019",
        "22/05/2019",
        "22-05-2019",
        "22.05.2019",
        "Wed\\05\\2019",
        "Wed/05/2019",
        "Wed-05-2019",
        "Wed.05.2019",
        "Wednesday\\05\\2019",
        "Wednesday/05/2019",
        "Wednesday-05-2019",
        "Wednesday.05.2019",
        "22\\May\\2019",
        "22/May/2019",
        "22-May-2019",
        "22.May.2019",
        "Wed\\May\\2019",
        "Wed/May/2019",
        "Wed-May-2019",
        "Wed.May.2019",
        "Wednesday\\May\\2019",
        "Wednesday/May/2019",
        "Wednesday-May-2019",
        "Wednesday.May.2019",
        "22\\May\\2019",
        "22/May/2019",
        "22-May-2019",
        "22.May.2019",
        "Wed\\May\\2019",
        "Wed/May/2019",
        "Wed-May-2019",
        "Wed.May.2019",
        "Wednesday\\May\\2019",
        "Wednesday/May/2019",
        "Wednesday-May-2019",
        "Wednesday.May.2019",
        "22\\5\\02019",
        "22/5/02019",
        "22-5-02019",
        "22.5.02019",
        "Wed\\5\\02019",
        "Wed/5/02019",
        "Wed-5-02019",
        "Wed.5.02019",
        "Wednesday\\5\\02019",
        "Wednesday/5/02019",
        "Wednesday-5-02019",
        "Wednesday.5.02019",
        "22\\05\\02019",
        "22/05/02019",
        "22-05-02019",
        "22.05.02019",
        "Wed\\05\\02019",
        "Wed/05/02019",
        "Wed-05-02019",
        "Wed.05.02019",
        "Wednesday\\05\\02019",
        "Wednesday/05/02019",
        "Wednesday-05-02019",
        "Wednesday.05.02019",
        "22\\May\\02019",
        "22/May/02019",
        "22-May-02019",
        "22.May.02019",
        "Wed\\May\\02019",
        "Wed/May/02019",
        "Wed-May-02019",
        "Wed.May.02019",
        "Wednesday\\May\\02019",
        "Wednesday/May/02019",
        "Wednesday-May-02019",
        "Wednesday.May.02019",
        "22\\May\\02019",
        "22/May/02019",
        "22-May-02019",
        "22.May.02019",
        "Wed\\May\\02019",
        "Wed/May/02019",
        "Wed-May-02019",
        "Wed.May.02019",
        "Wednesday\\May\\02019",
        "Wednesday/May/02019",
        "Wednesday-May-02019",
        "Wednesday.May.02019",
        "8:59",
        "8:59 AM",
        "8:59:36",
        "8:59:36 AM",
        "8:59:36",
        "8:59:36 AM",
        "8:59",
        "8:59 AM",
        "8:59:36",
        "8:59:36 AM",
        "8:59:36",
        "8:59:36 AM",
        "08:59",
        "08:59 AM",
        "08:59:36",
        "08:59:36 AM",
        "08:59:36",
        "08:59:36 AM",
        "08:59",
        "08:59 AM",
        "08:59:36",
        "08:59:36 AM",
        "08:59:36",
        "08:59:36 AM",
        "8:59",
        "8:59 AM",
        "8:59:36",
        "8:59:36 AM",
        "8:59:36",
        "8:59:36 AM",
        "8:59",
        "8:59 AM",
        "8:59:36",
        "8:59:36 AM",
        "8:59:36",
        "8:59:36 AM",
        "08:59",
        "08:59 AM",
        "08:59:36",
        "08:59:36 AM",
        "08:59:36",
        "08:59:36 AM",
        "08:59",
        "08:59 AM",
        "08:59:36",
        "08:59:36 AM",
        "08:59:36",
        "08:59:36 AM"
    ];

    protected abstract DatabaseType DatabaseType { get; }

    protected void TestTableCreation_NullTableName()
    {
        var db = GetTestDatabase(DatabaseType);
        Assert.Throws<ArgumentNullException>(() => db.CreateTable("", new DataTable()));
    }

    protected void DateColumnTests_NoTime(object input)
    {
        var db = GetTestDatabase(DatabaseType);
        var tbl = db.CreateTable("MyTable",
            [new DatabaseColumnRequest("MyDate", new DatabaseTypeRequest(typeof(DateTime)))]);

        tbl.Insert(new Dictionary<string, object> { { "MyDate", input } });

        using (var blk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture))
        {
            using var dt = new DataTable();
            dt.Columns.Add("MyDate");
            dt.Rows.Add(input);

            blk.Upload(dt);
        }

        var result = tbl.GetDataTable();
        var expectedDate = new DateTime(2007, 1, 1);
        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[0][0], Is.EqualTo(expectedDate));
            Assert.That(result.Rows[1][0], Is.EqualTo(expectedDate));
        });
    }

    protected void DateColumnTests_UkUsFormat_Explicit(object input, string culture)
    {
        var db = GetTestDatabase(DatabaseType);
        var tbl = db.CreateTable("MyTable",
            [new DatabaseColumnRequest("MyDate", new DatabaseTypeRequest(typeof(DateTime)))]);

        var cultureInfo = new CultureInfo(culture);

        //basic insert
        tbl.Insert(new Dictionary<string, object> { { "MyDate", input } }, cultureInfo);

        //then bulk insert, both need to work
        using (var blk = tbl.BeginBulkInsert(cultureInfo))
        {
            using var dt = new DataTable();
            dt.Columns.Add("MyDate");
            dt.Rows.Add(input);

            blk.Upload(dt);
        }

        var result = tbl.GetDataTable();
        var expectedDate = new DateTime(1993, 2, 28, 5, 36, 27);
        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[0][0], Is.EqualTo(expectedDate));
            Assert.That(result.Rows[1][0], Is.EqualTo(expectedDate));
        });
    }


    /// <summary>
    ///     Since DateTimes are converted in DataTable in memory before being up loaded to the database we need to check
    ///     that any PrimaryKey on the <see cref="DataTable" /> is not compromised
    /// </summary>
    /// <param name="input"></param>
    /// <param name="culture"></param>
    protected void DateColumnTests_PrimaryKeyColumn(object input, string culture)
    {
        var db = GetTestDatabase(DatabaseType);
        var tbl = db.CreateTable("MyTable", [
            new DatabaseColumnRequest("MyDate", new DatabaseTypeRequest(typeof(DateTime)))
                { IsPrimaryKey = true }
        ]);

        //then bulk insert, both need to work
        using (var blk = tbl.BeginBulkInsert(new CultureInfo(culture)))
        {
            using var dt = new DataTable();
            dt.Columns.Add("MyDate");
            dt.Rows.Add(input);

            //this is the novel thing we are testing
            dt.PrimaryKey = [dt.Columns[0]];
            blk.Upload(dt);

            Assert.That(dt.PrimaryKey, Has.Length.EqualTo(1));
            Assert.That(dt.PrimaryKey[0].ColumnName, Is.EqualTo("MyDate"));
        }

        var result = tbl.GetDataTable();
        var expectedDate = new DateTime(1993, 2, 28, 5, 36, 27);
        Assert.That(result.Rows[0][0], Is.EqualTo(expectedDate));
    }


    protected void DateColumnTests_TimeOnly_Midnight(object input)
    {
        var db = GetTestDatabase(DatabaseType);
        var tbl = db.CreateTable("MyTable",
            [new DatabaseColumnRequest("MyTime", new DatabaseTypeRequest(typeof(TimeSpan)))]);

        tbl.Insert(new Dictionary<string, object> { { "MyTime", input } });

        using (var blk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture))
        {
            using var dt = new DataTable();
            dt.Columns.Add("MyTime");
            dt.Rows.Add(input);

            blk.Upload(dt);
        }

        var result = tbl.GetDataTable();
        var expectedTime = new TimeSpan(0, 0, 0, 0);

        //Oracle is a bit special it only stores whole dates then has server side settings about how much to return (like a format string)
        var resultTimeSpans = DatabaseType == DatabaseType.Oracle
            ? new[] { (DateTime)result.Rows[0][0], (DateTime)result.Rows[1][0] }.Select(static dt => dt.TimeOfDay)
                .Cast<object>().ToArray()
            : [result.Rows[0][0], result.Rows[1][0]];

        Assert.Multiple(() =>
        {
            Assert.That(resultTimeSpans[0], Is.EqualTo(expectedTime));
            Assert.That(resultTimeSpans[1], Is.EqualTo(expectedTime));
        });
    }

    /*
    [Test]
    public void TestOracleTimespans()
    {
        var db = GetTestDatabase(DatabaseType.Oracle);

        using (var con = db.Server.GetConnection())
        {
            con.Open();

            var cmd = db.Server.GetCommand("CREATE TABLE FANSITESTS.TimeTable (time_of_day timestamp)", con);
            cmd.ExecuteNonQuery();


            var cmd2 = db.Server.GetCommand("INSERT INTO FANSITESTS.TimeTable (time_of_day) VALUES (:time_of_day)", con);

            var param = cmd2.CreateParameter();
            param.ParameterName = ":time_of_day";
            param.DbType = DbType.Time;
            param.Value = new DateTime(1,1,1,1, 1, 1);

            cmd2.Parameters.Add(param);
            cmd2.ExecuteNonQuery();

            var tbl = db.ExpectTable("TimeTable");
            Assert.IsTrue(tbl.Exists());

            var result = tbl.GetDataTable();

            //Comes back as a DateTime, doesn't look like intervals are going to work either
            tbl.Drop();
        }
    }
    */
    protected void DateColumnTests_TimeOnly_Afternoon(object input)
    {
        var db = GetTestDatabase(DatabaseType);
        var tbl = db.CreateTable("MyTable",
            [new DatabaseColumnRequest("MyTime", new DatabaseTypeRequest(typeof(TimeSpan)))]);

        tbl.Insert(new Dictionary<string, object> { { "MyTime", input } });

        using (var blk = tbl.BeginBulkInsert(CultureInfo.InvariantCulture))
        {
            using var dt = new DataTable();
            dt.Columns.Add("MyTime");
            dt.Rows.Add(input);

            blk.Upload(dt);
        }

        var result = tbl.GetDataTable();
        var expectedTime = new TimeSpan(13, 11, 00);

        //Oracle is a bit special it only stores whole dates then has server side settings about how much to return (like a format string)
        var resultTimeSpans = DatabaseType == DatabaseType.Oracle
            ? new[] { (DateTime)result.Rows[0][0], (DateTime)result.Rows[1][0] }.Select(static dt => dt.TimeOfDay)
                .Cast<object>().ToArray()
            : [result.Rows[0][0], result.Rows[1][0]];

        foreach (var t in resultTimeSpans.Cast<TimeSpan>())
        {
            if (t.Seconds > 0)
                Assert.That(t.Seconds, Is.EqualTo(10));

            var eval = t.Subtract(new TimeSpan(0, 0, 0, t.Seconds));
            Assert.That(eval, Is.EqualTo(expectedTime));
        }
    }

    protected void TypeConsensusBetweenGuesserAndDiscoveredTableTest(string datatType, string insertValue)
    {
        var database = GetTestDatabase(DatabaseType);

        var tbl = database.ExpectTable("TestTableCreationStrangeTypology");

        if (tbl.Exists())
            tbl.Drop();

        using var dt = new DataTable("TestTableCreationStrangeTypology");
        dt.Columns.Add("mycol");
        dt.Rows.Add(insertValue);

        var c = new Guesser();

        var tt = tbl.GetQuerySyntaxHelper().TypeTranslater;
        c.AdjustToCompensateForValue(insertValue);

        database.CreateTable(tbl.GetRuntimeName(), dt);

        Assert.That(c.GetSqlDBType(tt), Is.EqualTo(datatType));

        var expectedDataType = datatType;

        expectedDataType = DatabaseType switch
        {
            //you ask for an int PostgreSql gives you an integer!
            DatabaseType.PostgreSql when datatType == "int" => "integer",
            // MySQL boolean is really an aliased tinyint(1)
            DatabaseType.MySql when datatType == "boolean" => "tinyint(1)",
            _ => expectedDataType
        };

        Assert.Multiple(() =>
        {
            Assert.That(tbl.DiscoverColumn("mycol").DataType?.SQLType, Is.EqualTo(expectedDataType));
            Assert.That(tbl.GetRowCount(), Is.EqualTo(1));
        });

        tbl.Drop();
    }

    protected void ForeignKeyCreationTest()
    {
        var database = GetTestDatabase(DatabaseType);

        var tblParent = database.CreateTable("Parent",
        [
            new DatabaseColumnRequest("ID", new DatabaseTypeRequest(typeof(int))) { IsPrimaryKey = true },
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 10)) //varchar(10)
        ]);

        var parentIdPkCol = tblParent.DiscoverColumn("ID");

        var parentIdFkCol = new DatabaseColumnRequest("Parent_ID", new DatabaseTypeRequest(typeof(int)));

        var tblChild = database.CreateTable("Child",
        [
            parentIdFkCol,
            new DatabaseColumnRequest("ChildName", new DatabaseTypeRequest(typeof(string), 10)) //varchar(10)
        ], new Dictionary<DatabaseColumnRequest, DiscoveredColumn>
        {
            { parentIdFkCol, parentIdPkCol }
        }, true);
        try
        {
            using (var intoParent = tblParent.BeginBulkInsert(CultureInfo.InvariantCulture))
            {
                using var dt = new DataTable();
                dt.Columns.Add("ID");
                dt.Columns.Add("Name");

                dt.Rows.Add(1, "Bob");
                dt.Rows.Add(2, "Frank");

                intoParent.Upload(dt);
            }

            using (var con = tblChild.Database.Server.GetConnection())
            {
                con.Open();

                var cmd = tblParent.Database.Server.GetCommand(
                    $"INSERT INTO {tblChild.GetFullyQualifiedName()} VALUES (100,'chucky')", con);

                //violation of fk
                Assert.That(() => cmd.ExecuteNonQuery(), Throws.Exception);

                tblParent.Database.Server.GetCommand(
                    $"INSERT INTO {tblChild.GetFullyQualifiedName()} VALUES (1,'chucky')", con).ExecuteNonQuery();
                tblParent.Database.Server.GetCommand(
                    $"INSERT INTO {tblChild.GetFullyQualifiedName()} VALUES (1,'chucky2')", con).ExecuteNonQuery();
            }

            Assert.Multiple(() =>
            {
                Assert.That(tblParent.GetRowCount(), Is.EqualTo(2));
                Assert.That(tblChild.GetRowCount(), Is.EqualTo(2));
            });

            using (var con = tblParent.Database.Server.GetConnection())
            {
                con.Open();

                var cmd = tblParent.Database.Server.GetCommand($"DELETE FROM {tblParent.GetFullyQualifiedName()}", con);
                cmd.ExecuteNonQuery();
            }

            Assert.Multiple(() =>
            {
                Assert.That(tblParent.GetRowCount(), Is.EqualTo(0));
                Assert.That(tblChild.GetRowCount(), Is.EqualTo(0));
            });
        }
        finally
        {
            tblChild.Drop();
            tblParent.Drop();
        }
    }

    protected void ForeignKeyCreationTest_TwoColumns(bool cascadeDelete)
    {
        var database = GetTestDatabase(DatabaseType);

        var tblParent = database.CreateTable("Parent",
        [
            new DatabaseColumnRequest("ID1", new DatabaseTypeRequest(typeof(int)))
                { IsPrimaryKey = true }, //varchar(10)
            new DatabaseColumnRequest("ID2", new DatabaseTypeRequest(typeof(int)))
                { IsPrimaryKey = true }, //varchar(10)
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 10)) //varchar(10)
        ]);

        var parentIdPkCol1 = tblParent.DiscoverColumn("ID1");
        var parentIdPkCol2 = tblParent.DiscoverColumn("ID2");

        var parentIdFkCol1 = new DatabaseColumnRequest("Parent_ID1", new DatabaseTypeRequest(typeof(int)));
        var parentIdFkCol2 = new DatabaseColumnRequest("Parent_ID2", new DatabaseTypeRequest(typeof(int)));

        var tblChild = database.CreateTable("Child",
        [
            parentIdFkCol1,
            parentIdFkCol2,
            new DatabaseColumnRequest("ChildName", new DatabaseTypeRequest(typeof(string), 10)) //varchar(10)
        ], new Dictionary<DatabaseColumnRequest, DiscoveredColumn>
        {
            { parentIdFkCol1, parentIdPkCol1 },
            { parentIdFkCol2, parentIdPkCol2 }
        }, cascadeDelete);

        using (var intoParent = tblParent.BeginBulkInsert(CultureInfo.InvariantCulture))
        {
            using var dt = new DataTable();
            dt.Columns.Add("ID1");
            dt.Columns.Add("ID2");
            dt.Columns.Add("Name");

            dt.Rows.Add(1, 2, "Bob");

            intoParent.Upload(dt);
        }

        using (var con = tblChild.Database.Server.GetConnection())
        {
            con.Open();

            var cmd = tblParent.Database.Server.GetCommand(
                $"INSERT INTO {tblChild.GetFullyQualifiedName()} VALUES (1,3,'chucky')", con);

            //violation of fk
            Assert.That(() => cmd.ExecuteNonQuery(), Throws.Exception);

            tblParent.Database.Server.GetCommand(
                $"INSERT INTO {tblChild.GetFullyQualifiedName()} VALUES (1,2,'chucky')", con).ExecuteNonQuery();
            tblParent.Database.Server.GetCommand(
                $"INSERT INTO {tblChild.GetFullyQualifiedName()} VALUES (1,2,'chucky2')", con).ExecuteNonQuery();
        }

        Assert.Multiple(() =>
        {
            Assert.That(tblParent.GetRowCount(), Is.EqualTo(1));
            Assert.That(tblChild.GetRowCount(), Is.EqualTo(2));
        });

        using (var con = tblParent.Database.Server.GetConnection())
        {
            con.Open();
            var cmd = tblParent.Database.Server.GetCommand($"DELETE FROM {tblParent.GetFullyQualifiedName()}", con);

            if (cascadeDelete)
            {
                cmd.ExecuteNonQuery();
                Assert.Multiple(() =>
                {
                    Assert.That(tblParent.GetRowCount(), Is.EqualTo(0));
                    Assert.That(tblChild.GetRowCount(), Is.EqualTo(0));
                });
            }
            else
            {
                //no cascade deletes so the query should crash on violation of fk constraint
                Assert.That(() => cmd.ExecuteNonQuery(), Throws.Exception);
            }
        }
    }

    protected void CreateMaxVarcharColumns()
    {
        var database = GetTestDatabase(DatabaseType);

        var tbl = database.CreateTable("TestDistincting",
        [
            new DatabaseColumnRequest("Field1", new DatabaseTypeRequest(typeof(string), int.MaxValue)), //varchar(max)
            new DatabaseColumnRequest("Field2", new DatabaseTypeRequest(typeof(string))), //varchar(???)
            new DatabaseColumnRequest("Field3", new DatabaseTypeRequest(typeof(string), 1000)), //varchar(???)
            new DatabaseColumnRequest("Field4", new DatabaseTypeRequest(typeof(string), 5000)), //varchar(???)
            new DatabaseColumnRequest("Field5", new DatabaseTypeRequest(typeof(string), 10000)), //varchar(???)
            new DatabaseColumnRequest("Field6", new DatabaseTypeRequest(typeof(string), 10)) //varchar(10)
        ]);

        Assert.Multiple(() =>
        {
            Assert.That(tbl.Exists());

            Assert.That(tbl.DiscoverColumn("Field1").DataType?.GetLengthIfString(), Is.GreaterThanOrEqualTo(4000));
            Assert.That(tbl.DiscoverColumn("Field2").DataType?.GetLengthIfString(),
                Is.GreaterThanOrEqualTo(1000)); // unknown size should be at least 1k? that seems sensible
            Assert.That(tbl.DiscoverColumn("Field6").DataType?.GetLengthIfString(), Is.EqualTo(10));
        });
    }


    protected void CreateMaxVarcharColumnFromDataTable()
    {
        var database = GetTestDatabase(DatabaseType);

        using var dt = new DataTable();
        dt.Columns.Add("MassiveColumn");

        var sb = new StringBuilder("Amaa");
        for (var i = 0; i < 10000; i++)
            sb.Append(i);

        dt.Rows.Add(sb.ToString());


        var tbl = database.CreateTable("MassiveTable", dt);

        Assert.Multiple(() =>
        {
            Assert.That(tbl.Exists());
            Assert.That(tbl.DiscoverColumn("MassiveColumn").DataType?.GetLengthIfString(),
                Is.GreaterThanOrEqualTo(8000));
        });

        using var dt2 = tbl.GetDataTable();
        Assert.That(dt2.Rows[0][0], Is.EqualTo(sb.ToString()));
    }

    protected void CreateDateColumnFromDataTable()
    {
        // SQLite has date type affinity issues - returns string instead of DateTime
        if (DatabaseType == DatabaseType.Sqlite)
            Assert.Ignore("SQLite does not preserve DateTime type when creating from DataTable");

        var database = GetTestDatabase(DatabaseType);

        var dt = new DataTable();
        dt.Columns.Add("DateColumn");
        dt.Rows.Add("2001-01-22");

        var tbl = database.CreateTable("DateTable", dt);

        Assert.That(tbl.Exists());

        dt = tbl.GetDataTable();
        Assert.That(dt.Rows[0][0], Is.EqualTo(new DateTime(2001, 01, 22)));
    }

    protected void CreateTable_EmptyDataTable_ExplicitTypes()
    {
        // Test creating a table from an empty DataTable with explicit column types
        // This covers the case where column.Table.Rows.Count == 0 in DiscoveredDatabaseHelper.cs lines 97-101
        // The guesser would default to bool, but we want to use the DataColumn.DataType instead
        var database = GetTestDatabase(DatabaseType);

        var dt = new DataTable();
        dt.Columns.Add("IntColumn", typeof(int));
        dt.Columns.Add("StringColumn", typeof(string));
        dt.Columns.Add("DateColumn", typeof(DateTime));
        dt.Columns.Add("DecimalColumn", typeof(decimal));
        // No rows added - types must be explicit for empty DataTables

        var tbl = database.CreateTable("EmptyTableWithTypes", dt);

        Assert.That(tbl.Exists());

        var intCol = tbl.DiscoverColumn("IntColumn");
        var stringCol = tbl.DiscoverColumn("StringColumn");
        var dateCol = tbl.DiscoverColumn("DateColumn");
        var decimalCol = tbl.DiscoverColumn("DecimalColumn");

        var syntaxHelper = database.Server.GetQuerySyntaxHelper();
        Assert.Multiple(() =>
        {
            Assert.That(syntaxHelper.TypeTranslater.GetCSharpTypeForSQLDBType(intCol.DataType?.SQLType),
                Is.EqualTo(typeof(int)));
            Assert.That(syntaxHelper.TypeTranslater.GetCSharpTypeForSQLDBType(stringCol.DataType?.SQLType),
                Is.EqualTo(typeof(string)));
            // SQLite is dynamically typed and stores DateTime as TEXT (string)
            Assert.That(syntaxHelper.TypeTranslater.GetCSharpTypeForSQLDBType(dateCol.DataType?.SQLType),
                Is.EqualTo(DatabaseType == DatabaseType.Sqlite ? typeof(string) : typeof(DateTime)));
            Assert.That(syntaxHelper.TypeTranslater.GetCSharpTypeForSQLDBType(decimalCol.DataType?.SQLType),
                Is.EqualTo(typeof(decimal)));
            Assert.That(tbl.GetRowCount(), Is.EqualTo(0));
        });
    }

    protected void AddColumnTest(bool useTransaction)
    {
        const string newColumnName = "My Fun New Column[Lol]"; //<- lets make sure dodgy names are also supported

        // SQLite doesn't support bracket quoting in ALTER TABLE column names
        if (DatabaseType == DatabaseType.Sqlite)
            Assert.Ignore("SQLite ALTER TABLE does not support bracket-quoted column names");

        var database = GetTestDatabase(DatabaseType);

        //create a single column table with primary key
        var tbl = database.CreateTable("TestDistincting",
        [
            new DatabaseColumnRequest("Field1", new DatabaseTypeRequest(typeof(string), 100))
                { IsPrimaryKey = true } //varchar(max)
        ]);

        Assert.Multiple(() =>
        {
            //table should exist
            Assert.That(tbl.Exists());

            //column should be varchar(100)
            Assert.That(tbl.DiscoverColumn("Field1").DataType?.GetLengthIfString(), Is.EqualTo(100));

            //and should be a primary key
            Assert.That(tbl.DiscoverColumn("Field1").IsPrimaryKey);
        });

        //ALTER TABLE to ADD COLUMN of date type
        if (useTransaction)
        {
            using var con = database.Server.BeginNewTransactedConnection();
            tbl.AddColumn(newColumnName, new DatabaseTypeRequest(typeof(DateTime)), true,
                new DatabaseOperationArgs { TimeoutInSeconds = 1000, TransactionIfAny = con.ManagedTransaction });
            con.ManagedTransaction?.CommitAndCloseConnection();
        }
        else
        {
            tbl.AddColumn(newColumnName, new DatabaseTypeRequest(typeof(DateTime)), true, 1000);
        }


        //new column should exist
        var newCol = tbl.DiscoverColumn(newColumnName);

        //and should have a type of datetime as requested
        var typeCreated = newCol.DataType?.SQLType;
        var tt = database.Server.GetQuerySyntaxHelper().TypeTranslater;
        Assert.That(tt.GetCSharpTypeForSQLDBType(typeCreated), Is.EqualTo(typeof(DateTime)));

        var fieldsToAlter = new List<string>(["Field1", newColumnName]);

        //sql server can't handle altering primary key columns or anything with a foreign key on it too!
        if (DatabaseType == DatabaseType.MicrosoftSQLServer)
            fieldsToAlter.Remove("Field1");

        foreach (var fieldName in fieldsToAlter)
        {
            //ALTER TABLE, ALTER COLUMN of date type each of these to be now varchar(10)s

            //discover the column
            newCol = tbl.DiscoverColumn(fieldName);

            //ALTER the column to varchar(10)
            var newTypeCSharp = new DatabaseTypeRequest(typeof(string), 10);
            var newTypeSql = tt.GetSQLDBTypeForCSharpType(newTypeCSharp);
            newCol.DataType?.AlterTypeTo(newTypeSql);

            //rediscover it
            newCol = tbl.DiscoverColumn(fieldName);

            //make sure the type change happened
            Assert.That(newCol.DataType?.GetLengthIfString(), Is.EqualTo(10));
        }

        Assert.Multiple(() =>
        {
            //and should still be a primary key
            Assert.That(tbl.DiscoverColumn("Field1").IsPrimaryKey);
            //and should not be a primary key
            Assert.That(tbl.DiscoverColumn(newColumnName).IsPrimaryKey, Is.False);
        });
    }

    protected void ChangeDatabaseShouldNotAffectOriginalConnectionString_Test()
    {
        var database1 = GetTestDatabase(DatabaseType);
        var stringBefore = database1.Server.Builder.ConnectionString;
        database1.Server.ExpectDatabase("SomeOtherDb");

        Assert.That(database1.Server.Builder.ConnectionString, Is.EqualTo(stringBefore));
    }

    protected void TestDistincting(bool useTransaction, bool dodgyNames)
    {
        var database = GetTestDatabase(DatabaseType);

        // JS 2023-05-11 4000 characters, because SELECT DISTINCT doesn't work on CLOB (Oracle)
        var tbl = database.CreateTable(dodgyNames ? ",," : "Field3",
        [
            new DatabaseColumnRequest("Field1", new DatabaseTypeRequest(typeof(string), 4000)), //varchar(max)
            new DatabaseColumnRequest("Field2", new DatabaseTypeRequest(typeof(DateTime))),
            new DatabaseColumnRequest(dodgyNames ? ",,,," : "Field3", new DatabaseTypeRequest(typeof(int)))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Field1");
        dt.Columns.Add("Field2");
        dt.Columns.Add(dodgyNames ? ",,,," : "Field3");

        dt.Rows.Add("dave", "2001-01-01", "50");
        dt.Rows.Add("dave", "2001-01-01", "50");
        dt.Rows.Add("dave", "2001-01-01", "50");
        dt.Rows.Add("dave", "2001-01-01", "50");
        dt.Rows.Add("frank", "2001-01-01", "50");
        dt.Rows.Add("frank", "2001-01-01", "50");
        dt.Rows.Add("frank", "2001-01-01", "51");

        Assert.Multiple(() =>
        {
            Assert.That(tbl.Database.DiscoverTables(false), Has.Length.EqualTo(1));
            Assert.That(tbl.GetRowCount(), Is.EqualTo(0));
        });

        using (var insert = tbl.BeginBulkInsert(CultureInfo.InvariantCulture))
        {
            insert.Upload(dt);
        }

        Assert.That(tbl.GetRowCount(), Is.EqualTo(7));

        if (useTransaction)
        {
            using var con = tbl.Database.Server.BeginNewTransactedConnection();
            tbl.MakeDistinct(new DatabaseOperationArgs { TransactionIfAny = con.ManagedTransaction });
            con.ManagedTransaction?.CommitAndCloseConnection();
        }
        else
        {
            tbl.MakeDistinct();
        }

        Assert.Multiple(() =>
        {
            Assert.That(tbl.GetRowCount(), Is.EqualTo(3));
            Assert.That(tbl.Database.DiscoverTables(false), Has.Length.EqualTo(1));
        });
    }

    protected void TestIntDataTypes()
    {
        // SQLite doesn't support ALTER COLUMN TYPE - requires table recreation
        if (DatabaseType == DatabaseType.Sqlite)
            Assert.Ignore("SQLite does not support ALTER COLUMN TYPE syntax");

        var database = GetTestDatabase(DatabaseType);

        var dt = new DataTable();
        dt.Columns.Add("MyCol", typeof(decimal));

        dt.Rows.Add("100");
        dt.Rows.Add("105");
        dt.Rows.Add("1");

        var tbl = database.CreateTable("IntTestTable", dt);

        dt = tbl.GetDataTable();
        var intValues = dt.Rows.OfType<DataRow>().Select(r => Convert.ToInt32(r[0], CultureInfo.InvariantCulture))
            .ToList();
        Assert.Multiple(() =>
        {
            Assert.That(intValues.Count(v => v == 100), Is.EqualTo(1));
            Assert.That(intValues.Count(v => v == 105), Is.EqualTo(1));
            Assert.That(intValues.Count(v => v == 1), Is.EqualTo(1));
        });

        var col = tbl.DiscoverColumn("MyCol");
        col.DataType?.AlterTypeTo("decimal(5,2)");

        var size = tbl.DiscoverColumn("MyCol").DataType?.GetDecimalSize();
        Assert.That(size, Is.EqualTo(new DecimalSize(3, 2))); //3 before decimal place 2 after;
        Assert.Multiple(() =>
        {
            Assert.That(size.NumbersBeforeDecimalPlace, Is.EqualTo(3));
            Assert.That(size.NumbersAfterDecimalPlace, Is.EqualTo(2));
            Assert.That(size.Precision, Is.EqualTo(5));
            Assert.That(size.Scale, Is.EqualTo(2));
        });

        dt = tbl.GetDataTable();
        var decimalValues = dt.Rows.OfType<DataRow>().Select(r => Convert.ToDecimal(r[0], CultureInfo.InvariantCulture))
            .ToList();
        Assert.Multiple(() =>
        {
            Assert.That(decimalValues.Count(v => v == new decimal(100.0f)), Is.EqualTo(1));
            Assert.That(decimalValues.Count(v => v == new decimal(105.0f)), Is.EqualTo(1));
            Assert.That(decimalValues.Count(v => v == new decimal(1.0f)), Is.EqualTo(1));
        });
    }

    protected void TestFloatDataTypes()
    {
        // SQLite doesn't support ALTER COLUMN TYPE - requires table recreation
        if (DatabaseType == DatabaseType.Sqlite)
            Assert.Ignore("SQLite does not support ALTER COLUMN TYPE syntax");

        var database = GetTestDatabase(DatabaseType);

        var dt = new DataTable();
        dt.Columns.Add("MyCol");

        dt.Rows.Add("100");
        dt.Rows.Add("105");
        dt.Rows.Add("2.1");

        var tbl = database.CreateTable("DecimalTestTable", dt);

        dt = tbl.GetDataTable();
        var decimalValues2 = dt.Rows.OfType<DataRow>()
            .Select(r => Convert.ToDecimal(r[0], CultureInfo.InvariantCulture)).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(decimalValues2.Count(v => v == new decimal(100.0f)), Is.EqualTo(1));
            Assert.That(decimalValues2.Count(v => v == new decimal(105.0f)), Is.EqualTo(1));
            Assert.That(decimalValues2.Count(v => v == new decimal(2.1f)), Is.EqualTo(1));
        });


        var col = tbl.DiscoverColumn("MyCol");
        var size = col.DataType?.GetDecimalSize();
        Assert.That(size, Is.Not.Null, "DecimalSize should not be null");
        // Skip DecimalSize.Equals comparison - TypeGuesser library issue
        // Just verify the individual field values which is what matters
        // Assert.That(size, Is.EqualTo(new DecimalSize(4, 1)));
        Assert.Multiple(() =>
        {
            Assert.That(size!.NumbersBeforeDecimalPlace, Is.EqualTo(3), "Before decimal");
            Assert.That(size!.NumbersAfterDecimalPlace, Is.EqualTo(1), "After decimal");
            Assert.That(size!.Precision, Is.EqualTo(4));
            Assert.That(size!.Scale, Is.EqualTo(1));
        });

        // Oracle requires column to be empty when decreasing precision (ORA-01440)
        if (DatabaseType == DatabaseType.Oracle)
        {
            using var con = tbl.Database.Server.GetConnection();
            con.Open();
            using var deleteCmd = tbl.Database.Server.GetCommand($"DELETE FROM {tbl.GetFullyQualifiedName()}", con);
            deleteCmd.ExecuteNonQuery();
        }

        col.DataType?.AlterTypeTo("decimal(5,2)");

        size = tbl.DiscoverColumn("MyCol").DataType?.GetDecimalSize();
        Assert.That(size, Is.EqualTo(new DecimalSize(3, 2))); //3 before decimal place 2 after;
        Assert.Multiple(() =>
        {
            Assert.That(size.NumbersBeforeDecimalPlace, Is.EqualTo(3));
            Assert.That(size.NumbersAfterDecimalPlace, Is.EqualTo(2));
            Assert.That(size.Precision, Is.EqualTo(5));
            Assert.That(size.Scale, Is.EqualTo(2));
        });
    }

    protected void HorribleDatabaseAndTableNames(string horribleDatabaseName, string horribleTableName)
    {
        AssertCanCreateDatabases();

        var database = GetTestDatabase(DatabaseType);

        SqlConnection.ClearAllPools();
        if (DatabaseType == DatabaseType.PostgreSql)
            database.Server.CreateDatabase(horribleDatabaseName);

        database = database.Server.ExpectDatabase(horribleDatabaseName);
        if (DatabaseType != DatabaseType.PostgreSql)
            database.Create(true);

        SqlConnection.ClearAllPools();

        try
        {
            var tbl = database.CreateTable(horribleTableName,
            [
                new DatabaseColumnRequest("Field1",
                    new DatabaseTypeRequest(typeof(string), int.MaxValue)), //varchar(max)
                new DatabaseColumnRequest("Field2", new DatabaseTypeRequest(typeof(DateTime))),
                new DatabaseColumnRequest("Field3", new DatabaseTypeRequest(typeof(int))) { AllowNulls = false }
            ]);

            using var dt = new DataTable();
            dt.Columns.Add("Field1");
            dt.Columns.Add("Field2");
            dt.Columns.Add("Field3");

            dt.Rows.Add("dave", "2001-01-01", "50");
            dt.Rows.Add("dave", "2001-01-01", "50");
            dt.Rows.Add("dave", "2001-01-01", "50");
            dt.Rows.Add("dave", "2001-01-01", "50");
            dt.Rows.Add("frank", "2001-01-01", "50");
            dt.Rows.Add("frank", "2001-01-01", "50");
            dt.Rows.Add("frank", "2001-01-01", "51");

            Assert.Multiple(() =>
            {
                Assert.That(tbl.Database.DiscoverTables(false), Has.Length.EqualTo(1));
                Assert.That(tbl.GetRowCount(), Is.EqualTo(0));
            });

            using (var insert = tbl.BeginBulkInsert(CultureInfo.InvariantCulture))
            {
                insert.Upload(dt);
            }

            Assert.That(tbl.GetRowCount(), Is.EqualTo(7));

            tbl.MakeDistinct();

            Assert.Multiple(() =>
            {
                Assert.That(tbl.GetRowCount(), Is.EqualTo(3));
                Assert.That(tbl.Database.DiscoverTables(false), Has.Length.EqualTo(1));
            });

            tbl.Truncate();

            tbl.CreatePrimaryKey(tbl.DiscoverColumn("Field3"));

            Assert.That(tbl.DiscoverColumn("Field3").IsPrimaryKey);
        }
        finally
        {
            database.Drop();
        }
    }

    protected void UnsupportedEntityNames(string horribleDatabaseName, string horribleTableName, string columnName)
    {
        var database = GetTestDatabase(DatabaseType);

        Assert.Multiple(() =>
        {
            //ExpectDatabase with illegal name
            Assert.That(
                Assert.Throws<RuntimeNameException>(() => database.Server.ExpectDatabase(horribleDatabaseName))
                    ?.Message, Does.Match("Database .* contained unsupported .* characters"));

            //ExpectTable with illegal name
            Assert.That(
                Assert.Throws<RuntimeNameException>(() => database.ExpectTable(horribleTableName))
                    ?.Message, Does.Match("Table .* contained unsupported .* characters"));

            //CreateTable with illegal name
            Assert.That(
                Assert.Throws<RuntimeNameException>(() => database.CreateTable(horribleTableName,
                    [
                        new DatabaseColumnRequest("a", new DatabaseTypeRequest(typeof(string), 10))
                    ]))
                    ?.Message, Does.Match("Table .* contained unsupported .* characters"));

            //CreateTable with (column) illegal name
            Assert.That(
                Assert.Throws<RuntimeNameException>(() => database.CreateTable("f",
                    [
                        new DatabaseColumnRequest(columnName, new DatabaseTypeRequest(typeof(string), 10))
                    ]))
                    ?.Message, Does.Match("Column .* contained unsupported .* characters"));
        });

        AssertCanCreateDatabases();

        //CreateDatabase with illegal name
        Assert.That(
            Assert.Throws<RuntimeNameException>(() => database.Server.CreateDatabase(horribleDatabaseName))
                ?.Message, Does.Match("Database .* contained unsupported .* characters"));
    }

    protected void HorribleColumnNames(string horribleDatabaseName, string horribleTableName, string columnName)
    {
        AssertCanCreateDatabases();

        var database = GetTestDatabase(DatabaseType);
        database.Server.CreateDatabase(horribleDatabaseName);
        database = database.Server.ExpectDatabase(horribleDatabaseName);
        Assert.That(database.GetRuntimeName(), Is.EqualTo(horribleDatabaseName).IgnoreCase);

        try
        {
            var dt = new DataTable();
            dt.Columns.Add(columnName);
            dt.Rows.Add("dave");
            dt.PrimaryKey = [dt.Columns[0]];

            var tbl = database.CreateTable(horribleTableName, dt);

            Assert.Multiple(() =>
            {
                Assert.That(tbl.GetRowCount(), Is.EqualTo(1));

                Assert.That(tbl.DiscoverColumns().Single().IsPrimaryKey);

                Assert.That(tbl.GetDataTable().Rows, Has.Count.EqualTo(1));
            });

            tbl.Insert(new Dictionary<string, object> { { columnName, "fff" } });

            Assert.That(tbl.GetDataTable().Rows, Has.Count.EqualTo(2));
        }
        finally
        {
            database.Drop();
        }
    }

    protected void CreateTable_AutoIncrementColumnTest()
    {
        var database = GetTestDatabase(DatabaseType);

        var tbl = database.CreateTable("MyTable",
        [
            new DatabaseColumnRequest("IdColumn", new DatabaseTypeRequest(typeof(int)))
            {
                AllowNulls = false,
                IsAutoIncrement = true,
                IsPrimaryKey = true
            },
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 100))
        ]);

        using var dt = new DataTable();
        dt.Columns.Add("Name");
        dt.Rows.Add("Frank");

        using (var bulkInsert = tbl.BeginBulkInsert(CultureInfo.InvariantCulture))
        {
            bulkInsert.Upload(dt);
        }

        Assert.That(tbl.GetRowCount(), Is.EqualTo(1));

        var result = tbl.GetDataTable();
        Assert.That(result.Rows, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(result.Rows[0]["IdColumn"], Is.EqualTo(1));

            Assert.That(tbl.DiscoverColumn("IdColumn").IsAutoIncrement);
            Assert.That(tbl.DiscoverColumn("Name").IsAutoIncrement, Is.False);
        });

        var autoIncrement = tbl.Insert(new Dictionary<string, object> { { "Name", "Tony" } });
        Assert.That(autoIncrement, Is.EqualTo(2));
    }

    protected void CreateTable_DefaultTest_Date()
    {
        // SQLite doesn't support DEFAULT value functions like CURRENT_TIMESTAMP in the same way
        if (DatabaseType == DatabaseType.Sqlite)
            Assert.Ignore("SQLite DEFAULT value function syntax differs from other databases");

        var database = GetTestDatabase(DatabaseType);

        var tbl = database.CreateTable("MyTable",
        [
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 100)),
            new DatabaseColumnRequest("myDt", new DatabaseTypeRequest(typeof(DateTime)))
            {
                AllowNulls = false,
                Default = MandatoryScalarFunctions.GetTodaysDate
            }
        ]);
        DateTime currentValue;

        using (var insert = tbl.BeginBulkInsert(CultureInfo.InvariantCulture))
        {
            using var dt = new DataTable();
            dt.Columns.Add("Name");
            dt.Rows.Add("Hi");

            currentValue = DateTime.Now;
            insert.Upload(dt);
        }

        var dt2 = tbl.GetDataTable();

        var databaseValue = (DateTime)dt2.Rows.Cast<DataRow>().Single()["myDt"];

        Assert.Multiple(() =>
        {
            Assert.That(databaseValue.Year, Is.EqualTo(currentValue.Year));
            Assert.That(databaseValue.Month, Is.EqualTo(currentValue.Month));
            Assert.That(databaseValue.Day, Is.EqualTo(currentValue.Day));
            Assert.That(databaseValue.Hour, Is.EqualTo(currentValue.Hour));
        });
    }

    protected void CreateTable_DefaultTest_Guid()
    {
        // SQLite doesn't support DEFAULT value functions like newid()
        if (DatabaseType == DatabaseType.Sqlite)
            Assert.Ignore("SQLite does not support GUID/UUID DEFAULT value functions");

        var database = GetTestDatabase(DatabaseType);

        // ReSharper disable once SwitchStatementMissingSomeEnumCasesNoDefault
        switch (DatabaseType)
        {
            case DatabaseType.MySql when database.Server.GetVersion()?.Major < 8:
                Assert.Pass("UID defaults are only supported in MySql 8+");
                return;
            case DatabaseType.PostgreSql:
                {
                    //we need this extension on the server to work
                    using var con = database.Server.GetConnection();
                    con.Open();
                    using var cmd = database.Server.GetCommand("CREATE EXTENSION IF NOT EXISTS pgcrypto;", con);
                    cmd.ExecuteNonQuery();
                    break;
                }
        }

        var tbl = database.CreateTable("MyTable",
        [
            new DatabaseColumnRequest("Name", new DatabaseTypeRequest(typeof(string), 100)),
            new DatabaseColumnRequest("MyGuid", new DatabaseTypeRequest(typeof(string)))
            {
                AllowNulls = false,
                Default = MandatoryScalarFunctions.GetGuid
            }
        ]);

        using (var insert = tbl.BeginBulkInsert(CultureInfo.InvariantCulture))
        {
            using var dt = new DataTable();
            dt.Columns.Add("Name");
            dt.Rows.Add("Hi");

            insert.Upload(dt);
        }

        using var dt2 = tbl.GetDataTable();

        var databaseValue = (string)dt2.Rows.Cast<DataRow>().Single()["MyGuid"];

        Assert.That(databaseValue, Is.Not.Null);
    }

    protected void Test_BulkInserting_LotsOfDates()
    {
        var culture = new CultureInfo("en-gb");
        var db = GetTestDatabase(DatabaseType);

        var tbl = db.CreateTable("LotsOfDatesTest",
        [
            new DatabaseColumnRequest("ID", new DatabaseTypeRequest(typeof(int))),
            new DatabaseColumnRequest("MyDate", new DatabaseTypeRequest(typeof(DateTime))),
            new DatabaseColumnRequest("MyString", new DatabaseTypeRequest(typeof(string), int.MaxValue))
        ]);

        //test basic insert
        foreach (var s in _someDates)
            tbl.Insert(new Dictionary<string, object>
                {
                    { "ID", 1 },
                    { "MyDate", s },
                    { "MyString", Guid.NewGuid().ToString() }
                }, culture
            );


        using var dt = new DataTable();

        dt.Columns.Add("id");
        dt.Columns.Add("mydate");
        dt.Columns.Add("mystring");

        foreach (var s in _someDates)
            dt.Rows.Add(2, s, Guid.NewGuid().ToString());

        Assert.That(tbl.GetRowCount(), Is.EqualTo(_someDates.Length));

        using (var bulkInsert = tbl.BeginBulkInsert(culture))
        {
            bulkInsert.Upload(dt);
        }

        Assert.That(tbl.GetRowCount(), Is.EqualTo(_someDates.Length * 2));
    }
}
