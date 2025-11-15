using System;
using System.Collections.Generic;
using System.Linq;
using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.TypeTranslation;
using FAnsi.Exceptions;
using FAnsi.Implementation;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.TypeTranslation;

/// <summary>
/// <para>These tests cover the systems ability to match database provider specific data types to a C# Type.</para>
///
/// <para>This is further complicated since DBMS datatypes have aliases e.g. BOOL,BOOLEAN and tinyint(1) are all aliases
/// for the same thing in MySql (the DBMS will create a tinyint(1)).</para>
///
/// <para>These tests also create tables called TTT in the test database and test the systems ability to discover the column
/// and reverse engineer the original data type from the database.</para>
/// </summary>
public sealed class TypeTranslaterTests : DatabaseTests
{
    private readonly Dictionary<DatabaseType, ITypeTranslater> _translaters = [];

    [OneTimeSetUp]
    public void SetupDatabases()
    {
        foreach (var type in Enum.GetValues<DatabaseType>())
            try
            {
                var tt = ImplementationManager.GetImplementation(type).GetQuerySyntaxHelper().TypeTranslater;
                _translaters.Add(type, tt);
            }
            catch (ImplementationNotFoundException)
            {
                //no implementation for this Type
            }
    }

#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "varchar(10)")]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "varchar(10)")]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "varchar2(10)")]
#endif
#if POSTGRESQL_TESTS
    [TestCase(DatabaseType.PostgreSql, "varchar(10)")]
#endif
    public void Test_CSharpToDbType_String10(DatabaseType type, string expectedType)
    {
        var cSharpType = new DatabaseTypeRequest(typeof(string), 10);

        Assert.That(_translaters[type].GetSQLDBTypeForCSharpType(cSharpType), Is.EqualTo(expectedType));
    }

#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "varchar(max)")]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "longtext")]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "CLOB")]
#endif
#if POSTGRESQL_TESTS
    [TestCase(DatabaseType.PostgreSql, "text")]
#endif
    public void Test_CSharpToDbType_StringMax(DatabaseType type, string expectedType)
    {
        var cSharpType = new DatabaseTypeRequest(typeof(string), 10000000);

        Assert.Multiple(() =>
        {
            //Does a request for a max length string give the expected data type?
            Assert.That(_translaters[type].GetSQLDBTypeForCSharpType(cSharpType), Is.EqualTo(expectedType));

            //Does the TypeTranslater know that this datatype has no limit on characters?
            Assert.That(_translaters[type].GetLengthIfString(expectedType), Is.EqualTo(int.MaxValue));

            //And does the TypeTranslater know that this datatype is string
            Assert.That(_translaters[type].GetCSharpTypeForSQLDBType(expectedType), Is.EqualTo(typeof(string)));
        });
    }

#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "varchar(max)", false)]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "nvarchar(max)", true)]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "text", false)]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "ntext", true)]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "longtext", false)]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "CLOB", false)]
#endif
#if POSTGRESQL_TESTS
    [TestCase(DatabaseType.PostgreSql, "text", false)]
#endif
    public void Test_GetLengthIfString_VarcharMaxCols(DatabaseType type, string datatype, bool expectUnicode)
    {
        Assert.That(_translaters[type].GetLengthIfString(datatype), Is.EqualTo(int.MaxValue));
        var dbType = _translaters[type].GetDataTypeRequestForSQLDBType(datatype);

        Assert.Multiple(() =>
        {
            Assert.That(dbType.CSharpType, Is.EqualTo(typeof(string)));
            Assert.That(dbType.Width, Is.EqualTo(int.MaxValue));
            Assert.That(dbType.Unicode, Is.EqualTo(expectUnicode));
        });
    }

#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "bigint", typeof(long))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "binary", typeof(byte[]))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "bit", typeof(bool))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "char", typeof(string))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "date", typeof(DateTime))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "datetime", typeof(DateTime))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "datetime2", typeof(DateTime))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "datetimeoffset", typeof(DateTime))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "decimal", typeof(decimal))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "varbinary(max)", typeof(byte[]))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "float", typeof(decimal))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "image", typeof(byte[]))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "int", typeof(int))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "money", typeof(decimal))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "nchar", typeof(string))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "ntext", typeof(string))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "numeric", typeof(decimal))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "nvarchar", typeof(string))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "real", typeof(decimal))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "rowversion", typeof(byte[]))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "smalldatetime", typeof(DateTime))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "smallint", typeof(short))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "smallmoney", typeof(decimal))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "text", typeof(string))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "time", typeof(TimeSpan))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "timestamp", typeof(byte[]))] //yup thats right: https://stackoverflow.com/questions/7105093/difference-between-datetime-and-timestamp-in-sqlserver
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "tinyint", typeof(byte))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "uniqueidentifier", typeof(Guid))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "varbinary", typeof(byte[]))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "varchar", typeof(string))]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "xml", typeof(string))]
#endif

#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "BOOL", typeof(bool))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "BOOLEAN", typeof(bool))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "TINYINT", typeof(byte))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "CHARACTER VARYING(10)", typeof(string))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "FIXED", typeof(decimal))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "DEC", typeof(decimal))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "VARCHAR(10)", typeof(string))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "DECIMAL", typeof(decimal))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "FLOAT4", typeof(decimal))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "FLOAT", typeof(decimal))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "FLOAT8", typeof(decimal))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "DOUBLE", typeof(decimal))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "INT1", typeof(byte))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "INT2", typeof(short))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "INT3", typeof(int))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "INT4", typeof(int))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "INT8", typeof(long))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "INT(1)", typeof(int))] //Confusing but these are actually display names https://stackoverflow.com/questions/11563830/what-does-int1-stand-for-in-mysql
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "INT(2)", typeof(int))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "INT(3)", typeof(int))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "INT(4)", typeof(int))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "INT(8)", typeof(int))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "SMALLINT", typeof(short))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "MEDIUMINT", typeof(int))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "INT", typeof(int))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "BIGINT", typeof(long))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "LONG VARBINARY", typeof(byte[]))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "MEDIUMBLOB", typeof(byte[]))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "LONG VARCHAR", typeof(string))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "MEDIUMTEXT", typeof(string))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "LONG", typeof(string))] //yes in MySql LONG is text (https://dev.mysql.com/doc/refman/8.0/en/other-vendor-data-types.html)
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "MIDDLEINT", typeof(int))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "NUMERIC", typeof(decimal))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "INTEGER", typeof(int))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "BIT", typeof(bool))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "SMALLINT(3)", typeof(short))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "INT UNSIGNED", typeof(int))] //we don't distinguish between uint and int currently
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "INT UNSIGNED ZEROFILL", typeof(int))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "SMALLINT UNSIGNED", typeof(short))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "SMALLINT ZEROFILL UNSIGNED", typeof(short))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "LONGTEXT", typeof(string))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "CHAR(10)", typeof(string))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "TEXT", typeof(string))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "BLOB", typeof(byte[]))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "ENUM('fish','carrot')", typeof(string))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "SET('fish','carrot')", typeof(string))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "VARBINARY(10)", typeof(byte[]))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "date", typeof(DateTime))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "datetime", typeof(DateTime))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "TIMESTAMP", typeof(DateTime))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "TIME", typeof(TimeSpan))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "nchar", typeof(string))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "nvarchar(10)", typeof(string))]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "real", typeof(decimal))]
#endif

#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "varchar2(10)", typeof(string))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "CHAR(10)", typeof(string))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "CHAR", typeof(string))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "nchar", typeof(string))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "nvarchar2(1)", typeof(string))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "clob", typeof(string))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "nclob", typeof(string))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "long", typeof(string))]//yes in Oracle LONG is text (https://docs.oracle.com/cd/A58617_01/server.804/a58241/ch5.htm)
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "NUMBER", typeof(decimal))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "date", typeof(DateTime))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "BLOB", typeof(byte[]))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "BFILE", typeof(byte[]))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "RAW(100)", typeof(byte[]))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "LONG RAW", typeof(byte[]))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "ROWID", typeof(byte[]))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "CHARACTER", typeof(string))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "FLOAT", typeof(decimal))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "FLOAT(5)", typeof(decimal))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "REAL", typeof(decimal))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "DOUBLE PRECISION", typeof(decimal))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "CHARACTER VARYING(10)", typeof(string))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "CHAR VARYING(10)", typeof(string))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "LONG VARCHAR", typeof(string))]
#endif

    //[TestCase(DatabaseType.Oracle, "DECIMAL", typeof(decimal))] //GetBasicTypeFromOracleType makes this look like dcimal going in but Int32 comming out
    //[TestCase(DatabaseType.Oracle, "DEC", typeof(decimal))]

#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "DEC(3,2)", typeof(decimal))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "DEC(*,3)", typeof(decimal))]
#endif

    //These types are all converted to Number(38) by Oracle : https://docs.oracle.com/cd/A58617_01/server.804/a58241/ch5.htm (See ANSI/ISO, DB2, and SQL/DS Datatypes )
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "INTEGER", typeof(int))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "INT", typeof(int))]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "SMALLINT", typeof(int))] //yup, see the link above
#endif
    public void TestIsKnownType(DatabaseType databaseType, string sqlType, Type expectedType)
    {
        RunKnownTypeTest(databaseType, sqlType, expectedType);
    }

    private void RunKnownTypeTest(DatabaseType type, string sqlType, Type expectedType)
    {
        //Get test database
        var db = GetTestDatabase(type);
        var tt = db.Server.GetQuerySyntaxHelper().TypeTranslater;

        //Create it in database (crashes here if it's an invalid datatype according to DBMS)
        var tbl = db.CreateTable("TTT", [new DatabaseColumnRequest("MyCol", sqlType)]);

        try
        {
            //Find the column on the created table and fetch it
            var col = tbl.DiscoverColumns().Single();

            //What type does FAnsi think this is?
            var tBefore = tt.TryGetCSharpTypeForSQLDBType(sqlType);
            Assert.That(tBefore, Is.Not.Null, $"We asked to create a '{sqlType}', DBMS created a '{col.DataType?.SQLType}'.  FAnsi didn't recognise '{sqlType}' as a supported Type");

            //Does FAnsi understand the datatype that was actually created on the server (sometimes you specify something and it is an
            //alias for something else e.g. Oracle creates 'varchar2' when you ask for 'CHAR VARYING'
            var guesser = col.GetGuesser();
            var tAfter = guesser.Guess.CSharpType;

            Assert.Multiple(() =>
            {
                Assert.That(tAfter, Is.Not.Null);

                //was the Type REQUESTED correct according to the test case expectation
                Assert.That(tBefore, Is.EqualTo(expectedType), $"We asked to create a '{sqlType}', DBMS created a '{col.DataType?.SQLType}'.  FAnsi decided that '{sqlType}' is '{tBefore}' and that '{col.DataType?.SQLType}' is '{tAfter}'");

                //Was the Type CREATED matching the REQUESTED type (as far as FAnsi is concerned)
                Assert.That(tAfter, Is.EqualTo(tBefore), $"We asked to create a '{sqlType}', DBMS created a '{col.DataType?.SQLType}'.  FAnsi decided that '{sqlType}' is '{tBefore}' and that '{col.DataType?.SQLType}' is '{tAfter}'");
            });

            /*if (!string.Equals(col.DataType?.SQLType, sqlType, StringComparison.OrdinalIgnoreCase))
                TestContext.Out.WriteLine("{0} created a '{1}' when asked to create a '{2}'", type,
                    col.DataType?.SQLType, sqlType);*/
        }
        finally
        {
            tbl.Drop();
        }
    }

    //Data types not supported by FAnsi
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "GEOMETRY")]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "POINT")]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "LINESTRING")]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "POLYGON")]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "MULTIPOINT")]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "MULTILINESTRING")]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "MULTIPOLYGON")]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "GEOMETRYCOLLECTION")]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "sql_variant")]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "MLSLABEL")]
#endif
    public void TestNotSupportedTypes(DatabaseType type, string sqlType)
    {
        Assert.That(_translaters[type].IsSupportedSQLDBType(sqlType), Is.False);
    }
}
