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
public abstract class TypeTranslaterTests : DatabaseTests
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

    protected void Test_CSharpToDbType_String10(DatabaseType type, string expectedType)
    {
        var cSharpType = new DatabaseTypeRequest(typeof(string), 10);

        Assert.That(_translaters[type].GetSQLDBTypeForCSharpType(cSharpType), Is.EqualTo(expectedType));
    }

    protected void Test_CSharpToDbType_StringMax(DatabaseType type, string expectedType)
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

    protected void Test_GetLengthIfString_VarcharMaxCols(DatabaseType type, string datatype, bool expectUnicode)
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

    protected void TestIsKnownType(DatabaseType databaseType, string sqlType, Type expectedType)
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
    protected void TestNotSupportedTypes(DatabaseType type, string sqlType)
    {
        Assert.That(_translaters[type].IsSupportedSQLDBType(sqlType), Is.False);
    }
}
