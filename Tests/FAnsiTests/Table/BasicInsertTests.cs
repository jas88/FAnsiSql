using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using System.Collections.Generic;
using TypeGuesser;

namespace FAnsiTests.Table;

internal sealed class BasicInsertTests : DatabaseTests
{
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, "Dave")]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, "Dave")]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, "Dave")]
#endif
#if POSTGRESQL_TESTS
    [TestCase(DatabaseType.PostgreSql, "Dave")]
#endif

#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, @"].;\""ffff
[")]
#endif

#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, @"].;\""ffff
[")]
#endif

#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, @"].;\""ffff
[")]
#endif
#if POSTGRESQL_TESTS
    [TestCase(DatabaseType.PostgreSql, @"].;\""ffff
[")]
#endif
#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, 1.5)]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, 1.5)]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, 1.5)]
#endif
#if POSTGRESQL_TESTS
    [TestCase(DatabaseType.PostgreSql, 1.5)]
#endif
    public void CreateTableAndInsertAValue_ColumnOverload(DatabaseType type, object value)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("InsertTable",
            [
                new DatabaseColumnRequest("Name",new DatabaseTypeRequest(value.GetType(),100,new DecimalSize(5,5)))
            ]);

        var nameCol = tbl.DiscoverColumn("Name");

        tbl.Insert(new Dictionary<DiscoveredColumn, object>
        {
            {nameCol,value}
        });

        var result = tbl.GetDataTable();
        Assert.That(result.Rows, Has.Count.EqualTo(1));
        Assert.That(result.Rows[0][0], Is.EqualTo(value));

        tbl.Drop();
    }

#if MSSQL_TESTS
    [TestCase(DatabaseType.MicrosoftSQLServer, 1.5)]
#endif
#if MYSQL_TESTS
    [TestCase(DatabaseType.MySql, 1.5)]
#endif
#if ORACLE_TESTS
    [TestCase(DatabaseType.Oracle, 1.5)]
#endif
#if POSTGRESQL_TESTS
    [TestCase(DatabaseType.PostgreSql, 1.5)]
#endif
    public void CreateTableAndInsertAValue_StringOverload(DatabaseType type, object value)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("InsertTable",
            [
                new DatabaseColumnRequest("Name",new DatabaseTypeRequest(value.GetType(),100,new DecimalSize(5,5)))
            ]);

        tbl.Insert(new Dictionary<string, object>
        {
            {"Name",value}
        });

        var result = tbl.GetDataTable();
        Assert.That(result.Rows, Has.Count.EqualTo(1));
        Assert.That(result.Rows[0][0], Is.EqualTo(value));

        tbl.Drop();
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void CreateTableAndInsertAValue_ReturnsIdentity(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable("InsertTable",
            [
                new DatabaseColumnRequest("myidentity",new DatabaseTypeRequest(typeof(int))){IsPrimaryKey = true,IsAutoIncrement = true},
                new DatabaseColumnRequest("Name",new DatabaseTypeRequest(typeof(string),100))
            ]);

        var nameCol = tbl.DiscoverColumn("Name");

        var result = tbl.Insert(new Dictionary<DiscoveredColumn, object>
        {
            {nameCol,"fish"}
        });

        Assert.That(result, Is.EqualTo(1));


        result = tbl.Insert(new Dictionary<DiscoveredColumn, object>
        {
            {nameCol,"fish"}
        });

        Assert.That(result, Is.EqualTo(2));
    }
}
