using FAnsi;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using FAnsi.Implementations.PostgreSql;
using FAnsi.Implementations.Sqlite;
using NUnit.Framework;

namespace FAnsiTests.Query;

internal sealed class QuerySyntaxHelperTests
{
    [OneTimeSetUp]
    public void Setup()
    {
        // Explicit loading for tests (ModuleInitializer timing is unreliable in test runners)
#pragma warning disable CS0618 // Type or member is obsolete
        ImplementationManager.Load<MicrosoftSQLImplementation>();
        ImplementationManager.Load<MySqlImplementation>();
        ImplementationManager.Load<OracleImplementation>();
        ImplementationManager.Load<PostgreSqlImplementation>();
        ImplementationManager.Load<SqliteImplementation>();
#pragma warning restore CS0618 // Type or member is obsolete
    }


    //Oracle always uppers everything because... Oracle
    public void SyntaxHelperTest_GetRuntimeName(DatabaseType dbType, string expected, string forInput)
    {
        var syntaxHelper = ImplementationManager.GetImplementation(dbType).GetQuerySyntaxHelper();
        Assert.That(syntaxHelper.GetRuntimeName(forInput), Is.EqualTo(expected));
    }

    /// <summary>
    ///     Tests that no matter how many times you call EnsureWrapped or GetRuntimeName you always end up with the format that
    ///     matches the last method call
    /// </summary>
    /// <param name="dbType"></param>
    /// <param name="runtime"></param>
    /// <param name="wrapped"></param>
    public void SyntaxHelperTest_GetRuntimeName_MultipleCalls(DatabaseType dbType, string runtime, string wrapped)
    {
        // NOTE: Oracle does not support such shenanigans https://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements008.htm
        // "neither quoted nor unquoted identifiers can contain double quotation marks or the null character (\0)."

        var syntaxHelper = ImplementationManager.GetImplementation(dbType).GetQuerySyntaxHelper();

        var currentName = runtime;

        for (var i = 0; i < 10; i++)
            if (i % 2 == 0)
            {
                Assert.That(currentName, Is.EqualTo(runtime));
                currentName = syntaxHelper.EnsureWrapped(currentName);
                currentName = syntaxHelper.EnsureWrapped(currentName);
                currentName = syntaxHelper.EnsureWrapped(currentName);
            }
            else
            {
                Assert.That(currentName, Is.EqualTo(wrapped));
                currentName = syntaxHelper.GetRuntimeName(currentName);
                currentName = syntaxHelper.GetRuntimeName(currentName);
                currentName = syntaxHelper.GetRuntimeName(currentName);
            }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void EnsureWrapped_MultipleCalls(DatabaseType dbType)
    {
        var syntax = QuerySyntaxHelperFactory.Create(dbType);

        var once = syntax.EnsureWrapped("ff");
        var twice = syntax.EnsureWrapped(once);

        Assert.That(twice, Is.EqualTo(once));
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void SyntaxHelperTest_GetRuntimeName_Impossible(DatabaseType t)
    {
        var syntaxHelper = ImplementationManager.GetImplementation(t).GetQuerySyntaxHelper();

        // After removing IllegalNameChars validation, ALL databases now allow special characters in identifiers
        // "count(*)" is technically a valid unquoted identifier (though in practice you'd quote it)
        // Oracle uppercases unquoted identifiers, so we need case-insensitive comparison
        var expectedCount = t == DatabaseType.Oracle ? "COUNT(*)" : "count(*)";
        var expectedMethod = t == DatabaseType.Oracle
            ? "GETMYCOOLTHING(\"MAGIC FUN TIMES\")"
            : "GetMyCoolThing(\"Magic Fun Times\")";

        Assert.That(syntaxHelper.GetRuntimeName("count(*)"), Is.EqualTo(expectedCount));
        Assert.That(syntaxHelper.GetRuntimeName("dbo.GetMyCoolThing(\"Magic Fun Times\")"), Is.EqualTo(expectedMethod));

        Assert.Multiple(() =>
        {
            Assert.That(syntaxHelper.TryGetRuntimeName("count(*)", out var name1), Is.True);
            Assert.That(name1, Is.EqualTo(expectedCount));
            Assert.That(syntaxHelper.TryGetRuntimeName("dbo.GetMyCoolThing(\"Magic Fun Times\")", out var name2),
                Is.True);
            Assert.That(name2, Is.EqualTo(expectedMethod));
        });
    }

    [Test]
    public void SyntaxHelperTest_GetRuntimeName_Oracle()
    {
        var syntaxHelper = ImplementationManager.GetImplementation(DatabaseType.Oracle).GetQuerySyntaxHelper();
        Assert.Multiple(() =>
        {
            Assert.That(syntaxHelper.GetRuntimeName("count(*) as Frank"), Is.EqualTo("FRANK"));
            Assert.That(syntaxHelper.GetRuntimeName("count(cast(1 as int)) as Frank"), Is.EqualTo("FRANK"));
            Assert.That(syntaxHelper.GetRuntimeName("count(cast(1 as int)) as \"Frank\""), Is.EqualTo("FRANK"));
            Assert.That(syntaxHelper.GetRuntimeName("\"mydb\".\"mytbl\".\"mycol\" as Frank"), Is.EqualTo("FRANK"));
            Assert.That(syntaxHelper.GetRuntimeName("\"mydb\".\"mytbl\".\"mycol\""), Is.EqualTo("MYCOL"));
        });
    }

    /// <summary>
    ///     Tests that GetRuntimeName correctly handles dots INSIDE wrapped identifiers.
    ///     For example, `db`.`table`.`Column.Name` should return "Column.Name", not "Name".
    ///     See GitHub issue #75.
    /// </summary>
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void SyntaxHelperTest_GetRuntimeName_DotsInsideWrappedIdentifiers(DatabaseType dbType)
    {
        var syntaxHelper = ImplementationManager.GetImplementation(dbType).GetQuerySyntaxHelper();
        var open = syntaxHelper.OpenQualifier;
        var close = syntaxHelper.CloseQualifier;

        // Column name with a dot inside: `Column.Name` or [Column.Name] or "Column.Name"
        var columnWithDot = $"{open}Column.Name{close}";
        var fullyQualified = $"{open}db{close}.{open}table{close}.{columnWithDot}";

        // Should return "Column.Name", not "Name"
        var expected = dbType == DatabaseType.Oracle ? "COLUMN.NAME" : "Column.Name";
        Assert.That(syntaxHelper.GetRuntimeName(fullyQualified), Is.EqualTo(expected),
            $"GetRuntimeName should return the full column name including the dot inside the wrapped identifier");

        // Also test just the column itself
        Assert.That(syntaxHelper.GetRuntimeName(columnWithDot), Is.EqualTo(expected),
            "GetRuntimeName should handle a single wrapped identifier with a dot");

        // Test table.column with dot in column name
        var tableAndColumn = $"{open}table{close}.{columnWithDot}";
        Assert.That(syntaxHelper.GetRuntimeName(tableAndColumn), Is.EqualTo(expected),
            "GetRuntimeName should handle table.column where column has a dot");

        // Test a more complex name with multiple dots inside
        var complexName = $"{open}Version.1.0.Release{close}";
        var expectedComplex = dbType == DatabaseType.Oracle ? "VERSION.1.0.RELEASE" : "Version.1.0.Release";
        Assert.That(syntaxHelper.GetRuntimeName(complexName), Is.EqualTo(expectedComplex),
            "GetRuntimeName should handle multiple dots inside a wrapped identifier");
    }

    [TestCase("count(*) as Frank", "count(*)", "Frank")]
    [TestCase("count(cast(1 as int)) as Frank", "count(cast(1 as int))", "Frank")]
    [TestCase("[mydb].[mytbl].[mycol] as Frank", "[mydb].[mytbl].[mycol]", "Frank")]
    [TestCase("[mydb].[mytbl].[mycol] as [Frank]", "[mydb].[mytbl].[mycol]", "Frank")]
    [TestCase("[mydb].[mytbl].[mycol] as [Frank],", "[mydb].[mytbl].[mycol]", "Frank")]
    [TestCase("[mytbl].[mycol] AS `Frank`", "[mytbl].[mycol]", "Frank")]
    [TestCase("[mytbl].[mycol] AS [omg its full of spaces]", "[mytbl].[mycol]", "omg its full of spaces")]
    [TestCase("[mydb].[mytbl].[mycol]", "[mydb].[mytbl].[mycol]", null)]
    [TestCase("[mydb].[mytbl].[mycol],", "[mydb].[mytbl].[mycol]", null)]
    [TestCase("count(*) as Frank", "count(*)", "Frank")]
    [TestCase("count(*) as Frank32", "count(*)", "Frank32")]
    [TestCase("CAST([dave] as int) as [number]", "CAST([dave] as int)", "number")]
    [TestCase("CAST([dave] as int)", "CAST([dave] as int)", null)]
    public void SyntaxHelperTest_SplitLineIntoSelectSQLAndAlias(string line, string expectedSelectSql,
        string? expectedAlias)
    {
        foreach (var syntaxHelper in new[] { DatabaseType.Oracle, DatabaseType.MySql, DatabaseType.MicrosoftSQLServer }
                     .Select(static t => ImplementationManager.GetImplementation(t).GetQuerySyntaxHelper()))
            Assert.Multiple(() =>
            {
                Assert.That(syntaxHelper.SplitLineIntoSelectSQLAndAlias(line, out var selectSQL, out var alias),
                    Is.EqualTo(expectedAlias != null));
                Assert.That(selectSQL, Is.EqualTo(expectedSelectSql));
                Assert.That(alias, Is.EqualTo(expectedAlias));
            });
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_GetAlias(DatabaseType t)
    {
        var syntaxHelper = ImplementationManager.GetImplementation(t).GetQuerySyntaxHelper();

        if (!(syntaxHelper.AliasPrefix.StartsWith(' ') && syntaxHelper.AliasPrefix.EndsWith(' ')))
            Assert.Fail(
                $"GetAliasConst method on Type {GetType().Name} returned a value that was not bounded by whitespace ' '.  GetAliasConst must start and end with a space e.g. ' AS '");

        var testString = $"col {syntaxHelper.AliasPrefix} bob";

        syntaxHelper.SplitLineIntoSelectSQLAndAlias(testString, out var selectSQL, out var alias);

        Assert.Multiple(() =>
        {
            Assert.That(selectSQL, Is.EqualTo("col"));
            Assert.That(alias, Is.EqualTo("bob"));
        });
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_BooleanWrapper(DatabaseType dbType)
    {
        Assert.Multiple(() =>
        {
            var syntaxHelper = ImplementationManager.GetImplementation(dbType).GetQuerySyntaxHelper();

            Assert.That(syntaxHelper.True, Is.EqualTo(dbType == DatabaseType.PostgreSql ? "TRUE" : "1"));
            Assert.That(syntaxHelper.False, Is.EqualTo(dbType == DatabaseType.PostgreSql ? "FALSE" : "0"));
        });
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_NameValidation(DatabaseType dbType)
    {
        var syntaxHelper = ImplementationManager.GetImplementation(dbType).GetQuerySyntaxHelper();

        Assert.Throws<RuntimeNameException>(() => syntaxHelper.ValidateDatabaseName(null));
        Assert.Throws<RuntimeNameException>(() => syntaxHelper.ValidateDatabaseName("  "));

        // Special characters are now allowed in all databases since FAnsi wraps/quotes identifiers
        // Dots, parentheses, brackets, etc. are valid in quoted identifiers for all databases
        Assert.DoesNotThrow(() => syntaxHelper.ValidateDatabaseName("db.table"));
        Assert.DoesNotThrow(() => syntaxHelper.ValidateDatabaseName("db(lol)"));

        Assert.Throws<RuntimeNameException>(() =>
            syntaxHelper.ValidateDatabaseName(new string('A', syntaxHelper.MaximumDatabaseLength + 1)));

        Assert.DoesNotThrow(() => syntaxHelper.ValidateDatabaseName("A"));
        Assert.DoesNotThrow(() =>
            syntaxHelper.ValidateDatabaseName(new string('A', syntaxHelper.MaximumDatabaseLength)));
    }

    [Test]
    public void Test_MakeHeaderNameSensible_Unicode()
    {
        Assert.Multiple(static () =>
        {
            //normal unicode is fine
            Assert.That(QuerySyntaxHelper.MakeHeaderNameSensible("你好"), Is.EqualTo("你好"));
            Assert.That(QuerySyntaxHelper.MakeHeaderNameSensible("你好; drop database bob;"),
                Is.EqualTo("你好DropDatabaseBob"));
        });
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_GetFullyQualifiedName(DatabaseType dbType)
    {
        var syntaxHelper = ImplementationManager.GetImplementation(dbType).GetQuerySyntaxHelper();

        var name = syntaxHelper.EnsureFullyQualified("mydb", null, "Troll", ",,,");
        Assert.That(syntaxHelper.GetRuntimeName(name), Is.EqualTo(",,,"));

        switch (dbType)
        {
            case DatabaseType.MicrosoftSQLServer:
                Assert.That(name, Is.EqualTo("[mydb]..[Troll].[,,,]"));
                break;
            case DatabaseType.MySql:
                Assert.That(name, Is.EqualTo("`mydb`.`Troll`.`,,,`"));
                break;
            case DatabaseType.Oracle:
                Assert.That(name, Is.EqualTo("\"MYDB\".\"TROLL\".\",,,\""));
                break;
            case DatabaseType.PostgreSql:
                Assert.That(name, Is.EqualTo("\"mydb\".public.\"Troll\".\",,,\""));
                break;
            case DatabaseType.Sqlite:
                // SQLite doesn't support database/schema qualification, returns only table.column
                Assert.That(name, Is.EqualTo("\"Troll\".\",,,\""));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(dbType), dbType, null);
        }
    }

    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void Test_GetFullyQualifiedName_WhitespaceSchema(DatabaseType dbType)
    {
        var syntaxHelper = ImplementationManager.GetImplementation(dbType).GetQuerySyntaxHelper();

        foreach (var name in new[] { null, "", " ", "\t" }.Select(emptySchemaExpression =>
                     syntaxHelper.EnsureFullyQualified("mydb", emptySchemaExpression, "Troll", "MyCol")))
        {
            Assert.That(string.Equals("MyCol", syntaxHelper.GetRuntimeName(name), StringComparison.OrdinalIgnoreCase));

            switch (dbType)
            {
                case DatabaseType.MicrosoftSQLServer:
                    Assert.That(name, Is.EqualTo("[mydb]..[Troll].[MyCol]"));
                    break;
                case DatabaseType.MySql:
                    Assert.That(name, Is.EqualTo("`mydb`.`Troll`.`MyCol`"));
                    break;
                case DatabaseType.Oracle:
                    Assert.That(name, Is.EqualTo("\"MYDB\".\"TROLL\".\"MYCOL\""));
                    break;
                case DatabaseType.PostgreSql:
                    Assert.That(name, Is.EqualTo("\"mydb\".public.\"Troll\".\"MyCol\""));
                    break;
                case DatabaseType.Sqlite:
                    // SQLite doesn't support database/schema qualification, returns only table.column
                    Assert.That(name, Is.EqualTo("\"Troll\".\"MyCol\""));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(dbType), dbType, null);
            }
        }
    }

    [Test]
    public void Test_GetFullyQualifiedName_BacktickMySql()
    {
        var syntaxHelper = ImplementationManager.GetImplementation(DatabaseType.MySql).GetQuerySyntaxHelper();

        Assert.Multiple(() =>
        {
            //when names have backticks the correct response is to double back tick them
            Assert.That(syntaxHelper.EnsureWrapped("ff`ff"), Is.EqualTo("`ff``ff`"));
            Assert.That(syntaxHelper.EnsureFullyQualified("d`b", null, "ta`ble"), Is.EqualTo("`d``b`.`ta``ble`"));

            //runtime name should still be the actual name of the column
            Assert.That(syntaxHelper.GetRuntimeName("ff`ff"), Is.EqualTo("ff`ff"));
        });
    }
}
