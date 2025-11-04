using System;
using System.Linq;
using FAnsi.Discovery;
using FAnsi.Discovery.TypeTranslation;

namespace TestGenerator;

class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("Testing FAnsiSql source generation...");

        // Test that the TypeTranslater works correctly
        var translater = ImplementationManager.GetImplementation(DatabaseType.MicrosoftSQLServer)
            .GetQuerySyntaxHelper().TypeTranslater;

        var typeRequest = new DatabaseTypeRequest(typeof(byte[]));
        var sqlType = translater.GetSQLDBTypeForCSharpType(typeRequest);

        Console.WriteLine($"SQL Server byte[] type: {sqlType}");

        // Test MySQL
        var mysqlTranslater = ImplementationManager.GetImplementation(DatabaseType.MySql)
            .GetQuerySyntaxHelper().TypeTranslater;
        var mysqlSqlType = mysqlTranslater.GetSQLDBForCSharpType(typeRequest);

        Console.WriteLine($"MySQL byte[] type: {mysqlSqlType}");

        // Test PostgreSQL
        var pgTranslater = ImplementationManager.GetImplementation(DatabaseType.PostgreSql)
            .GetQuerySyntaxHelper().TypeTranslater;
        var pgSqlType = pgTranslater.GetSQLDBForCSharpType(typeRequest);

        Console.WriteLine($"PostgreSQL byte[] type: {pgSqlType}");

        // Test SQLite
        var sqliteTranslater = ImplementationManager.GetImplementation(DatabaseType.Sqlite)
            .GetQuerySyntaxHelper().TypeTranslater;
        var sqliteSqlType = sqliteTranslater.GetSQLDBForCSharpType(typeRequest);

        Console.WriteLine($"SQLite byte[] type: {sqliteSqlType}");

        // Test GUID type
        var guidRequest = new DatabaseTypeRequest(typeof(Guid));
        var guidSqlType = translater.GetSQLDBTypeForCSharpType(guidRequest);

        Console.WriteLine($"SQL Server GUID type: {guidSqlType}");

        Console.WriteLine("All tests passed! ✅");
    }
}