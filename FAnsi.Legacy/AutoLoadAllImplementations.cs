using System.Runtime.CompilerServices;
using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using FAnsi.Implementations.PostgreSql;
using FAnsi.Implementations.Sqlite;

namespace FAnsi.Legacy;

/// <summary>
/// Auto-loads all DBMS implementations when FAnsi.Legacy assembly is loaded.
/// This ensures all implementations are registered without requiring manual Load() calls.
/// </summary>
internal static class AutoLoadAllImplementations
{
    [ModuleInitializer]
#pragma warning disable CS0618 // Type or member is obsolete
    internal static void Initialize()
    {
        ImplementationManager.Load<MicrosoftSQLImplementation>();
        ImplementationManager.Load<MySqlImplementation>();
        ImplementationManager.Load<OracleImplementation>();
        ImplementationManager.Load<PostgreSqlImplementation>();
        ImplementationManager.Load<SqliteImplementation>();
    }
#pragma warning restore CS0618 // Type or member is obsolete
}
