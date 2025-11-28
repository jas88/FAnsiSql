using System.Runtime.CompilerServices;
using FAnsi.Implementation;
using FAnsi.Implementations.MicrosoftSQL;
using FAnsi.Implementations.MySql;
using FAnsi.Implementations.Oracle;
using FAnsi.Implementations.PostgreSql;
using FAnsi.Implementations.Sqlite;

namespace FAnsi.Legacy;

/// <summary>
/// Auto-registers all DBMS implementations when FAnsi.Legacy loads.
/// AOT-compatible: uses direct type references and explicit registration.
/// </summary>
internal static class AutoLoadAllImplementations
{
    [ModuleInitializer]
#pragma warning disable CS0618 // Type or member is obsolete
    internal static void Initialize()
    {
        // Directly register each implementation
        // This is AOT-compatible (no reflection, no Type.GetType)
        ImplementationManager.Load<MicrosoftSQLImplementation>();
        ImplementationManager.Load<MySqlImplementation>();
        ImplementationManager.Load<OracleImplementation>();
        ImplementationManager.Load<PostgreSqlImplementation>();
        ImplementationManager.Load<SqliteImplementation>();
    }
#pragma warning restore CS0618 // Type or member is obsolete
}
