using System.Data.Common;
using System.Runtime.CompilerServices;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using Oracle.ManagedDataAccess.Client;

namespace FAnsi.Implementations.Oracle;

public sealed class OracleImplementation() : Implementation<OracleConnectionStringBuilder>(DatabaseType.Oracle)
{
    /// <summary>
    /// Ensures this implementation is registered with the ImplementationManager.
    /// Call this method if you need to guarantee the implementation is loaded before use.
    /// </summary>
    public static void EnsureLoaded()
    {
        // Method body intentionally empty - the ModuleInitializer handles registration.
        // This method exists to force the assembly to load, triggering the ModuleInitializer.
    }

    public override IDiscoveredServerHelper GetServerHelper() => OracleServerHelper.Instance;

    public override bool IsFor(DbConnection connection) => connection is OracleConnection;

    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => OracleQuerySyntaxHelper.Instance;
}

/// <summary>
/// Internal class responsible for automatic registration of Oracle implementation on module initialization.
/// </summary>
internal static class AutoRegister
{
    /// <summary>
    /// Automatically registers the Oracle implementation when the assembly is loaded.
    /// </summary>
    [ModuleInitializer]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2255:The 'ModuleInitializer' attribute should not be used in libraries", Justification = "Required for automatic DBMS implementation registration")]
#pragma warning disable CS0618 // Type or member is obsolete
    internal static void Initialize() => ImplementationManager.Load<OracleImplementation>();
#pragma warning restore CS0618 // Type or member is obsolete
}
