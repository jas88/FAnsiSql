using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.Loader;
using NUnit.Framework;

namespace FAnsiTests.Server;

/// <summary>
///     Implementations can be loaded one at a time, in any order, with their EnsureLoaded() methods.  Which are loaded is
///     process-wide static state, and the other tests load every implementation before they start, so these tests load
///     FAnsi afresh into an <see cref="AssemblyLoadContext" /> of their own, whose statics start out as in a new process.
/// </summary>
internal sealed class ImplementationLoadOrderTests
{
    /// <summary>
    ///     A DBMS loaded after a connection string of another DBMS has been built can still be used, and still gets its
    ///     connection string keywords.  MySQL adds keywords (AllowUserVariables and others) in the static constructor of its
    ///     server helper, which failed when the keyword table had been set up before MySQL was loaded, so that MySQL could
    ///     not be used at all.
    /// </summary>
    [TestCase("PostgreSql", "MySql")]
    [TestCase("MicrosoftSql", "MySql")]
    [TestCase("MySql", "PostgreSql")]
    [RequiresUnreferencedCode("Loads FAnsi into an AssemblyLoadContext of its own and uses it through reflection")]
    public void ImplementationLoadedAfterAnotherIsUsed_CanBeUsed(string first, string second)
    {
        var context = new AssemblyLoadContext("FAnsi load order", true);

        try
        {
            // FAnsi.Core afresh: each provider loaded afterwards into the same context registers itself with this copy
            var core = LoadIntoContext(context, "FAnsi.Core");

            var firstServer = CreateDiscoveredServer(context, core, first, "fansi_first");
            var secondServer = CreateDiscoveredServer(context, core, second, "fansi_second");

            Assert.Multiple(() =>
            {
                Assert.That(GetConnectionString(firstServer), Does.Contain("fansi_first"));
                Assert.That(GetConnectionString(secondServer), Does.Contain("fansi_second"));
                Assert.That(GetConnectionString(first == "MySql" ? firstServer : secondServer),
                    Does.Match("(?i)Allow ?User ?Variables=True"), "MySQL's connection string keywords should be enforced");
            });
        }
        finally
        {
            context.Unload();
        }
    }

    /// <summary>
    ///     Loads the implementation of <paramref name="implementation" /> into <paramref name="context" />, with its
    ///     EnsureLoaded() method, and returns a DiscoveredServer for it whose connection string has
    ///     <paramref name="database" />
    /// </summary>
    [RequiresUnreferencedCode("Uses FAnsi through reflection")]
    private static object CreateDiscoveredServer(AssemblyLoadContext context, Assembly core, string implementation,
        string database)
    {
        var (assemblyName, typeName, databaseType, connectionString) = implementation switch
        {
            "MicrosoftSql" => ("FAnsi.MicrosoftSql", "FAnsi.Implementations.MicrosoftSQL.MicrosoftSQLImplementation",
                "MicrosoftSQLServer", $"Data Source=localhost;Initial Catalog={database};User ID=fansi"),
            "MySql" => ("FAnsi.MySql", "FAnsi.Implementations.MySql.MySqlImplementation", "MySql",
                $"Server=localhost;Database={database};Uid=fansi"),
            "PostgreSql" => ("FAnsi.PostgreSql", "FAnsi.Implementations.PostgreSql.PostgreSqlImplementation",
                "PostgreSql", $"Host=localhost;Database={database};Username=fansi"),
            _ => throw new ArgumentOutOfRangeException(nameof(implementation), implementation, null)
        };

        var provider = LoadIntoContext(context, assemblyName);
        var ensureLoaded = provider.GetType(typeName, true)?.GetMethod("EnsureLoaded") ??
                           throw new InvalidOperationException($"{typeName} has no EnsureLoaded method");
        ensureLoaded.Invoke(null, null);

        var discoveredServer = core.GetType("FAnsi.Discovery.DiscoveredServer", true) ??
                               throw new InvalidOperationException("No DiscoveredServer type");
        var databaseTypeEnum = core.GetType("FAnsi.DatabaseType", true) ??
                               throw new InvalidOperationException("No DatabaseType type");

        try
        {
            return Activator.CreateInstance(discoveredServer, connectionString,
                       Enum.Parse(databaseTypeEnum, databaseType)) ??
                   throw new InvalidOperationException("No DiscoveredServer was made");
        }
        catch (TargetInvocationException e)
        {
            throw new AssertionException(
                $"Could not make a {implementation} DiscoveredServer after loading it: {e.InnerException}", e);
        }
    }

    [RequiresUnreferencedCode("Loads an assembly by its path")]
    private static Assembly LoadIntoContext(AssemblyLoadContext context, string assemblyName) =>
        context.LoadFromAssemblyPath(Path.Combine(TestContext.CurrentContext.TestDirectory, $"{assemblyName}.dll"));

    [RequiresUnreferencedCode("Uses FAnsi through reflection")]
    private static string GetConnectionString(object server) =>
        (server.GetType().GetProperty("Builder")?.GetValue(server) as DbConnectionStringBuilder)?.ConnectionString ??
        throw new InvalidOperationException("The DiscoveredServer has no Builder");
}
