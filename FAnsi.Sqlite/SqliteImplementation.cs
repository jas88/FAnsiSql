using System.Data.Common;
using System.Runtime.CompilerServices;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Implementation;
using Microsoft.Data.Sqlite;

namespace FAnsi.Implementations.Sqlite;

/// <summary>
/// Database implementation for SQLite, providing SQLite-specific functionality through the FAnsi abstraction layer.
/// SQLite is a file-based, serverless, zero-configuration database engine supporting ACID transactions.
/// </summary>
/// <remarks>
/// <para>Key SQLite characteristics:</para>
/// <list type="bullet">
/// <item><description>File-based: Each database is a single disk file</description></item>
/// <item><description>Dynamic typing: Uses type affinity rather than strict types</description></item>
/// <item><description>No schemas: Schema separation not supported in traditional sense</description></item>
/// <item><description>Limited ALTER TABLE: Some operations require table recreation</description></item>
/// <item><description>No stored procedures or table-valued functions</description></item>
/// </list>
/// </remarks>
public sealed class SqliteImplementation()
    : Implementation<SqliteConnectionStringBuilder>(DatabaseType.Sqlite)
{
    /// <inheritdoc />
    public override IDiscoveredServerHelper GetServerHelper() => SqliteServerHelper.Instance;

    /// <inheritdoc />
    public override bool IsFor(DbConnection conn) => conn is SqliteConnection;

    /// <inheritdoc />
    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => SqliteQuerySyntaxHelper.Instance;
}

/// <summary>
/// Internal class responsible for automatic registration of SQLite implementation on module initialization.
/// </summary>
internal static class AutoRegister
{
    /// <summary>
    /// Automatically registers the SQLite implementation when the assembly is loaded.
    /// </summary>
    [ModuleInitializer]
#pragma warning disable CS0618 // Type or member is obsolete
    internal static void Initialize() => ImplementationManager.Load<SqliteImplementation>();
#pragma warning restore CS0618 // Type or member is obsolete
}
