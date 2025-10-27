namespace FAnsi;

/// <summary>
/// Global configuration options for FAnsi behavior
/// </summary>
public static class FAnsiConfiguration
{
    /// <summary>
    /// <para>Controls whether thread-local connection pooling is enabled.</para>
    ///
    /// <para>When <c>true</c> (default: <c>false</c>):</para>
    /// <list type="bullet">
    /// <item><description>SQL Server and MySQL use server-level pooling (one connection per server per thread)</description></item>
    /// <item><description>Database switching via USE (SQL Server) or ChangeDatabase() (MySQL)</description></item>
    /// <item><description>Can reduce connection count by up to 90% in multi-database scenarios</description></item>
    /// <item><description>PostgreSQL uses database-level pooling (cannot switch databases)</description></item>
    /// <item><description>Oracle falls back to ADO.NET native pooling</description></item>
    /// </list>
    ///
    /// <para>When <c>false</c> (default):</para>
    /// <list type="bullet">
    /// <item><description>All database types use ADO.NET's native connection pooling</description></item>
    /// <item><description>More connections but simpler behavior</description></item>
    /// <item><description>Avoids potential issues with dropped databases</description></item>
    /// </list>
    ///
    /// <para><b>⚠️ Warning:</b> Enabling pooling can cause issues if databases are dropped/recreated during operations.
    /// Set this at application startup before any database operations.</para>
    /// </summary>
    /// <example>
    /// <code>
    /// // Enable at application startup for better performance
    /// FAnsiConfiguration.EnableThreadLocalConnectionPooling = true;
    ///
    /// // Or leave disabled (default) for simpler behavior
    /// FAnsiConfiguration.EnableThreadLocalConnectionPooling = false;
    /// </code>
    /// </example>
    public static bool EnableThreadLocalConnectionPooling { get; set; } = false;

    /// <summary>
    /// <para>Clears all thread-local connection pools. Only has effect when
    /// <see cref="EnableThreadLocalConnectionPooling"/> is <c>true</c>.</para>
    ///
    /// <para>Call this when:</para>
    /// <list type="bullet">
    /// <item><description>You've dropped/recreated databases and want fresh connections</description></item>
    /// <item><description>During application shutdown to release resources</description></item>
    /// <item><description>After major schema changes</description></item>
    /// </list>
    /// </summary>
    /// <example>
    /// <code>
    /// // After dropping and recreating a database
    /// database.Drop();
    /// database.Create();
    /// FAnsiConfiguration.ClearConnectionPools(); // Get fresh connections
    /// </code>
    /// </example>
    public static void ClearConnectionPools()
    {
        if (EnableThreadLocalConnectionPooling)
            Connections.ManagedConnectionPool.ClearCurrentThreadConnections();
    }

    /// <summary>
    /// <para>Clears all connection pools across all threads. Only has effect when
    /// <see cref="EnableThreadLocalConnectionPooling"/> is <c>true</c>.</para>
    ///
    /// <para>Should be called during application shutdown to ensure all connections are properly closed.</para>
    /// </summary>
    /// <example>
    /// <code>
    /// // In application shutdown handler
    /// FAnsiConfiguration.ClearAllConnectionPools();
    /// </code>
    /// </example>
    public static void ClearAllConnectionPools()
    {
        if (EnableThreadLocalConnectionPooling)
            Connections.ManagedConnectionPool.ClearAllConnections();
    }
}
