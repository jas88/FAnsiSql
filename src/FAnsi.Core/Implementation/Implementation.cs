using System.Data.Common;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;

namespace FAnsi.Implementation;

public abstract class Implementation<T>(DatabaseType type, Type connectionType) : IImplementation
    where T : DbConnectionStringBuilder, new()
{
    /// <summary>
    ///     The database type this implementation supports
    /// </summary>
    public DatabaseType SupportedDatabaseType => type;

    /// <summary>
    ///     The connection string builder type this implementation supports
    /// </summary>
    public Type ConnectionStringBuilderType => typeof(T);

    /// <summary>
    ///     The connection type this implementation supports
    /// </summary>
    public Type ConnectionType => connectionType;

    public virtual DbConnectionStringBuilder GetBuilder() => new T();

    public abstract IDiscoveredServerHelper GetServerHelper();

    public virtual bool IsFor(DatabaseType databaseType) => type == databaseType;

    public virtual bool IsFor(DbConnectionStringBuilder builder) => builder is T;

    public abstract bool IsFor(DbConnection connection);

    public abstract IQuerySyntaxHelper GetQuerySyntaxHelper();
}
