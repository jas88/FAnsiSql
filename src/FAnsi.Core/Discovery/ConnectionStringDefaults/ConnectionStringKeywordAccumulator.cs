using System.Collections.Immutable;
using System.Data.Common;
using System.Globalization;
using System.Text;
using FAnsi.Implementation;

namespace FAnsi.Discovery.ConnectionStringDefaults;

/// <summary>
///     <para>
///         Gathers keywords for use in building connection strings for a given <see cref="DatabaseType" />.  Once created
///         you can add keywords and then apply the template
///         to new novel connection strings (see <see cref="EnforceOptions" />).
///     </para>
///     <para>Also handles connection string keyword aliases (where two words mean the same thing)</para>
///     <para>
///         Keywords can be added and enforced from several threads at once.  Each <see cref="AddOrUpdateKeyword" /> takes
///         effect as a whole, and <see cref="EnforceOptions" /> applies the keywords as they were when it started.
///     </para>
/// </summary>
public sealed class ConnectionStringKeywordAccumulator
{
    // Cached CompositeFormat for CA1863
    private static readonly CompositeFormat KeywordNotSupportedFormat =
        CompositeFormat.Parse(FAnsiStrings.ConnectionStringKeyword_ValueNotSupported);

    /// <summary>
    ///     Finds out which keywords are aliases of each other (see <see cref="GetCollisionWithKeyword" />).  That changes
    ///     its contents, so it is only used under <see cref="_lock" />.
    /// </summary>
    private readonly DbConnectionStringBuilder _builder;

    /// <summary>
    ///     Held while a keyword is added, so that one thread at a time uses <see cref="_builder" /> and replaces
    ///     <see cref="_keywords" />
    /// </summary>
    private readonly Lock _lock = new();

    /// <summary>
    ///     The keywords with their values and priorities.  The dictionary is never changed, only replaced under
    ///     <see cref="_lock" />, so <see cref="EnforceOptions" /> can read it without the lock.
    /// </summary>
    private volatile ImmutableDictionary<string, Tuple<string, ConnectionStringKeywordPriority>> _keywords =
        ImmutableDictionary.Create<string, Tuple<string, ConnectionStringKeywordPriority>>(
            StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Initialises a new blank instance that does nothing.  Call <see cref="AddOrUpdateKeyword" /> to adjust the template
    ///     connection string options.
    /// </summary>
    /// <param name="databaseType"></param>
    public ConnectionStringKeywordAccumulator(DatabaseType databaseType) : this(databaseType,
        ImplementationManager.GetImplementation(databaseType).GetBuilder())
    {
    }

    /// <summary>
    ///     Initialises a new blank instance that finds out which keywords are aliases of each other with
    ///     <paramref name="builder" /> rather than with a connection string builder of <paramref name="databaseType" />, so
    ///     that tests can see what happens while a keyword is being added
    /// </summary>
    internal ConnectionStringKeywordAccumulator(DatabaseType databaseType, DbConnectionStringBuilder builder)
    {
        DatabaseType = databaseType;
        _builder = builder;
    }

    /// <summary>
    ///     <see cref="DatabaseType" /> describing what implementation of DbConnectionStringBuilder is being manipulated
    /// </summary>
    public DatabaseType DatabaseType { get; private set; }

    /// <summary>
    ///     Adds a new connection string option (which must be compatible with <see cref="DatabaseType" />)
    /// </summary>
    /// <param name="keyword"></param>
    /// <param name="value"></param>
    /// <param name="priority"></param>
    public void AddOrUpdateKeyword(string keyword, string value, ConnectionStringKeywordPriority priority)
    {
        lock (_lock)
        {
            //if there is already a semantically equivalent keyword, update that one rather than recording this one too
            var key = GetCollisionWithKeyword(keyword, value) ?? keyword;

            //a value already there is only replaced by one of the same or a higher priority
            if (!_keywords.TryGetValue(key, out var existing) || existing.Item2 <= priority)
                _keywords = _keywords.SetItem(key, Tuple.Create(value, priority));
        }
    }

    /// <summary>
    ///     Returns the best alias for <paramref name="keyword" /> or null if there are no known aliases.  This is because some
    ///     builders allow multiple keys for changing the same underlying
    ///     property.
    /// </summary>
    /// <param name="keyword"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    private string? GetCollisionWithKeyword(string keyword, string value)
    {
        ArgumentNullException.ThrowIfNull(keyword);
        ArgumentNullException.ThrowIfNull(value);

        //let's evaluate this alleged keyword!
        _builder.Clear();

        try
        {
            //Make sure it is supported by the connection string builder
            _builder.Add(keyword, value);
        }
        catch (NotSupportedException ex)
        {
            //don't output the value since that could be a password
            throw new ArgumentException(string.Format(CultureInfo.InvariantCulture, KeywordNotSupportedFormat, keyword),
                ex);
        }

        //now iterate all the keys we had before and add those too, if the key count doesn't change for any of them we know it's a duplicate semantically
        if (_builder.Keys == null) return null;

        foreach (var current in _keywords)
        {
            var keysBefore = _builder.Keys.Count;

            _builder.Add(current.Key, current.Value.Item1);

            //key count in builder didn't change despite there being new values added
            if (_builder.Keys.Count == keysBefore)
                return current.Key;
        }

        //no collisions
        return null;
    }

    /// <summary>
    ///     Adds the currently configured keywords to the connection string builder.
    /// </summary>
    public void EnforceOptions(DbConnectionStringBuilder connectionStringBuilder)
    {
        foreach (var keyword in _keywords)
            connectionStringBuilder[keyword.Key] = keyword.Value.Item1;
    }
}
