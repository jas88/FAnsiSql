using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using FAnsi.Exceptions;

namespace FAnsi.Implementation;

/// <summary>
/// Handles detecting and loading implementations
/// </summary>
public sealed class ImplementationManager
{
    private static readonly ImplementationManager Instance = new();
    private static readonly object _lock = new();

    /// <summary>
    /// Fast read-only lookup for O(1) access by any supported type (DatabaseType, ConnectionStringBuilder, or Connection)
    /// </summary>
    private volatile FrozenDictionary<Type, IImplementation> _lookup = FrozenDictionary<Type, IImplementation>.Empty;

    /// <summary>
    /// Fast O(1) array lookup for DatabaseType to implementation mapping. Initialized with nulls.
    /// </summary>
    private static readonly IImplementation?[] _databaseTypeLookup = new IImplementation[5]; // 5 DatabaseType enum values

    // Cached CompositeFormat for CA1863
    private static readonly CompositeFormat NoDatabaseTypeImplementationFormat = CompositeFormat.Parse(FAnsiStrings.ImplementationManager_GetImplementation_No_implementation_found_for_DatabaseType__0_);
    private static readonly CompositeFormat NoAdoNetImplementationFormat = CompositeFormat.Parse(FAnsiStrings.ImplementationManager_GetImplementation_No_implementation_found_for_ADO_Net_object_of_Type__0_);

    /// <summary>
    /// Registers an implementation instance for fast O(1) lookups
    /// </summary>
    /// <param name="implementation">The implementation to register</param>
    public static void Register(IImplementation implementation)
    {
        lock (_lock)
        {
            // Convert to mutable dictionary to add new entries
            var builder = new Dictionary<Type, IImplementation>(Instance._lookup);

            // Add all three lookup types for this implementation
            builder[implementation.ConnectionStringBuilderType] = implementation;
            builder[implementation.ConnectionType] = implementation;

            // Rebuild the frozen dictionary
            Instance._lookup = builder.ToFrozenDictionary();

            // Store in DatabaseType array for O(1) lookup
            var dbType = implementation.SupportedDatabaseType;
            if (dbType >= 0 && (int)dbType < _databaseTypeLookup.Length)
                _databaseTypeLookup[(int)dbType] = implementation;
        }
    }

    /// <summary>
    /// loads all implementations in the assembly hosting the <typeparamref name="T"/>
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [Obsolete("Implementations now auto-register via static constructor in DiscoveredServer. To force assembly loading, call {TypeName}.EnsureLoaded() instead (e.g., MicrosoftSQLImplementation.EnsureLoaded()).")]
    public static void Load<T>() where T : IImplementation, new()
    {
        var loading = new T();
        Register(loading);
    }


    public static IImplementation GetImplementation(DatabaseType databaseType)
    {
        // O(1) array lookup using DatabaseType enum as index
        if (databaseType >= 0 && (int)databaseType < _databaseTypeLookup.Length)
        {
            var implementation = _databaseTypeLookup[(int)databaseType];
            if (implementation != null)
                return implementation;
        }

        throw new ImplementationNotFoundException(string.Format(
            CultureInfo.InvariantCulture,
            NoDatabaseTypeImplementationFormat,
            databaseType));
    }

    public static IImplementation GetImplementation(DbConnectionStringBuilder connectionStringBuilder)
    {
        var builderType = connectionStringBuilder.GetType();
        if (Instance._lookup.TryGetValue(builderType, out var implementation))
            return implementation;

        throw new ImplementationNotFoundException(string.Format(
            CultureInfo.InvariantCulture,
            NoAdoNetImplementationFormat,
            connectionStringBuilder.GetType()));
    }

    public static IImplementation GetImplementation(DbConnection connection)
    {
        var connectionType = connection.GetType();
        if (Instance._lookup.TryGetValue(connectionType, out var implementation))
            return implementation;

        throw new ImplementationNotFoundException(string.Format(
            CultureInfo.InvariantCulture,
            NoAdoNetImplementationFormat,
            connection.GetType()));
    }
    /// <summary>
    /// Returns all currently loaded implementations
    /// </summary>
    /// <returns></returns>
    public static IEnumerable<IImplementation> GetImplementations() => Instance._lookup.Values.Distinct();

    [Obsolete("MEF is dead")]
    public static void Load(params Assembly[] _)
    {
    }
}
