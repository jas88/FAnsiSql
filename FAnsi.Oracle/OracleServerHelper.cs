using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using FAnsi.Discovery;
using FAnsi.Discovery.QuerySyntax;
using FAnsi.Naming;
using Oracle.ManagedDataAccess.Client;

namespace FAnsi.Implementations.Oracle;

public sealed class OracleServerHelper : DiscoveredServerHelper
{
    public static readonly OracleServerHelper Instance = new();
    private OracleServerHelper() : base(DatabaseType.Oracle)
    {
    }

    /// <summary>
    /// Default timeout for Oracle service validation in seconds. Increased for Docker environments.
    /// </summary>
    private const int DefaultServiceValidationTimeout = 60;

    /// <summary>
    /// Maximum retry attempts for Oracle connection operations in Docker/test environments.
    /// </summary>
    private const int MaxRetryAttempts = 5;

    /// <summary>
    /// Base delay in milliseconds for exponential backoff retry logic.
    /// </summary>
    private const int BaseRetryDelayMs = 1000;

    /// <summary>
    /// Validates that Oracle service is ready to accept connections with enhanced Docker support.
    /// This method implements service readiness checks suitable for Docker environments where
    /// the Oracle service may take significant time to fully initialize.
    /// </summary>
    /// <param name="builder">Connection string builder for Oracle</param>
    /// <param name="timeoutInSeconds">Maximum time to wait for service validation (default: 60s)</param>
    /// <returns>True if Oracle service is ready, false otherwise</returns>
    public bool ValidateOracleService(DbConnectionStringBuilder builder, int timeoutInSeconds = DefaultServiceValidationTimeout)
    {
        var startTime = DateTime.UtcNow;
        var endTime = startTime.AddSeconds(timeoutInSeconds);

        while (DateTime.UtcNow < endTime)
        {
            try
            {
                // Create a connection with increased timeout for validation
                var validationBuilder = GetConnectionStringBuilder(builder.ConnectionString);
                validationBuilder[ConnectionTimeoutKeyName] = Math.Min(30, timeoutInSeconds); // Cap at 30s for individual attempts

                using var connection = GetConnection(validationBuilder);

                // For Oracle connections, set the specific properties for Docker compatibility
                if (connection is OracleConnection oracleConnection)
                {
                    oracleConnection.UseHourOffsetForUnsupportedTimezone = true;
                }

                connection.Open();

                // Validate service is actually ready by executing a simple query
                using var command = GetCommand("SELECT 1 FROM DUAL", connection);
                command.CommandTimeout = 5; // Short timeout for the validation query
                command.ExecuteScalar();

                connection.Close();
                return true;
            }
            catch (OracleException ex) when (IsOracleStartupException(ex))
            {
                // Oracle is still starting up, wait and retry
                Thread.Sleep(2000);
            }
            catch (Exception)
            {
                // For other exceptions, check if we've timed out
                if (DateTime.UtcNow >= endTime)
                    return false;

                // Wait before retrying
                Thread.Sleep(1000);
            }
        }

        return false;
    }

    /// <summary>
    /// Determines if an Oracle exception indicates the service is still starting up.
    /// This helps distinguish between startup delays and other connection issues.
    /// </summary>
    /// <param name="exception">Oracle exception to analyze</param>
    /// <returns>True if the exception indicates Oracle is still starting up</returns>
    private static bool IsOracleStartupException(OracleException exception)
    {
        // Oracle error codes that indicate startup or unavailability issues
        var startupErrorCodes = new HashSet<int>
        {
            12514, // TNS:listener does not currently know of service requested in connect descriptor
            12528, // TNS:listener: all appropriate instances are blocking new connections
            12541, // TNS:no listener
            12560, // TNS:protocol adapter error
            1034,   // ORA-01034: ORACLE not available
            27101,  // ORA-27101: shared memory realm does not exist
            12500,  // TNS:listener failed to start a dedicated server process
            12516   // TNS:listener could not find available instance with requested protocol
        };

        return startupErrorCodes.Contains(exception.Number);
    }

    /// <summary>
    /// Executes an Oracle operation with exponential backoff retry logic for Docker environments.
    /// This method provides robust error handling for operations that may fail due to Docker startup delays.
    /// </summary>
    /// <typeparam name="T">Return type of the operation</typeparam>
    /// <param name="operation">Operation to execute</param>
    /// <param name="isRetryableException">Function to determine if an exception is retryable</param>
    /// <param name="maxRetries">Maximum number of retry attempts (default: 5)</param>
    /// <returns>Result of the operation</returns>
    /// <exception cref="Exception">Thrown if all retry attempts fail</exception>
    public T ExecuteWithRetry<T>(Func<T> operation, Func<Exception, bool>? isRetryableException = null, int maxRetries = MaxRetryAttempts)
    {
        Exception? lastException = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++)
        {
            try
            {
                return operation();
            }
            catch (Exception ex) when (attempt < maxRetries && (isRetryableException?.Invoke(ex) ?? IsRetryableOracleException(ex)))
            {
                lastException = ex;
                var delayMs = BaseRetryDelayMs * (int)Math.Pow(2, attempt); // Exponential backoff
                Thread.Sleep(delayMs);
            }
        }

        // If we get here, all retries failed
        throw lastException ?? new InvalidOperationException("Operation failed after all retry attempts.");
    }

    /// <summary>
    /// Determines if an Oracle exception is retryable (likely due to temporary Docker startup issues).
    /// </summary>
    /// <param name="exception">Exception to analyze</param>
    /// <returns>True if the exception is likely retryable</returns>
    private static bool IsRetryableOracleException(Exception exception)
    {
        // Consider timeout, connection, and startup issues as retryable
        return exception switch
        {
            OracleException oracleEx => IsOracleStartupException(oracleEx) ||
                                      oracleEx.Number == 03113 || // end-of-file on communication channel
                                      oracleEx.Number == 03114,   // not connected to ORACLE
            TimeoutException => true,
            InvalidOperationException ex when ex.Message.Contains("timeout", StringComparison.OrdinalIgnoreCase) => true,
            _ => false
        };
    }

    /// <summary>
    /// Opens a connection with enhanced timeout handling and retry logic for Docker environments.
    /// </summary>
    /// <param name="connection">Connection to open</param>
    /// <param name="validateServiceFirst">Whether to validate service readiness before attempting connection</param>
    public void OpenConnectionWithRetry(DbConnection connection, bool validateServiceFirst = true)
    {
        if (validateServiceFirst && connection is OracleConnection oracleConnection)
        {
            var builder = GetConnectionStringBuilder(oracleConnection.ConnectionString);
            if (!ValidateOracleService(builder))
            {
                throw new InvalidOperationException($"Oracle service validation failed after {DefaultServiceValidationTimeout} seconds.");
            }
        }

        ExecuteWithRetry(() =>
        {
            if (connection.State != System.Data.ConnectionState.Open)
            {
                connection.Open();
            }
            return true;
        });
    }

    protected override string ServerKeyName => "DATA SOURCE";
    protected override string DatabaseKeyName => "USER ID"; //ok is this really what oracle does?


    protected override string ConnectionTimeoutKeyName => "Connection Timeout";

    #region Up Typing
    public override DbCommand GetCommand(string s, DbConnection con, DbTransaction? transaction = null) => new OracleCommand(s, con as OracleConnection) { Transaction = transaction as OracleTransaction };

    public override DbDataAdapter GetDataAdapter(DbCommand cmd) => new OracleDataAdapter((OracleCommand)cmd);

    public override DbCommandBuilder GetCommandBuilder(DbCommand cmd) => new OracleCommandBuilder((OracleDataAdapter)GetDataAdapter(cmd));

    public override DbParameter GetParameter(string parameterName) => new OracleParameter(parameterName, null);

    public override DbConnection GetConnection(DbConnectionStringBuilder builder) => new OracleConnection(builder.ConnectionString);

    protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string? connectionString) =>
        new OracleConnectionStringBuilder(connectionString);

    #endregion

    protected override DbConnectionStringBuilder GetConnectionStringBuilderImpl(string server, string? database, string username, string password)
    {
        var toReturn = new OracleConnectionStringBuilder { DataSource = server };

        if (string.IsNullOrWhiteSpace(username))
            toReturn.UserID = "/";
        else
        {
            toReturn.UserID = username;
            toReturn.Password = password;
        }

        return toReturn;
    }

    public override DbConnectionStringBuilder ChangeDatabase(DbConnectionStringBuilder builder, string newDatabase) =>
        //does not apply to oracle since user = database but we create users with random passwords
        builder;

    public override DbConnectionStringBuilder EnableAsync(DbConnectionStringBuilder builder) => builder;

    public override IDiscoveredDatabaseHelper GetDatabaseHelper() => OracleDatabaseHelper.Instance;

    public override IQuerySyntaxHelper GetQuerySyntaxHelper() => OracleQuerySyntaxHelper.Instance;

    public override string? GetCurrentDatabase(DbConnectionStringBuilder builder) =>
        //Oracle does not persist database as a connection string (only server).
        null;

    public override void CreateDatabase(DbConnectionStringBuilder builder, IHasRuntimeName newDatabaseName)
    {
        ExecuteWithRetry(() =>
        {
            using var con = new OracleConnection(builder.ConnectionString);
            con.UseHourOffsetForUnsupportedTimezone = true;

            // Use enhanced connection opening with retry logic
            OpenConnectionWithRetry(con, validateServiceFirst: true);

            var password = $"pwd{Guid.NewGuid().ToString().Replace("-", "")[..27]}"; // Oracle only allows 30 character passwords

            // Create a new user with a random password - Oracle's approach where user = database
            using (var cmd = new OracleCommand(
                      $"CREATE USER \"{newDatabaseName.GetRuntimeName()}\" IDENTIFIED BY {password}", con))
            {
                cmd.CommandTimeout = CreateDatabaseTimeoutInSeconds;
                cmd.ExecuteNonQuery();
            }

            // Grant unlimited quota on system tablespace
            using (var cmd = new OracleCommand(
                      $"ALTER USER \"{newDatabaseName.GetRuntimeName()}\" quota unlimited on system", con))
            {
                cmd.CommandTimeout = CreateDatabaseTimeoutInSeconds;
                cmd.ExecuteNonQuery();
            }

            // Grant unlimited quota on users tablespace
            using (var cmd = new OracleCommand(
                      $"ALTER USER \"{newDatabaseName.GetRuntimeName()}\" quota unlimited on users", con))
            {
                cmd.CommandTimeout = CreateDatabaseTimeoutInSeconds;
                cmd.ExecuteNonQuery();
            }

            return true;
        });
    }

    public override Dictionary<string, string> DescribeServer(DbConnectionStringBuilder builder) => throw new NotImplementedException();

    public override string GetExplicitUsernameIfAny(DbConnectionStringBuilder builder) => ((OracleConnectionStringBuilder)builder).UserID;

    public override string GetExplicitPasswordIfAny(DbConnectionStringBuilder builder) => ((OracleConnectionStringBuilder)builder).Password;

    public override Version? GetVersion(DiscoveredServer server)
    {
        return ExecuteWithRetry(() =>
        {
            using var tcon = server.GetConnection();
            if (tcon is not OracleConnection con) throw new ArgumentException("Oracle helper called on non-Oracle server", nameof(server));

            con.UseHourOffsetForUnsupportedTimezone = true;
            OpenConnectionWithRetry(con, validateServiceFirst: true);

            using var cmd = server.GetCommand("SELECT * FROM v$version WHERE BANNER like 'Oracle Database%'", con);
            cmd.CommandTimeout = 30; // Reasonable timeout for version query
            using var r = cmd.ExecuteReader();
            return !r.Read() || r[0] == DBNull.Value ? null : CreateVersionFromString((string)r[0]);
        });
    }

    public override IEnumerable<string> ListDatabases(DbConnectionStringBuilder builder)
    {
        //todo do we have to edit the builder in here in case it is pointed at nothing?
        using var con = new OracleConnection(builder.ConnectionString);
        con.UseHourOffsetForUnsupportedTimezone = true;

        // Use enhanced connection opening with retry logic
        OpenConnectionWithRetry(con, validateServiceFirst: true);

        foreach (var listDatabase in ListDatabases(con)) yield return listDatabase;
    }

    public override IEnumerable<string> ListDatabases(DbConnection con)
    {
        using var cmd = GetCommand("select * from all_users", con);
        using var r = cmd.ExecuteReader();
        while (r.Read())
            yield return (string)r["username"];
    }

    public override bool IsConnectionAlive(DbConnection connection)
    {
        try
        {
            // Check for dangling transactions (fixes #30)
            if (HasDanglingTransaction(connection))
                return false;

            // Try a simple command to verify connection is usable with increased timeout for Docker environments
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT 1 FROM DUAL";  // Oracle syntax
            cmd.CommandTimeout = 5; // Increased timeout for Docker environments
            cmd.ExecuteScalar();
            return true;
        }
        catch (OracleException ex) when (IsOracleStartupException(ex))
        {
            // Oracle is still starting up, consider connection not alive for now
            return false;
        }
        catch
        {
            return false;
        }
    }

    public override bool HasDanglingTransaction(DbConnection connection)
    {
        // Oracle doesn't have an easy way to detect uncommitted transactions at the session level
        // OracleConnection does track transactions internally but property isn't public
        // For now, rely on ADO.NET's internal tracking
        return false;
    }

    public override bool DatabaseExists(DiscoveredDatabase database)
    {
        return ExecuteWithRetry(() =>
        {
            // In Oracle, databases are schemas/users - can query ALL_USERS from any connection
            var oracleServer = new DiscoveredServer(database.Server.Builder.ConnectionString, DatabaseType.Oracle);
            using var con = oracleServer.GetManagedConnection();

            // Ensure the underlying connection is opened with retry logic
            if (con.Connection.State != System.Data.ConnectionState.Open)
            {
                OpenConnectionWithRetry(con.Connection, validateServiceFirst: true);
            }

            using var cmd = new OracleCommand("SELECT CASE WHEN EXISTS(SELECT 1 FROM ALL_USERS WHERE USERNAME = UPPER(:name)) THEN 1 ELSE 0 END FROM DUAL", (OracleConnection)con.Connection);
            cmd.CommandTimeout = 30; // Reasonable timeout for database existence check
            cmd.Parameters.Add(new OracleParameter("name", database.GetRuntimeName()));
            return Convert.ToInt32(cmd.ExecuteScalar()) == 1;
        });
    }

    /// <summary>
    /// Oracle-specific enhanced service validation method for Docker environments where Oracle may take longer to start up.
    /// This method provides more robust validation than the base RespondsWithinTime for Oracle-specific scenarios.
    /// </summary>
    /// <param name="builder">Connection string builder</param>
    /// <param name="timeoutInSeconds">Timeout in seconds</param>
    /// <param name="exception">Output exception if validation fails</param>
    /// <returns>True if Oracle responds within timeout, false otherwise</returns>
    public bool ValidateOracleServiceWithinTime(DbConnectionStringBuilder builder, int timeoutInSeconds, out Exception? exception)
    {
        try
        {
            // Use our enhanced service validation for Oracle
            var isValid = ValidateOracleService(builder, Math.Max(timeoutInSeconds, DefaultServiceValidationTimeout));

            exception = null;
            return isValid;
        }
        catch (Exception e)
        {
            exception = e;
            return false;
        }
    }
}
