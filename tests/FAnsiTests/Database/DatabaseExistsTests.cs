using System.Globalization;
using FAnsi;
using FAnsi.Discovery;
using NUnit.Framework;
using TypeGuesser;

namespace FAnsiTests.Database;

/// <summary>
///     Tests for DiscoveredDatabase.Exists() method across all database types.
///     Verifies that checking for non-existent databases returns false instead of throwing exceptions.
/// </summary>
internal sealed class DatabaseExistsTests : DatabaseTests
{
    /// <summary>
    ///     Test that DiscoveredDatabase.Exists() returns false for non-existent databases
    ///     without throwing exceptions. This is critical for PostgreSQL where connecting
    ///     to a non-existent database throws an exception.
    /// </summary>
    /// <remarks>
    ///     Upstream issue: HicServices/FAnsiSql#335
    ///     PostgreSQL requires connecting to a system database (postgres) first to query pg_database.
    ///     This fork's implementation correctly handles this by using DatabaseExists() which connects
    ///     to the postgres system database before checking.
    /// </remarks>
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void DiscoveredDatabase_Exists_NonExistentDatabase_ReturnsFalse(DatabaseType dbType)
    {
        var server = GetTestServer(dbType);
        var db = server.ExpectDatabase("NonExistentDatabase_12345");

        // Should return false, not throw an exception
        Assert.That(db.Exists(), Is.False,
            $"Exists() should return false for non-existent database on {dbType}, not throw an exception");
    }

    /// <summary>
    ///     Test that DiscoveredDatabase.Exists() returns true for existing databases
    /// </summary>
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void DiscoveredDatabase_Exists_ExistingDatabase_ReturnsTrue(DatabaseType dbType)
    {
        var db = GetTestDatabase(dbType);

        Assert.That(db.Exists(), Is.True,
            $"Exists() should return true for existing database on {dbType}");
    }

    /// <summary>
    ///     Test that creating and then checking a database works correctly
    /// </summary>
    [TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
    public void DiscoveredDatabase_Exists_AfterCreate_ReturnsTrue(DatabaseType dbType)
    {
        var server = GetTestServer(dbType);
        var dbName = $"TestExistsDB_{dbType}_{Guid.NewGuid():N}".Substring(0, 30);
        var db = server.ExpectDatabase(dbName);

        try
        {
            // Should not exist initially
            Assert.That(db.Exists(), Is.False, "Database should not exist before creation");

            // Create it
            db.Create();

            // Should exist now
            Assert.That(db.Exists(), Is.True, "Database should exist after creation");
        }
        finally
        {
            // Clean up
            if (db.Exists())
                db.Drop();
        }
    }

    /// <summary>
    ///     A SQLite database is a file, and Microsoft.Data.Sqlite keeps closed connections open in a pool.  Dropping a
    ///     database deleted its file but left the pooled connections to it, so creating a database of the same name again
    ///     reopened one of those: no file was created and CreateDatabase failed with "didn't exist after the creation
    ///     attempt".
    /// </summary>
    [Test]
    public void CreateDatabase_Sqlite_SameNameAfterDrop_CreatesNewFile()
    {
        var server = GetTestServer(DatabaseType.Sqlite);
        var directory = Path.Combine(Path.GetTempPath(), $"FAnsiTests_{Guid.NewGuid():N}");
        var file = Path.Combine(directory, "Recreated");

        try
        {
            var db = server.CreateDatabase(file);
            db.CreateTable("MyTable", [new DatabaseColumnRequest("MyCol", new DatabaseTypeRequest(typeof(int)))]);
            Assert.That(db.DiscoverTables(false), Has.Length.EqualTo(1));

            db.Drop();
            Assert.That(File.Exists(file), Is.False);

            db = server.CreateDatabase(file);
            Assert.Multiple(() =>
            {
                Assert.That(File.Exists(file));
                Assert.That(db.Exists());
                Assert.That(db.DiscoverTables(false), Is.Empty, "Expected a new, empty, database file");
            });

            db.Drop();
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, true);
        }
    }

    /// <summary>
    ///     Dropping a SQLite database whose file has already gone must still close the idle pooled connections to it.
    ///     Otherwise the next connection with the same connection string is one of them: opening it does not create the
    ///     file again, and tables created through it go into the deleted file.  DiscoveredDatabase.Drop refuses a database
    ///     that does not exist, but the helper's DropDatabase, which it calls, is public, and the file can go in between.
    /// </summary>
    [Test]
    [Platform(Exclude = "Win",
        Reason = "Windows cannot delete the file of an open connection, so the file cannot be gone while a pooled connection to it is open")]
    public void Drop_Sqlite_FileAlreadyDeleted_ClosesPooledConnections()
    {
        var server = GetTestServer(DatabaseType.Sqlite);
        var directory = Path.Combine(Path.GetTempPath(), $"FAnsiTests_{Guid.NewGuid():N}");
        var file = Path.Combine(directory, "Deleted.db");

        try
        {
            var db = server.CreateDatabase(file);

            // Leaves an idle connection to the file in the pool
            db.CreateTable("MyTable", [new DatabaseColumnRequest("MyCol", new DatabaseTypeRequest(typeof(int)))]);

            File.Delete(file);
            db.Helper.DropDatabase(db);

            using (var con = db.Server.GetConnection())
                con.Open();

            Assert.That(File.Exists(file), "Expected a connection opened after Drop to create the file again");
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, true);
        }
    }

    /// <summary>
    ///     The name of a SQLite database is the path of its file, and the dot in a name like "fansitests.db" is part of the
    ///     file name.  GetRuntimeName returned "db" for such a database, so Exists looked for a file called "db", and Create
    ///     created one, in the working directory.
    /// </summary>
    [Test]
    public void Create_Sqlite_FileNameWithDot_CreatesThatFile()
    {
        var server = GetTestServer(DatabaseType.Sqlite);
        var directory = Path.Combine(Path.GetTempPath(), $"FAnsiTests_{Guid.NewGuid():N}");
        var file = Path.Combine(directory, "NotYetCreated", "MyDatabase.db");

        try
        {
            var db = server.ExpectDatabase(file);
            Assert.Multiple(() =>
            {
                Assert.That(db.GetRuntimeName(), Is.EqualTo(file));
                Assert.That(db.Exists(), Is.False);
            });

            db.Create();

            Assert.Multiple(() =>
            {
                Assert.That(File.Exists(file), "Expected Create to create the file, and the directory it is in");
                Assert.That(db.Exists());
            });

            db.Drop();
            Assert.That(File.Exists(file), Is.False);
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, true);
        }
    }

    /// <summary>
    ///     SQL has no name for a database file: SQLite calls the database a connection opens main.  GetWrappedName gave the
    ///     path of the file to EnsureWrapped, which rejects the dot in a name like "MyDatabase.db", so it threw for most
    ///     SQLite databases.
    /// </summary>
    [Test]
    public void GetWrappedName_Sqlite_IsMainAndWorksInSql()
    {
        var server = GetTestServer(DatabaseType.Sqlite);
        var directory = Path.Combine(Path.GetTempPath(), $"FAnsiTests_{Guid.NewGuid():N}");
        var file = Path.Combine(directory, "MyDatabase.db");

        try
        {
            var db = server.CreateDatabase(file);
            var tbl = db.CreateTable("MyTable",
                [new DatabaseColumnRequest("MyCol", new DatabaseTypeRequest(typeof(int)))]);
            tbl.Insert(new Dictionary<string, object> { { "MyCol", 1 } });

            var wrapped = db.GetWrappedName();
            Assert.That(wrapped, Is.EqualTo("\"main\""));

            // The name refers to the database in SQL run on its connections
            using (var con = db.Server.GetConnection())
            {
                con.Open();
                using var cmd = db.Server.GetCommand($"SELECT COUNT(*) FROM {wrapped}.{tbl.GetWrappedName()}", con);
                Assert.That(Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture), Is.EqualTo(1));
            }

            db.Drop();
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, true);
        }
    }
}
