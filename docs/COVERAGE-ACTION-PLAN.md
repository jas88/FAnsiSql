# FAnsiSql Coverage Improvement Action Plan

## Current Status
- **Actual Coverage:** 9.0% (NOT 44% as reported on Codecov)
- **Branch Coverage:** 4.3%
- **Files with 0% coverage:** 96 out of 126 (76%)

## Critical Finding: SQLite Has ZERO Coverage

This is the **easiest and highest-impact** quick win. SQLite uses in-memory databases and requires NO external setup. There is no reason for 0% coverage here.

## Quick Wins (Week 1)

### Task 1: Investigate SQLite Test Skipping (Day 1)
**Goal:** Understand why SQLite tests are being skipped

```bash
# Run SQLite tests specifically
cd Tests/FAnsi.Sqlite.Tests
dotnet test --logger "console;verbosity=detailed"

# Check for TestDatabases.xml
ls -la bin/Debug/net9.0/TestDatabases.xml
```

**Expected Issues:**
- Missing `TestDatabases.xml` in build output
- Test setup failure in `DatabaseTests.cs`
- Incorrect database type filtering

**Fix:**
1. Ensure `TestDatabases.xml` is copied to output directory
2. Verify SQLite connection string works
3. Check test fixture setup doesn't skip SQLite

**Impact:** +687 lines coverage (~6.8%)

### Task 2: Add Basic TableHelper Tests (Days 2-3)
**Goal:** Test core table operations for at least one database (SQLite)

Create `Tests/FAnsi.Core.Tests/TableHelperTests.cs`:

```csharp
[TestFixture]
public class TableHelperTests : DatabaseTests
{
    [TestCase(DatabaseType.Sqlite)]
    public void TestCreateTable(DatabaseType dbType)
    {
        var server = GetTestServer(dbType);
        var db = server.ExpectDatabase("TestDB");

        // Create table
        var table = db.CreateTable("TestTable",
            new[] {
                new DatabaseColumnRequest("ID", "int"),
                new DatabaseColumnRequest("Name", "varchar(100)")
            });

        Assert.That(table.Exists(), Is.True);
        Assert.That(table.GetRowCount(), Is.EqualTo(0));
    }

    [TestCase(DatabaseType.Sqlite)]
    public void TestAddColumn(DatabaseType dbType)
    {
        var server = GetTestServer(dbType);
        var db = server.ExpectDatabase("TestDB");
        var table = db.CreateTable("TestTable",
            new[] { new DatabaseColumnRequest("ID", "int") });

        // Add column
        table.AddColumn("NewCol", "varchar(50)", allowNulls: true);

        var columns = table.DiscoverColumns();
        Assert.That(columns, Has.Length.EqualTo(2));
        Assert.That(columns.Any(c => c.GetRuntimeName() == "NewCol"), Is.True);
    }

    [TestCase(DatabaseType.Sqlite)]
    public void TestDropTable(DatabaseType dbType)
    {
        var server = GetTestServer(dbType);
        var db = server.ExpectDatabase("TestDB");
        var table = db.CreateTable("TestTable",
            new[] { new DatabaseColumnRequest("ID", "int") });

        Assert.That(table.Exists(), Is.True);

        table.Drop();

        Assert.That(table.Exists(), Is.False);
    }
}
```

**Impact:** +150-200 lines coverage (~2%)

### Task 3: Add Basic BulkCopy Tests (Days 4-5)
**Goal:** Test bulk insert operations

Create `Tests/FAnsi.Core.Tests/BulkCopyTests.cs`:

```csharp
[TestFixture]
public class BulkCopyTests : DatabaseTests
{
    [TestCase(DatabaseType.Sqlite)]
    public void TestSimpleBulkInsert(DatabaseType dbType)
    {
        var server = GetTestServer(dbType);
        var db = server.ExpectDatabase("TestDB");

        // Create test table
        var table = db.CreateTable("BulkTest",
            new[] {
                new DatabaseColumnRequest("ID", "int"),
                new DatabaseColumnRequest("Name", "varchar(100)")
            });

        // Create test data
        var dt = new DataTable();
        dt.Columns.Add("ID", typeof(int));
        dt.Columns.Add("Name", typeof(string));
        for (int i = 0; i < 1000; i++)
        {
            dt.Rows.Add(i, $"Name_{i}");
        }

        // Bulk insert
        using (var bulk = table.BeginBulkInsert())
        {
            bulk.Upload(dt);
        }

        Assert.That(table.GetRowCount(), Is.EqualTo(1000));
    }

    [TestCase(DatabaseType.Sqlite)]
    public void TestBulkInsertWithColumnMapping(DatabaseType dbType)
    {
        var server = GetTestServer(dbType);
        var db = server.ExpectDatabase("TestDB");

        var table = db.CreateTable("BulkTest",
            new[] {
                new DatabaseColumnRequest("ID", "int"),
                new DatabaseColumnRequest("FullName", "varchar(100)")
            });

        var dt = new DataTable();
        dt.Columns.Add("ID", typeof(int));
        dt.Columns.Add("Name", typeof(string)); // Different column name
        dt.Rows.Add(1, "Test");

        using (var bulk = table.BeginBulkInsert())
        {
            bulk.Mappings.Add(new DataColumnMapping("Name", "FullName"));
            bulk.Upload(dt);
        }

        Assert.That(table.GetRowCount(), Is.EqualTo(1));
    }
}
```

**Impact:** +80-100 lines coverage (~1%)

**Week 1 Total Impact:** ~10% coverage gain (9% → 19%)

## Medium Priority (Weeks 2-4)

### Task 4: Complete TableHelper Tests
**Goal:** Test all table operations across all databases

Add tests for:
- Primary key operations
- Foreign key operations
- Index creation/deletion
- Table truncation
- Column modification
- Constraint handling

**Files to cover:**
- `DiscoveredTableHelper.cs` (220 lines)
- `*TableHelper.cs` for each database (2,168 total lines)

**Impact:** +1,500 lines coverage (~15%)

### Task 5: Complete BulkCopy Tests
**Goal:** Test bulk operations with error handling

Add tests for:
- Transaction support
- Error handling
- Large datasets
- Type conversion
- Null handling

**Files to cover:**
- All `*BulkCopy.cs` files (995 lines)

**Impact:** +700 lines coverage (~7%)

### Task 6: Add AggregateHelper Tests
**Goal:** Test query aggregation

Add tests for:
- Basic GROUP BY
- COUNT, SUM, AVG, MIN, MAX
- HAVING clauses
- Calendar aggregations (day, month, year)
- Custom aggregation lines

**Files to cover:**
- All `*AggregateHelper.cs` files (1,523 lines)

**Impact:** +1,000 lines coverage (~10%)

**Weeks 2-4 Total Impact:** +32% coverage (19% → 51%)

## High Priority (Weeks 5-8)

### Task 7: Connection Management Tests
Test:
- Connection pooling
- Transaction commit/rollback
- Connection disposal
- Thread safety
- Connection string handling

**Files:**
- `ManagedConnectionPool.cs` (190 lines)
- `ManagedConnection.cs` (35 lines)
- `ManagedTransaction.cs` (35 lines)

**Impact:** +200 lines coverage (~2%)

### Task 8: Database Discovery Tests
Test:
- Database listing
- Database creation
- Table discovery
- Column discovery
- Stored procedure discovery
- Foreign key discovery

**Files:**
- `DiscoveredDatabase.cs` (92 lines)
- `DiscoveredDatabaseHelper.cs` (191 lines)
- `DiscoveredServer.cs` (167 lines)
- `DiscoveredColumn.cs` (29 lines)

**Impact:** +400 lines coverage (~4%)

### Task 9: Error Path Testing
Test:
- All custom exceptions
- Error conditions
- Edge cases
- Invalid inputs

**Goal:** Improve branch coverage from 4.3% to 30%

**Impact:** Better reliability, +10% branch coverage

**Weeks 5-8 Total Impact:** +6% line coverage, +26% branch coverage

## Test Infrastructure Improvements

### Docker Compose for Local Testing

Create `docker-compose.yml`:

```yaml
version: '3.8'
services:
  sqlserver:
    image: mcr.microsoft.com/mssql/server:2022-latest
    environment:
      - ACCEPT_EULA=Y
      - SA_PASSWORD=YourStrong!Passw0rd
    ports:
      - "1433:1433"

  mysql:
    image: mysql:8.0
    environment:
      - MYSQL_ROOT_PASSWORD=root
    ports:
      - "3306:3306"

  postgres:
    image: postgres:15
    environment:
      - POSTGRES_PASSWORD=pgpass4291
    ports:
      - "5432:5432"

  oracle:
    image: gvenzl/oracle-xe:21-slim
    environment:
      - ORACLE_PASSWORD=oracle
    ports:
      - "1521:1521"
```

### Test Configuration Template

Create `Tests/FAnsiTests/TestDatabases.template.xml`:

```xml
<?xml version="1.0"?>
<TestDatabases>
  <Settings>
    <AllowDatabaseCreation>True</AllowDatabaseCreation>
    <TestScratchDatabase>FAnsiTests</TestScratchDatabase>
  </Settings>
  <TestDatabase>
    <DatabaseType>MicrosoftSQLServer</DatabaseType>
    <ConnectionString>Data Source=localhost;Encrypt=true;TrustServerCertificate=true;Integrated Security=false;User ID=sa;Password=YourStrong!Passw0rd;</ConnectionString>
  </TestDatabase>
  <TestDatabase>
    <DatabaseType>MySql</DatabaseType>
    <ConnectionString>server=127.0.0.1;Uid=root;Pwd=root;AllowPublicKeyRetrieval=True;ConvertZeroDateTime=true</ConnectionString>
  </TestDatabase>
  <TestDatabase>
    <DatabaseType>Oracle</DatabaseType>
    <ConnectionString>Data Source=localhost:1521/XEPDB1;User Id=system;Password=oracle</ConnectionString>
  </TestDatabase>
  <TestDatabase>
    <DatabaseType>PostgreSql</DatabaseType>
    <ConnectionString>User ID=postgres;Password=pgpass4291;Host=127.0.0.1;Port=5432;Database=postgres</ConnectionString>
  </TestDatabase>
  <TestDatabase>
    <DatabaseType>Sqlite</DatabaseType>
    <ConnectionString>Data Source=:memory:</ConnectionString>
  </TestDatabase>
</TestDatabases>
```

### Development Setup Script

Create `scripts/setup-test-environment.sh`:

```bash
#!/bin/bash
set -e

echo "Setting up FAnsiSql test environment..."

# Start databases
echo "Starting Docker containers..."
docker-compose up -d

# Wait for databases
echo "Waiting for databases to be ready..."
sleep 10

# Wait for SQL Server
until docker exec fansi-sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -Q "SELECT 1" > /dev/null 2>&1
do
  echo "Waiting for SQL Server..."
  sleep 5
done

# Wait for Oracle
until docker exec fansi-oracle sqlplus -S system/oracle@//localhost/XEPDB1 <<< "SELECT 1 FROM DUAL;" > /dev/null 2>&1
do
  echo "Waiting for Oracle..."
  sleep 5
done

# Copy test configuration
echo "Configuring tests..."
cp Tests/FAnsiTests/TestDatabases.template.xml Tests/FAnsiTests/TestDatabases.xml

echo "Test environment ready!"
echo "Run tests with: dotnet test"
```

## CI/CD Improvements

### Add Coverage Quality Gate

Update `.github/workflows/dotnet-core.yml`:

```yaml
- name: Check coverage threshold
  run: |
    # Extract coverage percentage
    COVERAGE=$(grep -oP 'Line coverage: \K[0-9.]+' TestResults/CoverageReport/Summary.txt)
    echo "Current coverage: ${COVERAGE}%"

    # Set minimum threshold
    THRESHOLD=40

    if (( $(echo "$COVERAGE < $THRESHOLD" | bc -l) )); then
      echo "ERROR: Coverage ${COVERAGE}% is below threshold ${THRESHOLD}%"
      exit 1
    fi
```

### Add Coverage PR Comments

Add to workflow:

```yaml
- name: Comment coverage on PR
  if: github.event_name == 'pull_request'
  uses: actions/github-script@v6
  with:
    script: |
      const fs = require('fs');
      const summary = fs.readFileSync('TestResults/CoverageReport/Summary.txt', 'utf8');
      const lines = summary.split('\n');

      const coverage = lines.find(l => l.includes('Line coverage:'));
      const branch = lines.find(l => l.includes('Branch coverage:'));

      github.rest.issues.createComment({
        issue_number: context.issue.number,
        owner: context.repo.owner,
        repo: context.repo.repo,
        body: `## Code Coverage Report\n\n${coverage}\n${branch}`
      });
```

## Tracking Progress

### Coverage Milestones

| Milestone | Target Date | Coverage Goal | Key Deliverables |
|-----------|-------------|---------------|------------------|
| Quick Wins | Week 1 | 19% | SQLite tests, Basic TableHelper, Basic BulkCopy |
| Critical Components | Week 4 | 51% | Complete TableHelper, BulkCopy, AggregateHelper |
| Core Features | Week 8 | 57% | Connection Management, Discovery, Error Paths |
| Production Ready | Week 12 | 65% | All components, edge cases, branch coverage 50% |

### Weekly Review Checklist

- [ ] Run coverage report
- [ ] Update coverage metrics
- [ ] Review uncovered files
- [ ] Identify test gaps
- [ ] Plan next week's tests
- [ ] Update documentation

## Success Criteria

### Definition of Done for Coverage Improvement

✅ **Minimum Coverage Achieved:**
- Line coverage ≥ 65%
- Branch coverage ≥ 50%
- Method coverage ≥ 60%

✅ **No Zero-Coverage Components:**
- All `*TableHelper.cs` files > 60% coverage
- All `*BulkCopy.cs` files > 60% coverage
- All `*AggregateHelper.cs` files > 40% coverage

✅ **Database Parity:**
- All databases (SQLite, MySQL, PostgreSQL, SQL Server, Oracle) > 50% coverage
- No single database < 40% coverage

✅ **Test Infrastructure:**
- Docker Compose setup documented
- Local test environment easy to set up (<5 minutes)
- All tests runnable locally
- CI/CD coverage gates in place

✅ **Documentation:**
- Test setup guide complete
- Coverage report automated
- Contributing guide updated with testing requirements

## Next Steps

1. **This Week:**
   - Fix SQLite test skipping
   - Add 3 basic TableHelper tests
   - Add 2 basic BulkCopy tests

2. **Next Week:**
   - Complete TableHelper test suite
   - Add Docker Compose configuration
   - Update documentation

3. **This Month:**
   - Achieve 30% coverage
   - Set up coverage quality gates
   - Implement test infrastructure improvements

## Resources

### Useful Commands

```bash
# Run all tests with coverage
dotnet test --collect:"XPlat Code Coverage" --settings coverage.runsettings

# Generate coverage report
reportgenerator -reports:"Tests/*/TestResults/*/coverage.info" \
                -targetdir:"TestResults/CoverageReport" \
                -reporttypes:"Html;TextSummary"

# Run specific database tests
dotnet test Tests/FAnsi.Sqlite.Tests

# Start test databases
docker-compose up -d

# Stop test databases
docker-compose down -v
```

### Coverage Tools

- **Report Generator:** `dotnet tool install -g dotnet-reportgenerator-globaltool`
- **Coverage Gutters (VSCode):** Real-time coverage visualization
- **Fine Code Coverage (Visual Studio):** Real-time coverage visualization

### Documentation

- [Testing Guide](../README.md#testing)
- [Coverage Report](../TestResults/CoverageReport/index.html)
- [CI/CD Configuration](../.github/workflows/dotnet-core.yml)
