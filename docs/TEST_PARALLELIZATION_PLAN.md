# Test Parallelization Improvement Plan

## UPDATED APPROACH (Simplest!)

**Single test assembly** with **per-database test classes** that run in parallel.

### Structure
```
Tests/FAnsi.Tests/
├── BulkCopy/
│   ├── BulkCopyTests_MicrosoftSql.cs    [Parallelizable(ParallelScope.Children)]
│   ├── BulkCopyTests_MySql.cs           [Parallelizable(ParallelScope.Children)]
│   ├── BulkCopyTests_PostgreSql.cs      [Parallelizable(ParallelScope.Children)]
│   ├── BulkCopyTests_Oracle.cs          [Parallelizable(ParallelScope.Children)]
│   └── BulkCopyTests_Sqlite.cs          [Parallelizable(ParallelScope.Children)]
├── TableHelper/
│   ├── TableHelperTests_MicrosoftSql.cs
│   └── ... (per database)
└── Core/
    └── TypeGuesserTests.cs              (database-agnostic)
```

### Pattern

**Current (slow)**:
```csharp
[TestCaseSource(typeof(All), nameof(All.DatabaseTypes))]
public void Upload_SingleRow_Success(DatabaseType type)
{
    var db = GetTestDatabase(type);
    var tbl = db.CreateTable(...);
    // test logic
}
```

**New (fast)**:
```csharp
// BulkCopyTests_MicrosoftSql.cs
[Parallelizable(ParallelScope.Children)]
public class BulkCopyTests_MicrosoftSql : DatabaseTestsBase
{
    [TestCase(DatabaseType.MicrosoftSQLServer)]
    [TestCase(DatabaseType.MicrosoftSQLServer)] // Can add multiple if needed
    public void Upload_SingleRow_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable(...);
        // SHARED test logic (one copy, no duplication!)
    }
}

// BulkCopyTests_MySql.cs
[Parallelizable(ParallelScope.Children)]
public class BulkCopyTests_MySql : DatabaseTestsBase
{
    [TestCase(DatabaseType.MySql)]
    public void Upload_SingleRow_Success(DatabaseType type)
    {
        var db = GetTestDatabase(type);
        var tbl = db.CreateTable(...);
        // SAME test logic (shared via copy, not inheritance)
    }
}
```

### Migration Process
1. Create new class file per database (e.g., `BulkCopyTests_MicrosoftSql.cs`)
2. Copy ALL test methods from `Shared/BulkCopyTests.cs`
3. Replace `[TestCaseSource(...)]` with `[TestCase(DatabaseType.{Specific})]`
4. Remove skip logic for that database (SQLite classes just omit DateTime tests)
5. Add `[Parallelizable(ParallelScope.Children)]` to class
6. Repeat for each database
7. Delete `Shared/` folder

### Benefits
- ✅ **Single assembly** - simpler project structure
- ✅ **Per-class parallelization** - NUnit runs each database class in parallel
- ✅ **No code duplication of logic** - just test method stubs per database
- ✅ **No skip logic** - incompatible tests simply don't exist in that class
- ✅ **Clear organization** - `BulkCopyTests_MySql.cs` obviously contains MySQL tests
- ✅ **Parallel test execution** - all 5 database classes run simultaneously

### NUnit Parallelization
```csharp
[assembly: Parallelizable(ParallelScope.All)]  // Enable parallelization

[Parallelizable(ParallelScope.Children)]  // Tests within this class run in parallel
public class BulkCopyTests_MicrosoftSql
{
    [TestCase(DatabaseType.MicrosoftSQLServer)]
    public void Test1(DatabaseType type) { }

    [TestCase(DatabaseType.MicrosoftSQLServer)]
    public void Test2(DatabaseType type) { }  // Runs in parallel with Test1
}
```

NUnit will run:
- `BulkCopyTests_MicrosoftSql` class in parallel with
- `BulkCopyTests_MySql` class in parallel with
- `BulkCopyTests_PostgreSql` class, etc.

Within each class, all test methods also run in parallel (ParallelScope.Children).

---

# Test Parallelization Improvement Plan (Original)

## Current State

### Structure
- **Shared tests**: `Tests/Shared/` - Database-agnostic test code
- **Database-specific assemblies**: `Tests/FAnsi.{Database}.Tests/`
- Each assembly has `[Parallelizable(ParallelScope.None)]` - sequential execution

### Execution
```
FAnsi.MicrosoftSql.Tests  │  All SQL Server tests run sequentially
FAnsi.MySql.Tests         │  All MySQL tests run sequentially
FAnsi.PostgreSql.Tests    │  All PostgreSQL tests run sequentially
FAnsi.Oracle.Tests        │  All Oracle tests run sequentially
FAnsi.Sqlite.Tests        │  All SQLite tests run sequentially
FAnsi.Core.Tests          │  Core/Guesser tests run sequentially
```

Assemblies run in parallel, but tests within each are sequential.

**Current timing**: ~3-4 minutes total

---

## Proposed Design

### Code Generation Approach

Generate per-database test classes from shared templates using source generators or build-time code generation:

```
Tests/
├── Shared/
│   ├── Templates/
│   │   ├── BulkCopyTests.template.cs      # Template with {DATABASE} placeholders
│   │   ├── TableHelperTests.template.cs
│   │   └── UpdateTests.template.cs
│   └── AgnosticTests/
│       └── TypeGuesserTests.cs             # Database-independent tests
└── Generated/
    ├── MicrosoftSql/
    │   ├── BulkCopyTests.MicrosoftSql.cs  # Generated, [Parallelizable(ParallelScope.All)]
    │   └── TableHelperTests.MicrosoftSql.cs
    ├── MySql/
    │   ├── BulkCopyTests.MySql.cs
    │   └── TableHelperTests.MySql.cs
    └── ... (PostgreSQL, Oracle, Sqlite)
```

### Benefits

1. **True parallelization**: All database tests run simultaneously
2. **No TestCase parameters**: Each class is database-specific
3. **Better isolation**: No shared state between database types
4. **Faster feedback**: Could drop from 3-4min to 1-2min
5. **Cleaner failures**: Database-specific assemblies make failures obvious

### Generator Implementation

**Option 1: Source Generator** (compile-time)
```csharp
[Generator]
public class DatabaseTestGenerator : ISourceGenerator
{
    public void Execute(GeneratorExecutionContext context)
    {
        foreach (var dbType in new[] { "MicrosoftSql", "MySql", "PostgreSql", "Oracle", "Sqlite" })
        {
            var template = LoadTemplate("BulkCopyTests.template.cs");
            var code = template
                .Replace("{DATABASE}", dbType)
                .Replace("{DATABASE_TYPE}", $"DatabaseType.{dbType}");
            context.AddSource($"BulkCopyTests.{dbType}.g.cs", code);
        }
    }
}
```

**Option 2: Build Script** (pre-build code generation)
```bash
# In prebuild.sh
for db in MicrosoftSql MySql PostgreSql Oracle Sqlite; do
    sed "s/{DATABASE}/$db/g" Tests/Shared/Templates/BulkCopyTests.template.cs \
        > Tests/Generated/$db/BulkCopyTests.$db.cs
done
```

**Option 3: T4 Templates** (Visual Studio/MSBuild)
```xml
<ItemGroup>
  <T4Template Include="Tests/Shared/Templates/*.template.tt">
    <Generator>TextTemplatingFileGenerator</Generator>
  </T4Template>
</ItemGroup>
```

---

## Migration Strategy

### Phase 1: Proof of Concept
1. Pick one test file (e.g., `BulkCopyTests.cs`)
2. Create template version with `{DATABASE}` placeholders
3. Generate 5 database-specific classes
4. Verify tests run and pass
5. Measure performance improvement

### Phase 2: Full Migration
1. Convert all test files to templates
2. Remove old `[TestCaseSource]` pattern
3. Update CI to build/run generated tests
4. Update documentation

### Phase 3: Optimization
1. Enable `ParallelScope.All` for generated classes
2. Add resource pooling if needed (database connections)
3. Fine-tune worker thread count

---

## Example Template

**File**: `Tests/Shared/Templates/BulkCopyTests.template.cs`
```csharp
namespace FAnsiTests.{DATABASE};

[Parallelizable(ParallelScope.All)]
public class BulkCopyTests : DatabaseTests
{
    private const DatabaseType DbType = DatabaseType.{DATABASE};

    [Test]
    public void Upload_SingleRow_Success()
    {
        var db = GetTestDatabase(DbType);
        var tbl = db.CreateTable("TestSingleRow", [...]);
        // ... test implementation
    }
}
```

**Generated**: `Tests/Generated/MicrosoftSql/BulkCopyTests.MicrosoftSql.cs`
```csharp
namespace FAnsiTests.MicrosoftSql;

[Parallelizable(ParallelScope.All)]
public class BulkCopyTests : DatabaseTests
{
    private const DatabaseType DbType = DatabaseType.MicrosoftSql;

    [Test]
    public void Upload_SingleRow_Success()
    {
        var db = GetTestDatabase(DbType);
        var tbl = db.CreateTable("TestSingleRow", [...]);
        // ... test implementation
    }
}
```

---

## Special Handling

### Database-Specific Skips
Template can include conditional logic:
```csharp
#if !SQLITE
    [Test]
    public void Test_RequiresDateTime()
    {
        // ... test that requires proper DateTime type
    }
#endif

#if ORACLE
    // Oracle-specific setup (delete before ALTER, etc.)
#endif
```

### Agnostic Tests
Keep TypeGuesser/Core tests separate - don't generate per-database:
```
Tests/Core/
└── TypeGuesserTests.cs  # No database dependency, runs once
```

---

## Performance Estimate

**Current**: ~3-4 minutes
- Build: ~2 min
- Tests: ~1-2 min (sequential within each database)

**After parallelization**: ~1.5-2.5 minutes
- Build: ~2 min (same)
- Tests: ~30-60 sec (all databases in parallel)

**Speedup**: ~40-50% faster test execution

---

## Risks & Mitigations

**Risk 1**: Database resource contention
- **Mitigation**: Connection pooling, limit parallelism if needed

**Risk 2**: Code generation complexity
- **Mitigation**: Start simple (string replacement), iterate

**Risk 3**: Debugging generated code
- **Mitigation**: Keep templates readable, emit line directives

**Risk 4**: Duplicate test maintenance
- **Mitigation**: Templates are single source of truth

---

## Next Steps for Implementation

1. Choose generator approach (recommend Source Generator for tooling integration)
2. Create proof of concept with BulkCopyTests
3. Measure actual performance gain
4. If successful (>30% speedup), proceed with full migration
5. Document template syntax and maintenance process

---

## References

- NUnit Parallelization: https://docs.nunit.org/articles/nunit/writing-tests/attributes/parallelizable.html
- Source Generators: https://learn.microsoft.com/en-us/dotnet/csharp/roslyn-sdk/source-generators-overview
- Current test structure: `Tests/Shared/` (agnostic templates waiting to be formalized)
