# Server Tests Code Generation

This directory uses T4 templates to auto-generate test files for all 5 database types.

## How it works

1. **ServerTestsBase.cs** - Contains all test logic as `protected` methods taking `DatabaseType` parameter
2. **ServerTests.tt** - T4 template that reads `ServerTestsBase.cs` and generates test classes
3. **ServerTests.g.cs** - Auto-generated file containing 5 test classes (one per database)

## Adding a new test

Just add a protected method to `ServerTestsBase.cs`:

```csharp
protected void MyNewTest(DatabaseType type)
{
    var server = GetTestServer(type);
    // test logic
}
```

Then regenerate the test files:

```bash
cd Tests/FAnsiTests/Server
t4 ServerTests.tt -o ServerTests.g.cs
```

The T4 template will automatically:
- Generate a `[Test]` method in all 5 database classes calling your method
- Handle methods with `bool` parameters (generates `[TestCase(true)]` and `[TestCase(false)]`)
- Skip SQLite for methods with `[SkipDatabase(DatabaseType.Sqlite)]` attribute

## Filtering tests by database

Use attributes on the protected method in `ServerTestsBase.cs`:

```csharp
[SkipDatabase(DatabaseType.Sqlite, "SQLite is file-based")]
protected void ServerHelper_GetConnectionStringBuilder(DatabaseType type)
{
    // This test will NOT be generated for SQLite
}
```

## Prerequisites

Install the T4 command-line tool:

```bash
dotnet tool install --global dotnet-t4
```
