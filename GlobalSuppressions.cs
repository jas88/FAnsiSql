using System.Diagnostics.CodeAnalysis;

// Test naming conventions - NUnit uses underscores in test method names
// Example: Test_BulkInserting_LotsOfDates, TestDistincting, etc.
[assembly: SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores",
    Justification = "Test methods follow NUnit naming conventions which use underscores for readability")]

// Public API compatibility - These fields are part of the stable public API
// Converting to properties would be a breaking change for consumers
[assembly: SuppressMessage("Design", "CA1051:Do not declare visible instance fields",
    Justification = "Public fields are part of stable API - changing to properties would break compatibility")]

// Reserved keywords in public API - Cannot rename without breaking changes
// Examples: IQuerySyntaxHelper.True, IQuerySyntaxHelper.False properties
[assembly: SuppressMessage("Naming", "CA1716:Identifiers should not match keywords",
    Justification = "API compatibility - renaming would break existing consumers in other languages")]

// Naming suffixes in public API - Cannot rename without breaking changes
// Examples: AggregateCustomLineCollection, method names ending in 'Impl'
[assembly: SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix",
    Justification = "API compatibility - renaming would break existing consumers")]

// Parameter name mismatches between interface and implementation
// These are minor naming differences that don't affect functionality
[assembly: SuppressMessage("Naming", "CA1725:Parameter names should match base declaration",
    Justification = "Parameter name differences are minor and don't affect API compatibility")]

// CancellationToken parameter order - Would require breaking API changes
[assembly: SuppressMessage("Design", "CA1068:CancellationToken parameters must come last",
    Justification = "Changing parameter order would be a breaking API change")]

// Unnecessary field initialization - Minor performance impact, suppressing for clarity
[assembly: SuppressMessage("Performance", "CA1805:Do not initialize unnecessarily",
    Justification = "Explicit initialization improves code clarity")]

// Static method candidates - Some methods are intentionally instance methods for polymorphism
[assembly: SuppressMessage("Performance", "CA1822:Mark members as static",
    Justification = "Methods are intentionally instance methods to allow for future override/polymorphism")]
