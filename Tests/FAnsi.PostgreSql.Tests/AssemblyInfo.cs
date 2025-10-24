using NUnit.Framework;

// Disable parallel execution at assembly level to prevent tests from interfering
// Tests share the same FAnsiTests database and must run sequentially
[assembly: LevelOfParallelism(1)]
[assembly: Parallelizable(ParallelScope.None)]
