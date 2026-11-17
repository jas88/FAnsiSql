using FAnsi;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal;
using NUnit.Framework.Internal.Commands;

namespace FAnsiTests.TestGeneration;

/// <summary>
///     Reports a test case as ignored, without running it or its SetUp and TearDown, when any of its
///     <see cref="DatabaseType" /> arguments is excluded.  This is how <see cref="SkipDatabaseAttribute" /> and
///     <see cref="OnlyDatabaseAttribute" /> are honoured at run time.
/// </summary>
internal sealed class DatabaseTypeFilterCommand(
    TestCommand command,
    Func<DatabaseType, bool> isExcluded,
    string? reason)
    : DelegatingTestCommand(command)
{
    public override TestResult Execute(TestExecutionContext context)
    {
        foreach (var argument in context.CurrentTest.Arguments)
            if (argument is DatabaseType type && isExcluded(type))
            {
                context.CurrentResult.SetResult(ResultState.Ignored, reason ?? $"Not run on {type}");
                return context.CurrentResult;
            }

        return innerCommand.Execute(context);
    }
}
