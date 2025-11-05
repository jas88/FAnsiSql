using FAnsi.Implementation;
using NUnit.Framework;
using System.Linq;

#pragma warning disable NUnit2022 // IEnumerable<IImplementation> Count analyzer false positive

namespace FAnsiTests;

internal sealed class ImplementationManagerLoadTests
{
    [Test]
    public void Test_LoadAssemblies_FromDirectory()
    {
        Assert.That(ImplementationManager.GetImplementations().Count(), Is.GreaterThanOrEqualTo(3));
    }
}

#pragma warning restore NUnit2022 // IEnumerable<IImplementation> Count analyzer false positive
