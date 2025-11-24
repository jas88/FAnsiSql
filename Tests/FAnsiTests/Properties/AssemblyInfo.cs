using System.Runtime.InteropServices;
using NUnit.Framework;

// Setting ComVisible to false makes the types in this assembly not visible
// to COM components.  If you need to access a type in this assembly from
// COM, set the ComVisible attribute to true on that type.
[assembly: ComVisible(false)]

// Enable parallel test execution at fixture level (not within fixtures to avoid OneTimeSetUp conflicts)
[assembly: Parallelizable(ParallelScope.Fixtures)]
[assembly: LevelOfParallelism(8)]

// The following GUID is for the ID of the typelib if this project is exposed to COM
[assembly: Guid("acd9fcb4-aa3e-44c7-a27c-c1f5aa92a9e2")]
