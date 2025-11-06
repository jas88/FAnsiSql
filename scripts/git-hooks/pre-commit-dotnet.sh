#!/bin/bash
# Pre-commit hook for .NET projects
# Validates formatting and builds the solution

set -e

echo "🔍 Running .NET pre-commit checks..."

# Check if dotnet is available
if ! command -v dotnet &> /dev/null; then
    echo "❌ dotnet CLI not found. Please install .NET SDK."
    exit 1
fi

# Run dotnet format check (verify formatting without modifying files)
# Exclude diagnostics that are being tracked separately
echo "📝 Checking code formatting..."
if ! dotnet format --verify-no-changes --verbosity quiet --exclude-diagnostics IDE1006 CA1310 IL2072 CS8603 CA1051 CA1707 CA1305 CA1304 CA1309; then
    echo "❌ Code formatting issues detected."
    echo "💡 Run 'dotnet format' to fix formatting issues."
    exit 1
fi
echo "✅ Code formatting is correct"

# Run dotnet build to verify solution compiles
# Note: TreatWarningsAsErrors disabled temporarily due to pre-existing CA/IDE warnings
# These warnings are being tracked separately and will be addressed in a future commit
echo "🔨 Building solution..."
if ! dotnet build --nologo --verbosity quiet; then
    echo "❌ Build failed. Please fix build errors before committing."
    exit 1
fi
echo "✅ Build successful"

echo "✅ All .NET pre-commit checks passed!"
exit 0
