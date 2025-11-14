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
# Exclude pre-existing warnings (IDE1006, CS8603)
echo "📝 Checking code formatting..."
if ! dotnet format --verify-no-changes --verbosity quiet --exclude-diagnostics IDE1006 CS8603; then
    echo "❌ Code formatting issues detected."
    echo "💡 Run 'dotnet format' to fix formatting issues."
    exit 1
fi
echo "✅ Code formatting is correct"

# Run dotnet build to verify solution compiles
# Note: We don't use TreatWarningsAsErrors here because some pre-existing warnings
# (IDE1006 naming, CS8603 nullable) would fail the build. CI handles this separately.
echo "🔨 Building solution..."
if ! dotnet build --nologo --verbosity quiet; then
    echo "❌ Build failed. Please fix build errors before committing."
    exit 1
fi
echo "✅ Build successful"

echo "✅ All .NET pre-commit checks passed!"
exit 0
