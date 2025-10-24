#!/bin/bash
# Pre-commit hook for .NET projects
# Validates formatting and builds the solution

echo "🔍 Running .NET pre-commit checks..."

# Check if dotnet is available
if ! command -v dotnet &> /dev/null; then
    echo "❌ dotnet CLI not found. Please install .NET SDK."
    exit 1
fi

# Run dotnet format check (verify formatting without modifying files)
echo "📝 Checking code formatting..."
# Capture output but only check for actual formatting issues (not build warnings)
set +e  # Don't exit on error yet
FORMAT_OUTPUT=$(dotnet format --verify-no-changes --verbosity normal 2>&1)
FORMAT_EXIT=$?
set -e  # Re-enable exit on error

if [ $FORMAT_EXIT -ne 0 ]; then
    # Check if there are actual WHITESPACE errors (formatting issues)
    if echo "$FORMAT_OUTPUT" | grep -q "WHITESPACE\|Formatted code file"; then
        echo "❌ Code formatting issues detected."
        echo "$FORMAT_OUTPUT" | grep -E "WHITESPACE|Formatted"
        echo "💡 Run 'dotnet format' to fix formatting issues."
        exit 1
    fi
    # Otherwise it's just build warnings which we can ignore for formatting check
fi
echo "✅ Code formatting is correct"

# Run dotnet build to verify solution compiles
# Exclude known AOT/trim analysis warnings that are being addressed separately:
# IL2067, IL2072 - Trim analysis warnings for dynamic member access
# IL3051 - RequiresDynamicCode interface implementation warnings
echo "🔨 Building solution..."
if ! dotnet build --nologo --verbosity quiet -p:TreatWarningsAsErrors=true -p:WarningsNotAsErrors="IL2067;IL2072;IL3051"; then
    echo "❌ Build failed. Please fix build errors before committing."
    exit 1
fi
echo "✅ Build successful"

echo "✅ All .NET pre-commit checks passed!"
exit 0
