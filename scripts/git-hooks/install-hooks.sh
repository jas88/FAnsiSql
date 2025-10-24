#!/bin/bash
# Install git hooks for FAnsiSql repository
# Run this script after cloning the repository

set -e

REPO_ROOT="$(git rev-parse --show-toplevel)"
HOOKS_DIR="$REPO_ROOT/.git/hooks"
SCRIPTS_DIR="$REPO_ROOT/scripts/git-hooks"

echo "🔧 Installing git hooks for FAnsiSql..."

# Check if we're in a git repository
if [ ! -d "$REPO_ROOT/.git" ]; then
    echo "❌ Not in a git repository"
    exit 1
fi

# Method 1: Install using pre-commit framework (preferred)
if command -v pre-commit &> /dev/null; then
    echo "✅ Found pre-commit framework"
    echo "📦 Installing pre-commit hooks..."
    cd "$REPO_ROOT"
    pre-commit install --hook-type pre-commit
    pre-commit install --hook-type commit-msg
    echo "✅ pre-commit hooks installed successfully"
    echo ""
    echo "💡 You can run 'pre-commit run --all-files' to test all hooks"
else
    echo "⚠️  pre-commit framework not found"
    echo "📝 Installing as direct git hooks (fallback method)..."

    # Make scripts executable
    chmod +x "$SCRIPTS_DIR"/*.sh

    # Create pre-commit hook
    cat > "$HOOKS_DIR/pre-commit" <<'EOF'
#!/bin/bash
# Pre-commit hook - runs .NET checks
REPO_ROOT="$(git rev-parse --show-toplevel)"
exec "$REPO_ROOT/scripts/git-hooks/pre-commit-dotnet.sh"
EOF

    # Create commit-msg hook
    cat > "$HOOKS_DIR/commit-msg" <<'EOF'
#!/bin/bash
# Commit-msg hook - blocks Claude co-authorship
REPO_ROOT="$(git rev-parse --show-toplevel)"
exec "$REPO_ROOT/scripts/git-hooks/commit-msg-no-claude.sh" "$1"
EOF

    # Make hooks executable
    chmod +x "$HOOKS_DIR/pre-commit"
    chmod +x "$HOOKS_DIR/commit-msg"

    echo "✅ Git hooks installed successfully (direct method)"
    echo ""
    echo "💡 For better hook management, consider installing pre-commit:"
    echo "   pip install pre-commit"
    echo "   Then re-run this script"
fi

echo ""
echo "✅ Setup complete!"
echo ""
echo "🎯 The following hooks are now active:"
echo "   • pre-commit: YAML validation, dotnet format, dotnet build"
echo "   • commit-msg: Block Claude co-authorship"
echo ""
echo "📚 To temporarily bypass hooks (use sparingly):"
echo "   git commit --no-verify"
