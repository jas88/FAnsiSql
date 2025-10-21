#!/bin/bash
# Commit message hook to prevent Claude co-authorship
# This prevents AI attribution from appearing in commit history

COMMIT_MSG_FILE="$1"

# If no argument provided, read from stdin (for pre-commit framework)
if [ -z "$COMMIT_MSG_FILE" ]; then
    COMMIT_MSG_FILE=".git/COMMIT_EDITMSG"
fi

# Check if commit message contains Claude co-authorship
if grep -qi "Co-authored-by:.*Claude\|Co-authored-by:.*claude\|Co-authored-by:.*@anthropic.com" "$COMMIT_MSG_FILE" 2>/dev/null; then
    echo "❌ ERROR: Commit message contains Claude co-authorship line"
    echo ""
    echo "Please remove lines like:"
    echo "  Co-authored-by: Claude <noreply@anthropic.com>"
    echo ""
    echo "AI assistance is valuable, but co-authorship should be reserved for human contributors."
    exit 1
fi

exit 0

# Also check PR bodies when called directly with PR text
if [ "$1" != ".git/COMMIT_EDITMSG" ] && [ -n "$1" ]; then
    # Being called with PR body text
    if echo "$1" | grep -qi "Generated with.*Claude\|Co-authored-by:.*Claude\|Co-authored-by:.*@anthropic.com"; then
        echo "❌ ERROR: PR body contains Claude attribution"
        echo ""
        echo "AI assistance is valuable, but attribution should be reserved for human contributors."
        exit 1
    fi
fi
