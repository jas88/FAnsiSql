#!/usr/bin/env python3
"""
Apply conditional compilation to TestCase attributes based on DatabaseType.

This script wraps [TestCase(DatabaseType.X, ...)] attributes with
#if X_TESTS directives to ensure tests only run for the appropriate RDBMS.
"""

import re
import sys
from pathlib import Path
from typing import List, Tuple

# Mapping from DatabaseType enum values to conditional compilation symbols
DB_TYPE_TO_SYMBOL = {
    'MicrosoftSQLServer': 'MSSQL_TESTS',
    'MySql': 'MYSQL_TESTS',
    'Oracle': 'ORACLE_TESTS',
    'PostgreSql': 'POSTGRESQL_TESTS',
    'Sqlite': 'SQLITE_TESTS',
}

def find_testcase_end(lines: List[str], start_index: int) -> int:
    """
    Find the end of a TestCase attribute, handling multi-line attributes.
    Returns the index of the line containing the closing bracket ']'.
    """
    # Count brackets to handle nested brackets in string literals
    bracket_depth = 0
    in_string = False
    in_verbatim_string = False
    escape_next = False

    for i in range(start_index, len(lines)):
        line = lines[i]

        j = 0
        while j < len(line):
            char = line[j]

            # Handle verbatim strings (@"...)
            if char == '@' and j + 1 < len(line) and line[j + 1] == '"' and not in_string and not in_verbatim_string:
                in_verbatim_string = True
                j += 2  # Skip both @ and opening "
                continue

            # Handle regular strings
            if char == '"' and not in_verbatim_string:
                if not escape_next:
                    in_string = not in_string
                escape_next = False
                j += 1
                continue

            # Handle verbatim string end (double quote escapes quote in verbatim strings)
            if char == '"' and in_verbatim_string:
                # Check if it's escaped (doubled quote)
                if j + 1 < len(line) and line[j + 1] == '"':
                    j += 2  # Skip the escaped quote
                    continue
                else:
                    in_verbatim_string = False
                    j += 1
                    continue

            # Handle escaping in regular strings
            if char == '\\' and in_string and not in_verbatim_string:
                escape_next = True
                j += 1
                continue

            # Only count brackets outside strings
            if not in_string and not in_verbatim_string:
                if char == '[':
                    bracket_depth += 1
                elif char == ']':
                    bracket_depth -= 1
                    if bracket_depth == 0:
                        return i

            escape_next = False
            j += 1

    # If we get here, we didn't find the closing bracket
    return start_index

def process_file(file_path: Path) -> Tuple[int, bool]:
    """
    Process a single file, wrapping TestCase attributes with conditional compilation.

    Returns: (count of TestCase attributes wrapped, whether file was modified)
    """
    try:
        content = file_path.read_text(encoding='utf-8')
    except Exception as e:
        print(f"Error reading {file_path}: {e}", file=sys.stderr)
        return 0, False

    # Check if file already has conditional compilation directives
    if '#if MSSQL_TESTS' in content or '#if MYSQL_TESTS' in content:
        print(f"Skipping {file_path.name}: already has conditional compilation directives")
        return 0, False

    original_content = content
    lines = content.split('\n')
    new_lines = []
    count = 0
    i = 0

    while i < len(lines):
        line = lines[i]

        # Check if this line is a TestCase attribute with DatabaseType
        # Match both with comma (multiple params) and without (single param)
        match = re.match(r'^(\s*)\[TestCase\(DatabaseType\.(\w+)(?:,|\))', line)

        if match:
            indent = match.group(1)
            db_type = match.group(2)

            if db_type in DB_TYPE_TO_SYMBOL:
                symbol = DB_TYPE_TO_SYMBOL[db_type]

                # Find the end of the TestCase attribute
                end_line_index = find_testcase_end(lines, i)

                # Add the #if directive before the TestCase
                new_lines.append(f'{indent}#if {symbol}')

                # Add all lines of the TestCase attribute
                for j in range(i, end_line_index + 1):
                    new_lines.append(lines[j])

                # Add the #endif after the TestCase
                new_lines.append(f'{indent}#endif')

                count += 1
                i = end_line_index + 1
                continue

        new_lines.append(line)
        i += 1

    new_content = '\n'.join(new_lines)
    modified = new_content != original_content

    if modified:
        try:
            file_path.write_text(new_content, encoding='utf-8')
        except Exception as e:
            print(f"Error writing {file_path}: {e}", file=sys.stderr)
            return count, False

    return count, modified

def main():
    # Find all test files
    base_path = Path(__file__).parent.parent
    test_paths = [
        base_path / 'Tests' / 'Shared',
        base_path / 'Tests' / 'FAnsiTests',
    ]

    total_count = 0
    files_modified = 0
    files_processed = 0

    for test_path in test_paths:
        if not test_path.exists():
            print(f"Warning: {test_path} does not exist", file=sys.stderr)
            continue

        for cs_file in test_path.rglob('*.cs'):
            # Skip obj directories
            if '/obj/' in str(cs_file) or '\\obj\\' in str(cs_file):
                continue

            count, modified = process_file(cs_file)

            if count > 0:
                files_processed += 1
                print(f"{cs_file.relative_to(base_path)}: {count} TestCase attributes wrapped")

                if modified:
                    files_modified += 1

                total_count += count

    print(f"\nSummary:")
    print(f"  Files processed: {files_processed}")
    print(f"  Files modified: {files_modified}")
    print(f"  Total TestCase attributes wrapped: {total_count}")

if __name__ == '__main__':
    main()
