#!/usr/bin/env perl
use strict;
use warnings;
use v5.10;
use Getopt::Long;

# Configuration
my $fix = 0;
my $repo = 'jas88/FAnsiSql';
my $help = 0;

GetOptions(
    'fix' => \$fix,
    'repo=s' => \$repo,
    'help' => \$help,
) or die "Error in command line arguments\n";

if ($help) {
    print <<'HELP';
Usage: lint-changelog.pl [options] [CHANGELOG.md]

Options:
  --fix           Fix issues automatically and write back to file
  --repo=OWNER/REPO  GitHub repository (default: jas88/FAnsiSql)
  --help          Show this help message

Examples:
  # Validate only (dry-run)
  ./lint-changelog.pl CHANGELOG.md

  # Validate and fix issues
  ./lint-changelog.pl --fix CHANGELOG.md

  # Use different repository
  ./lint-changelog.pl --fix --repo=HicServices/FAnsiSql CHANGELOG.md
HELP
    exit 0;
}

my $file = shift @ARGV || 'CHANGELOG.md';
die "File not found: $file\n" unless -f $file;

# Read the file
open my $fh, '<', $file or die "Cannot open $file: $!\n";
my @lines = <$fh>;
chomp @lines;
close $fh;

# Track issues
my @issues;
my $modified = 0;

# Detect and fix trailing whitespace
my $has_trailing_ws = 0;
for my $i (0 .. $#lines) {
    if ($lines[$i] =~ /\s+$/) {
        $has_trailing_ws = 1;
        if ($fix) {
            $lines[$i] =~ s/\s+$//;
            $modified = 1;
        } else {
            push @issues, sprintf("Line %d: Trailing whitespace detected", $i + 1);
        }
    }
}
if ($has_trailing_ws && $fix) {
    say "Fixed: Removed trailing whitespace";
}

# Parse versions from headers
my @versions;
my %version_lines;
my %version_dates;

for my $i (0 .. $#lines) {
    my $line = $lines[$i];

    # Match version headers: ## [3.6.0] - 2025-11-27
    if ($line =~ /^##\s+\[([^\]]+)\]\s*(?:-\s*(.+))?/) {
        my ($version, $date) = ($1, $2);

        next if $version eq 'Unreleased';

        push @versions, $version;
        $version_lines{$version} = $i;

        if ($date) {
            $version_dates{$version} = $date;

            # Validate date format
            unless ($date =~ /^\d{4}-\d{2}-\d{2}$/) {
                push @issues, "Line " . ($i + 1) . ": Invalid date format '$date' (should be YYYY-MM-DD)";
            }

            # Validate date is reasonable
            if ($date =~ /^(\d{4})-(\d{2})-(\d{2})$/) {
                my ($year, $month, $day) = ($1, $2, $3);
                if ($year < 2015 || $year > 2030) {
                    push @issues, "Line " . ($i + 1) . ": Suspicious year $year in date";
                }
                if ($month < 1 || $month > 12) {
                    push @issues, "Line " . ($i + 1) . ": Invalid month $month in date";
                }
                if ($day < 1 || $day > 31) {
                    push @issues, "Line " . ($i + 1) . ": Invalid day $day in date";
                }
            }
        } else {
            push @issues, "Line " . ($i + 1) . ": Version [$version] missing date";
        }
    }
}

# Find link definitions section
my $links_start = -1;
my %existing_links;

for my $i (0 .. $#lines) {
    my $line = $lines[$i];

    # Match link definitions: [3.6.0]: https://github.com/...
    if ($line =~ /^\[([^\]]+)\]:\s*(.+)$/) {
        my ($version, $url) = ($1, $2);

        if ($links_start == -1) {
            $links_start = $i;
        }

        $existing_links{$version} = {
            url => $url,
            line => $i,
        };

        # Validate URL format
        unless ($url =~ m{^https://github\.com/[^/]+/[^/]+/compare/}) {
            push @issues, "Line " . ($i + 1) . ": Invalid diff URL format for [$version]";
        }

        # Check for wrong repository
        unless ($url =~ m{^https://github\.com/$repo/compare/}) {
            my $old_repo = $url;
            $old_repo =~ s{^https://github\.com/([^/]+/[^/]+)/compare/.*}{$1};

            if ($old_repo ne $repo) {
                push @issues, "Line " . ($i + 1) . ": Wrong repository '$old_repo' (should be '$repo') for [$version]";

                if ($fix) {
                    # Extract the tags from the URL
                    if ($url =~ m{/compare/([^/]+\.\.\.[^/]+)$}) {
                        my $tags = $1;
                        my $new_url = "https://github.com/$repo/compare/$tags";
                        $lines[$i] = "[$version]: $new_url";
                        $modified = 1;
                        say "Fixed: Updated repository for [$version] to $repo";
                    }
                }
            }
        }

        # Validate tag format consistency
        if ($url =~ m{/compare/([^/.]+)\.\.\.([^/]+)$}) {
            my ($from_tag, $to_tag) = ($1, $2);

            # Check for v prefix consistency
            my $from_has_v = ($from_tag =~ /^v/);
            my $to_has_v = ($to_tag =~ /^v/);

            if ($from_has_v != $to_has_v) {
                push @issues, "Line " . ($i + 1) . ": Inconsistent 'v' prefix in tags: $from_tag vs $to_tag";

                if ($fix) {
                    # Prefer 'v' prefix for both
                    $from_tag = "v$from_tag" unless $from_has_v;
                    $to_tag = "v$to_tag" unless $to_has_v;

                    my $new_url = "https://github.com/$repo/compare/$from_tag...$to_tag";
                    $lines[$i] = "[$version]: $new_url";
                    $modified = 1;
                    say "Fixed: Normalized tags to use 'v' prefix for [$version]";
                }
            }
        }
    }
}

# Generate missing links
if (@versions) {
    # Find missing links
    my @missing;
    for my $version (@versions) {
        unless (exists $existing_links{$version}) {
            push @missing, $version;
            push @issues, "Missing diff link for version [$version]";
        }
    }

    if (@missing && $fix) {
        # Determine where to insert (at end or after last link)
        my $insert_pos = $links_start >= 0 ? $links_start : $#lines + 1;

        # Find the position of the last existing link
        my $last_link_pos = -1;
        for my $version (keys %existing_links) {
            my $pos = $existing_links{$version}{line};
            $last_link_pos = $pos if $pos > $last_link_pos;
        }

        if ($last_link_pos >= 0) {
            $insert_pos = $last_link_pos + 1;
        }

        # Generate links for missing versions
        my @new_links;

        for my $i (0 .. $#missing) {
            my $version = $missing[$i];

            # Determine previous version for compare link
            my $version_idx = 0;
            for my $j (0 .. $#versions) {
                if ($versions[$j] eq $version) {
                    $version_idx = $j;
                    last;
                }
            }

            my $prev_version = $version_idx < $#versions ? $versions[$version_idx + 1] : undef;

            if ($prev_version) {
                # Generate diff link
                my $from_tag = "v$prev_version";
                my $to_tag = "v$version";
                my $url = "https://github.com/$repo/compare/$from_tag...$to_tag";

                push @new_links, "[$version]: $url";
                say "Generated: diff link for [$version]";
            } else {
                # First version - no previous version to compare
                say "Skipped: No previous version for [$version] (first release)";
            }
        }

        if (@new_links) {
            # Insert new links at the appropriate position
            splice @lines, $insert_pos, 0, @new_links;
            $modified = 1;
        }
    }
}

# Check for Unreleased link
unless (exists $existing_links{'Unreleased'}) {
    push @issues, "Missing [Unreleased] link";

    if ($fix && @versions) {
        # Add Unreleased link comparing HEAD to latest version
        my $latest = $versions[0];
        my $url = "https://github.com/$repo/compare/v$latest...HEAD";

        # Insert at the beginning of links section
        my $insert_pos = $links_start >= 0 ? $links_start : $#lines + 1;
        splice @lines, $insert_pos, 0, "[Unreleased]: $url";
        $modified = 1;
        say "Generated: [Unreleased] link";
    }
}

# Report issues
if (@issues) {
    say "\n=== CHANGELOG Issues Found ===";
    say $_ for @issues;
    say "\nTotal issues: " . scalar(@issues);

    if ($fix && $modified) {
        say "\n=== Writing fixes to $file ===";
        open my $out, '>', $file or die "Cannot write to $file: $!\n";
        print $out join("\n", @lines) . "\n";
        close $out;
        say "File updated successfully!";
        exit 0;
    } elsif ($fix) {
        say "\nNo automatic fixes available for these issues.";
        exit 1;
    } else {
        say "\nRun with --fix to automatically fix issues.";
        exit 1;
    }
} else {
    say "✅ CHANGELOG is valid!";
    exit 0;
}
