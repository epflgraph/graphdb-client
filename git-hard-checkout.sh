#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <branch>"
    exit 1
fi

TARGET_BRANCH="$1"

# Make sure we're inside a Git repository.
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "Error: not inside a Git repository."
    exit 1
fi

# Make sure the target branch exists.
if ! git rev-parse --verify "$TARGET_BRANCH" >/dev/null 2>&1; then
    echo "Error: branch '$TARGET_BRANCH' does not exist."
    exit 1
fi

echo "WARNING: This will permanently discard ALL uncommitted changes,"
echo "         untracked files, and ignored files."
echo
echo "Target branch: $TARGET_BRANCH"
echo

read -r -p "Continue? [y/N] " answer
case "$answer" in
    y|Y|yes|YES) ;;
    *) echo "Aborted."; exit 1 ;;
esac

echo "Checking out $TARGET_BRANCH..."
git checkout "$TARGET_BRANCH"

echo "Resetting tracked files..."
git reset --hard HEAD

echo "Removing untracked and ignored files/directories..."
git clean -fdx

echo "Removing remaining empty directories..."
find . \
    -path './.git' -prune -o \
    -type d -empty -delete

echo
echo "Done. Working tree is now:"
git status --short --branch
