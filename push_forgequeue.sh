#!/usr/bin/env bash
#
# push_forgequeue.sh
# Pushes ForgeQueue to GitHub with a clean, logical multi-commit history.
# Commits are backdated to span between Feb 2026 and March 2026.
#
# Usage:
#   1. Unzip ForgeQueue so you have the ForgeQueue-main/ folder.
#   2. Put this script next to it (or edit SRC_DIR below).
#   3. Create an empty repo on GitHub named "ForgeQueue" under your account
#      (or let the gh CLI do it — see the REMOTE section).
#   4. bash push_forgequeue.sh
#
set -euo pipefail

# ---- config ---------------------------------------------------------------
SRC_DIR="."                 # where the unzipped project lives
GH_USER="workswithsatvik"
REPO="ForgeQueue"
# Use SSH (default) or swap for HTTPS: https://github.com/$GH_USER/$REPO.git
REMOTE_URL="git@github.com:${GH_USER}/${REPO}.git"
BRANCH="main"
# ---------------------------------------------------------------------------

if [ ! -d "$SRC_DIR" ]; then
  echo "Can't find $SRC_DIR — edit SRC_DIR at the top of this script." >&2
  exit 1
fi

cd "$SRC_DIR"

# Clean macOS zip cruft if present
rm -rf __MACOSX 2>/dev/null || true
find . -name '._*' -delete 2>/dev/null || true

# Init repo if needed
if [ ! -d .git ]; then
  git init -q
  git branch -M "$BRANCH"
fi

# Pre-defined timeline of dates between Feb 2026 and March 2026
COMMIT_DATES=(
  "2026-02-05T10:15:00"
  "2026-02-09T14:30:00"
  "2026-02-14T09:45:00"
  "2026-02-18T16:20:00"
  "2026-02-23T11:10:00"
  "2026-02-27T13:55:00"
  "2026-03-04T10:05:00"
  "2026-03-09T15:40:00"
  "2026-03-15T09:30:00"
  "2026-03-21T14:15:00"
  "2026-03-26T11:00:00"
  "2026-03-30T16:45:00"
)
COMMIT_INDEX=0

# Helper: stage specific paths and commit with a backdated timestamp.
commit() {
  local msg="$1"; shift
  local staged=0
  for p in "$@"; do
    if [ -e "$p" ]; then git add -- "$p"; staged=1; fi
  done
  
  if [ "$staged" -eq 1 ] && ! git diff --cached --quiet; then
    # Grab the date for this commit step, fallback to end of March if we run out
    local cdate="${COMMIT_DATES[$COMMIT_INDEX]:-2026-03-31T12:00:00}"
    
    GIT_AUTHOR_DATE="$cdate" GIT_COMMITTER_DATE="$cdate" git commit -q -m "$msg"
    echo "  ✓ [$cdate] $msg"
    
    COMMIT_INDEX=$((COMMIT_INDEX + 1))
  fi
}

echo "Building commit history..."

# 1. Scaffolding
commit "chore: project scaffolding and packaging" \
  pyproject.toml LICENSE .gitignore forgequeue/__init__.py

# 2. Schema
commit "feat: postgres schema for the job queue" \
  forgequeue/schema.sql

# 3. Core queue
commit "feat: core queue — enqueue, claim, ack with SKIP LOCKED" \
  forgequeue/queue.py

# 4. Worker
commit "feat: worker loop for processing jobs" \
  forgequeue/worker.py

# 5. CLI
commit "feat: command-line interface" \
  forgequeue/cli.py

# 6. Tests
commit "test: queue test suite and fixtures" \
  tests/

# 7. CI
commit "ci: github actions workflow for tests" \
  .github/

# 8. Local dev tooling
commit "chore: docker compose for local postgres" \
  compose.yaml

# 9. Benchmarks
commit "perf: throughput benchmarks" \
  benchmarks/

# 10. Docs
commit "docs: architecture notes and README" \
  docs/ README.md

# Sweep up anything not explicitly grouped above
if ! git diff --cached --quiet || [ -n "$(git status --porcelain)" ]; then
  git add -A
  if ! git diff --cached --quiet; then
    cdate="${COMMIT_DATES[$COMMIT_INDEX]:-2026-03-31T12:00:00}"
    GIT_AUTHOR_DATE="$cdate" GIT_COMMITTER_DATE="$cdate" git commit -q -m "chore: remaining project files"
    echo "  ✓ [$cdate] chore: remaining project files"
  fi
fi

# ---- remote & push --------------------------------------------------------
# If you have the gh CLI and the repo doesn't exist yet, uncomment:
# gh repo create "$GH_USER/$REPO" --public --source=. --remote=origin --push && exit 0

if git remote get-url origin >/dev/null 2>&1; then
  git remote set-url origin "$REMOTE_URL"
else
  git remote add origin "$REMOTE_URL"
fi

echo "Pushing to $REMOTE_URL ..."
git push -u origin "$BRANCH"
echo "Done."