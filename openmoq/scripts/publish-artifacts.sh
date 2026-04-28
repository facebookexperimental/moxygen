#!/usr/bin/env bash
# publish-artifacts.sh — Publish build artifacts as a rolling GitHub pre-release.
#
# Creates (or replaces) a `snapshot-latest` pre-release tagged at the given
# commit SHA, uploading all .tar.gz files from the artifacts directory.
#
# Usage:
#   publish-artifacts.sh \
#     --artifacts-dir ./release-assets \
#     --sha <full-commit-sha>
#
# Requires: gh CLI authenticated with a token that has contents:write.

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────

ARTIFACTS_DIR=""
SHA=""
TAG="snapshot-latest"
BRANCH="main"
REPO=""  # defaults to current repo if empty
DRY_RUN=false

# ── Argument parsing ─────────────────────────────────────────────────────────

usage() {
  cat <<EOF
Usage: $(basename "$0") --artifacts-dir DIR --sha SHA [OPTIONS]

Options:
  --artifacts-dir DIR   Directory containing .tar.gz artifact files
  --sha SHA             Full commit SHA for the release
  --tag TAG             Pre-release tag name (default: snapshot-latest)
  --branch BRANCH       Source branch name for release notes (default: main)
  --repo OWNER/REPO     GitHub repository (default: current repo from gh)
  --dry-run             Show what would be done without creating the release
  -h, --help            Show this help
EOF
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --artifacts-dir) ARTIFACTS_DIR="$2"; shift 2 ;;
    --sha)           SHA="$2"; shift 2 ;;
    --tag)           TAG="$2"; shift 2 ;;
    --branch)        BRANCH="$2"; shift 2 ;;
    --repo)          REPO="$2"; shift 2 ;;
    --dry-run)       DRY_RUN=true; shift ;;
    -h|--help)       usage 0 ;;
    *)               echo "Unknown option: $1" >&2; usage 1 ;;
  esac
done

if [[ -z "$ARTIFACTS_DIR" || -z "$SHA" ]]; then
  echo "Error: --artifacts-dir and --sha are required." >&2
  usage 1
fi

if [[ ! -d "$ARTIFACTS_DIR" ]]; then
  echo "Error: artifacts directory does not exist: $ARTIFACTS_DIR" >&2
  exit 1
fi

REPO_FLAG=""
if [[ -n "$REPO" ]]; then
  REPO_FLAG="--repo $REPO"
fi

SHORT_SHA="${SHA:0:7}"

# ── Step 1: Collect artifact files ───────────────────────────────────────────

echo "==> Collecting artifacts from: $ARTIFACTS_DIR"

# download-artifact@v4 creates a subdirectory per artifact name.
# Flatten: find all .tar.gz files regardless of nesting depth.
RELEASE_DIR=$(mktemp -d)
trap 'rm -rf "$RELEASE_DIR"' EXIT

ASSET_COUNT=0
while IFS= read -r -d '' tarball; do
  cp "$tarball" "$RELEASE_DIR/"
  ASSET_COUNT=$((ASSET_COUNT + 1))
  SIZE=$(du -sh "$tarball" | cut -f1)
  echo "    $(basename "$tarball"): $SIZE"
done < <(find "$ARTIFACTS_DIR" -name '*.tar.gz' -type f -print0)

if [[ "$ASSET_COUNT" -eq 0 ]]; then
  echo "Error: no .tar.gz files found in $ARTIFACTS_DIR" >&2
  exit 1
fi

echo "    Found $ASSET_COUNT artifact(s)"

# ── Step 2: Upload with retry ────────────────────────────────────────────────

upload_with_retry() {
  local asset="$1"
  local name
  name=$(basename "$asset")
  local max=3 delay=10 attempt=1
  while [[ $attempt -le $max ]]; do
    # shellcheck disable=SC2086
    if gh release upload "$TAG" "$asset" --clobber $REPO_FLAG; then
      return 0
    fi
    if [[ $attempt -lt $max ]]; then
      echo "    Upload failed (attempt $attempt/$max), retrying in ${delay}s..."
      sleep "$delay"
      delay=$((delay * 2))
    fi
    attempt=$((attempt + 1))
  done
  echo "    ERROR: $name upload failed after $max attempts" >&2
  return 1
}

# ── Step 3: Create/replace snapshot-latest ────────────────────────────────────

echo "==> Publishing snapshot: $TAG (commit $SHORT_SHA)"

if [[ "$DRY_RUN" == true ]]; then
  echo "    [dry-run] Would delete existing release $TAG"
  echo "    [dry-run] Would create pre-release $TAG with $ASSET_COUNT assets"
else
  # Delete existing snapshot release and tag
  # shellcheck disable=SC2086
  gh release delete "$TAG" --yes $REPO_FLAG 2>/dev/null || true
  git tag -d "$TAG" 2>/dev/null || true
  git push origin ":refs/tags/$TAG" 2>/dev/null || true

  # Create as pre-release so it doesn't show as "Latest release"
  # shellcheck disable=SC2086
  gh release create "$TAG" \
    --target "$SHA" \
    --title "Latest build — ${BRANCH} (${SHORT_SHA})" \
    --prerelease \
    --notes "$(cat <<EOF
Rolling snapshot of the latest build from \`${BRANCH}\`.

**Commit:** \`${SHA}\`
**Built:** $(date -u +%Y-%m-%dT%H:%M:%SZ)

This pre-release is automatically replaced on every push to \`${BRANCH}\`.

Each platform has two tarballs:
- \`moxygen-<platform>.tar.gz\` — stripped release build
- \`moxygen-<platform>-dbg.tar.gz\` — split debug symbols (.debug sidecar files)
EOF
    )" \
    $REPO_FLAG

  # Upload each asset individually with retry
  for asset in "$RELEASE_DIR"/*.tar.gz; do
    echo "    Uploading $(basename "$asset")..."
    upload_with_retry "$asset"
  done

  # Ensure the release is not stuck as draft
  # (gh release create without files may leave it in draft state)
  RELEASE_ID=$(gh api repos/{owner}/{repo}/releases \
    --jq ".[] | select(.tag_name == \"$TAG\") | .id")
  if [[ -n "$RELEASE_ID" ]]; then
    gh api "repos/{owner}/{repo}/releases/$RELEASE_ID" \
      -X PATCH -f draft=false >/dev/null
    echo "    Release published (draft=false)"
  fi

  echo "    Snapshot published: $TAG"
fi

echo "==> Done."
