#!/usr/bin/env bash
# create-release.sh — Create a GitHub Release from artifact tarballs.
#
# Collects .tar.gz files from an artifacts directory, creates (or replaces)
# a GitHub Release tagged build-<sha>, and prunes old build releases.
#
# Usage:
#   create-release.sh \
#     --artifacts-dir ./release-assets \
#     --sha <full-commit-sha> \
#     --keep 20
#
# Requires: gh CLI authenticated with a token that has contents:write.

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────

ARTIFACTS_DIR=""
SHA=""
KEEP=20
REPO=""  # defaults to current repo if empty
DRY_RUN=false

# ── Argument parsing ─────────────────────────────────────────────────────────

usage() {
  cat <<EOF
Usage: $(basename "$0") --artifacts-dir DIR --sha SHA [OPTIONS]

Options:
  --artifacts-dir DIR   Directory containing .tar.gz artifact files
  --sha SHA             Full commit SHA for the release tag
  --keep N              Number of old build releases to keep (default: 20)
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
    --keep)          KEEP="$2"; shift 2 ;;
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

# ── Step 2: Create release ───────────────────────────────────────────────────

SHORT_SHA="${SHA:0:12}"
TAG="build-${SHORT_SHA}"

# Upload a single asset with retry on transient failures (502/404 on large files).
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

echo "==> Creating release: $TAG"

if [[ "$DRY_RUN" == true ]]; then
  echo "    [dry-run] Would delete existing release $TAG"
  echo "    [dry-run] Would create release $TAG with $ASSET_COUNT assets"
else
  # Delete existing release with this tag if it exists (idempotent)
  # shellcheck disable=SC2086
  gh release delete "$TAG" --yes $REPO_FLAG 2>/dev/null || true
  git tag -d "$TAG" 2>/dev/null || true
  git push origin ":refs/tags/$TAG" 2>/dev/null || true

  # Create the release (no assets yet — upload separately for per-asset retry)
  # shellcheck disable=SC2086
  gh release create "$TAG" \
    --title "Build artifacts for ${SHORT_SHA}" \
    --notes "$(cat <<EOF
Automated build artifacts for commit \`${SHORT_SHA}\`.

These bundles contain moxygen and all transitive Meta dependencies
(folly, fizz, wangle, mvfst, proxygen) as static libraries with
headers and CMake config files.

Each platform has two tarballs:
- \`moxygen-<platform>.tar.gz\` — stripped release build
- \`moxygen-<platform>-dbg.tar.gz\` — split debug symbols (.debug sidecar files)

**Usage in o-rly CI:**
\`\`\`bash
gh release download "${TAG}" --repo openmoq/moxygen \\
  --pattern "moxygen-<platform>.tar.gz" --dir .scratch/
tar xzf .scratch/moxygen-*.tar.gz -C .scratch/
\`\`\`
EOF
    )" \
    $REPO_FLAG

  # Upload each asset individually with retry
  for asset in "$RELEASE_DIR"/*.tar.gz; do
    echo "    Uploading $(basename "$asset")..."
    upload_with_retry "$asset"
  done

  echo "    Release created: $TAG"
fi

# ── Step 3: Prune old releases ───────────────────────────────────────────────

echo "==> Pruning old build releases (keeping last $KEEP)..."

# shellcheck disable=SC2086
OLD_RELEASES=$(gh release list --limit 100 $REPO_FLAG \
  | grep '^build-' \
  | tail -n +$((KEEP + 1)) \
  | awk '{print $1}') || true

if [[ -z "$OLD_RELEASES" ]]; then
  echo "    Nothing to prune"
else
  echo "$OLD_RELEASES" | while read -r tag; do
    if [[ "$DRY_RUN" == true ]]; then
      echo "    [dry-run] Would delete: $tag"
    else
      echo "    Deleting: $tag"
      # shellcheck disable=SC2086
      gh release delete "$tag" --yes --cleanup-tag $REPO_FLAG
    fi
  done
fi

echo "==> Done."
