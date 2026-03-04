#!/usr/bin/env bash
# collect-artifacts-standalone.sh — Package a cmake install prefix into a release tarball.
#
# Strips debug symbols from libraries and creates a compressed tarball suitable
# for upload as a GitHub Release asset. Optionally extracts debug symbols into
# separate .debug sidecar files (split debug) and packages them as a second tarball.
#
# Usage:
#   collect-artifacts-standalone.sh \
#     --install-prefix /path/to/install \
#     --output /path/to/moxygen-platform.tar.gz \
#     [--debug-output /path/to/moxygen-platform-dbg.tar.gz]
#
# The --debug-output option extracts split debug symbols before stripping.

set -euo pipefail

INSTALL_PREFIX=""
OUTPUT=""
DEBUG_OUTPUT=""

usage() {
  cat <<EOF
Usage: $(basename "$0") --install-prefix DIR --output FILE [--debug-output FILE]

Options:
  --install-prefix DIR   Path to the cmake install prefix
  --output FILE          Output tarball path (must end in .tar.gz)
  --debug-output FILE    Create a separate tarball of unstripped libs before stripping
  -h, --help             Show this help
EOF
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-prefix) INSTALL_PREFIX="$2"; shift 2 ;;
    --output)         OUTPUT="$2"; shift 2 ;;
    --debug-output)   DEBUG_OUTPUT="$2"; shift 2 ;;
    -h|--help)        usage 0 ;;
    *)                echo "Unknown option: $1" >&2; usage 1 ;;
  esac
done

if [[ -z "$INSTALL_PREFIX" || -z "$OUTPUT" ]]; then
  echo "Error: --install-prefix and --output are required." >&2
  usage 1
fi

if [[ ! -d "$INSTALL_PREFIX" ]]; then
  echo "Error: install prefix does not exist: $INSTALL_PREFIX" >&2
  exit 1
fi

# ── Step 1: Report contents ──────────────────────────────────────────────────

echo "==> Install prefix: $INSTALL_PREFIX"
echo "    Directories:"
for d in lib include bin share lib64; do
  if [[ -d "$INSTALL_PREFIX/$d" ]]; then
    COUNT=$(find "$INSTALL_PREFIX/$d" -type f | wc -l)
    echo "      $d/ ($COUNT files)"
  fi
done

PRE_STRIP_SIZE=$(du -sh "$INSTALL_PREFIX" | cut -f1)
echo "    Total size (before strip): $PRE_STRIP_SIZE"

# ── Step 2: Extract split debug symbols and strip ────────────────────────────

echo "==> Stripping debug symbols..."

OS=$(uname -s)
STRIPPED=0
DEBUG_DIR=""

# Set up debug output directory if requested
if [[ -n "$DEBUG_OUTPUT" ]]; then
  DEBUG_DIR=$(mktemp -d)
fi

if [[ "$OS" == "Darwin" ]]; then
  while IFS= read -r -d '' lib; do
    if [[ -n "$DEBUG_DIR" ]]; then
      REL="${lib#$INSTALL_PREFIX/}"
      mkdir -p "$DEBUG_DIR/$(dirname "$REL")"
      # macOS: copy unstripped lib as debug sidecar (no objcopy equivalent)
      cp "$lib" "$DEBUG_DIR/${REL}.debug" 2>/dev/null || true
    fi
    strip -S "$lib" 2>/dev/null && STRIPPED=$((STRIPPED + 1)) || true
  done < <(find "$INSTALL_PREFIX" \( -name '*.a' -o -name '*.dylib' \) -type f -print0)
  echo "    macOS: stripped $STRIPPED libraries"

elif [[ "$OS" == "Linux" ]]; then
  while IFS= read -r -d '' lib; do
    if [[ -n "$DEBUG_DIR" ]]; then
      REL="${lib#$INSTALL_PREFIX/}"
      mkdir -p "$DEBUG_DIR/$(dirname "$REL")"
      # Extract debug sections into sidecar file
      objcopy --only-keep-debug "$lib" "$DEBUG_DIR/${REL}.debug" 2>/dev/null || true
    fi
    if strip --strip-debug "$lib" 2>/dev/null; then
      # Add debuglink so gdb can find the sidecar automatically
      if [[ -n "$DEBUG_DIR" && -f "$DEBUG_DIR/${REL}.debug" ]]; then
        objcopy --add-gnu-debuglink="$DEBUG_DIR/${REL}.debug" "$lib" 2>/dev/null || true
      fi
      STRIPPED=$((STRIPPED + 1))
    fi
  done < <(find "$INSTALL_PREFIX" \( -name '*.a' -o -name '*.so' -o -name '*.so.*' \) -type f -print0)
  echo "    Linux: stripped $STRIPPED libraries"

else
  echo "    Warning: unknown OS '$OS', skipping strip" >&2
fi

POST_STRIP_SIZE=$(du -sh "$INSTALL_PREFIX" | cut -f1)
echo "    Size after strip: $POST_STRIP_SIZE"

# ── Step 3: Create debug tarball ─────────────────────────────────────────────

if [[ -n "$DEBUG_OUTPUT" && -n "$DEBUG_DIR" ]]; then
  DBG_FILE_COUNT=$(find "$DEBUG_DIR" -name '*.debug' -type f | wc -l)
  if [[ "$DBG_FILE_COUNT" -gt 0 ]]; then
    mkdir -p "$(dirname "$DEBUG_OUTPUT")"
    echo "==> Creating debug tarball: $DEBUG_OUTPUT ($DBG_FILE_COUNT debug files)"
    tar czf "$DEBUG_OUTPUT" -C "$DEBUG_DIR" .
    DBG_SIZE=$(du -sh "$DEBUG_OUTPUT" | cut -f1)
    echo "    Debug tarball size: $DBG_SIZE"
  else
    echo "==> No debug files extracted, skipping debug tarball"
  fi
  rm -rf "$DEBUG_DIR"
fi

# ── Step 4: Create release tarball (stripped) ─────────────────────────────────

mkdir -p "$(dirname "$OUTPUT")"

echo "==> Creating tarball: $OUTPUT"
tar czf "$OUTPUT" -C "$INSTALL_PREFIX" .

TARBALL_SIZE=$(du -sh "$OUTPUT" | cut -f1)
echo "    Tarball size: $TARBALL_SIZE"

# Check against GitHub Release 2GB limit (safety net)
TARBALL_BYTES=$(stat --format=%s "$OUTPUT" 2>/dev/null || stat -f%z "$OUTPUT" 2>/dev/null)
LIMIT=$((2 * 1024 * 1024 * 1024))
if [[ "$TARBALL_BYTES" -ge "$LIMIT" ]]; then
  echo "ERROR: Tarball exceeds GitHub Release 2 GiB limit ($TARBALL_SIZE)!" >&2
  exit 1
fi

echo "==> Done: $PRE_STRIP_SIZE -> $POST_STRIP_SIZE (stripped) -> $TARBALL_SIZE (compressed)"
