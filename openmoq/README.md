# OpenMOQ Customizations

This directory contains OpenMOQ-specific files that are not part of the upstream
[facebookexperimental/moxygen](https://github.com/facebookexperimental/moxygen) tree.

## Branch Architecture

- **`main`** — Pure upstream mirror. Unmodified copy of the latest green upstream
  commit. No workflows run. Not the default branch.
- **`openmoq-main`** (default) — Working branch with all OpenMOQ customizations.
  Developer PRs go here. Artifacts published from here.

See [GITHUB_WORKFLOW.md](https://github.com/openmoq/o-rly/blob/main/design/GITHUB_WORKFLOW.md)
for the full design.

## Directory Structure

- **`patches/`** — Reserved for post-merge patches if needed (e.g., changes to
  generated files that git merge cannot handle).
- **`scripts/`** — Build and CI scripts extracted from workflows for local testing.

## Workflows

OpenMOQ workflows live on `openmoq-main` and are prefixed `openmoq-` to avoid
collisions with upstream workflow files:

- `openmoq-upstream-sync.yml` — Daily upstream sync: mirrors upstream to `main`,
  creates merge PR to `openmoq-main`, auto-merges on CI pass.
- `openmoq-publish-artifacts.yml` — Builds per-platform artifact bundles on merge
  to `openmoq-main` using the standalone CMake build.
- `openmoq-ci.yml` — Standalone build CI for PRs to `openmoq-main`.

## Scripts

### `openmoq/scripts/create-release.sh`

Creates a GitHub Release from artifact tarballs and prunes old releases.

```bash
openmoq/scripts/create-release.sh \
  --artifacts-dir ./my-artifacts \
  --sha "$(git rev-parse HEAD)" \
  --keep 20 \
  --dry-run
```

## Building Moxygen

### Standalone build (recommended)

```bash
# Install system deps
./standalone/install-system-deps.sh

# Build
cmake -B _build -S standalone -G Ninja
cmake --build _build -j$(nproc)

# Test
ctest --test-dir _build --output-on-failure

# Install
cmake --install _build --prefix /tmp/moxygen-install
```
