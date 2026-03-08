# OpenMOQ Customizations

This directory contains OpenMOQ-specific files that are not part of the upstream
[facebookexperimental/moxygen](https://github.com/facebookexperimental/moxygen) tree.

## Branch Architecture

- **`upstream`** — Pure upstream mirror. Unmodified copy of the latest green upstream
  commit. No workflows run.
- **`main`** (default) — Working branch with all OpenMOQ customizations.
  Developer PRs go here. Artifacts published from here.

See [GITHUB_WORKFLOW.md](https://github.com/openmoq/o-rly/blob/main/design/GITHUB_WORKFLOW.md)
for the full design.

## Directory Structure

- **`patches/`** — Reserved for post-merge patches if needed (e.g., changes to
  generated files that git merge cannot handle).
- **`scripts/`** — Build and CI scripts extracted from workflows for local testing.

## Workflows

OpenMOQ workflows live on `main` and are prefixed `omoq-` to avoid collisions
with upstream workflow files:

- `omoq-upstream-sync.yml` — Daily upstream sync: mirrors upstream to `upstream`,
  creates merge PR to `main`, auto-merges on CI pass.
- `omoq-auto-merge-sync.yml` — Merges sync PRs after CI passes (triggered by
  `verify` workflow completion on `sync/*` branches).
- `omoq-publish-artifacts.yml` — Builds per-platform artifact bundles on merge
  to `main` using the standalone CMake build. Publishes as rolling
  `snapshot-latest` pre-release.
- `omoq-verify.yml` — Standalone build CI for PRs to `main`.

## Scripts

### `openmoq/scripts/publish-artifacts.sh`

Publishes build artifacts as a rolling `snapshot-latest` GitHub pre-release.

```bash
openmoq/scripts/publish-artifacts.sh \
  --artifacts-dir ./my-artifacts \
  --sha "$(git rev-parse HEAD)" \
  --dry-run
```

### `openmoq/scripts/collect-artifacts-standalone.sh`

Strips binaries, splits debug symbols, and creates release tarballs from a
cmake install prefix.

## Building Moxygen

### Standalone build (recommended)

```bash
# Install system deps
./standalone/install-system-deps.sh

# Build
cmake -B _build -S standalone -G Ninja
cmake --build _build -j$(getconf _NPROCESSORS_ONLN)

# Test
ctest --test-dir _build --output-on-failure

# Install
cmake --install _build --prefix /tmp/moxygen-install
```
