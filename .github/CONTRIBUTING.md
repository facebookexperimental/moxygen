# Contributing to openmoq/moxygen

This document describes guidelines for contributing **directly to the
`openmoq/moxygen` fork repo** — a community-maintained fork of
[facebookexperimental/moxygen](https://github.com/facebookexperimental/moxygen)
for use within OpenMOQ projects. It maintains local customizations and may
carry changes not yet merged upstream.

> **Note:** Whenever possible, if a contribution applies to core `moxygen` functionality, the PR submission should be made to the upstream parent repo. Contributions to upstream Meta moxygen repo are governed by [the upstream `CONTRIBUTING.md`](https://github.com/facebookexperimental/moxygen/blob/main/CONTRIBUTING.md) and that document does not apply to contributions made directly to this repo.

Openness and community participation are foundational principles of the OpenMOQ Consortium. All are welcome and encouraged to use the software here freely, and contribute to testing, fixing, and enhancing the code when possible.

The following describes the general philosophy and approach for handling contributions. The process remains flexible and subject to review, and may change in the future. The ultimate goal is to facilitate the production of high-quality, high-performance, professional-grade software.

## Pull request scope and content

**Each PR should address a single and logically cohesive thesis.**

This makes reviews more approachable and more likely to occur. Avoid bundling various unrelated changes and ad hoc changes.

The following is a list of ideals for PR content — developer
discretion and sound judgement is expected:

- The title should clearly reflect the functional impact of the PR (in most cases this becomes the commit message on the merge commit).
- The description should contain additional technical details and solution rationale.
- If the PR addresses an existing issue, reference it in the description — `Fixes #N` to auto-close on merge, or `Refs #N` for partial or related work.
- Tests or other evaluation criteria should be included for independent pass/fail evaluation (including unit tests where possible).
- Relevant logs, developer test output, or other supporting material may be attached to support the review process.

## PR state

PRs with all checks passing may be merged by maintainers at
unpredictable times based on availability and relative priority. The
author can signal merge intent in the following ways:

- **Draft** — not ready for review. No auto-reviewer request; CI still runs.
- **`WIP:` prefix** — ready for review and CI, not ready for merge.
- **Ready** (non-draft, no `WIP:` prefix) — merge when all checks pass.

## How to contribute

- Outside contributors: fork, branch, PR against `main`.
- Org members: branch directly on this repo, PR against `main`.

PRs run CI with no secrets. Publish, release, and deploy run only on
`push: main` after merge.

First-time fork PRs show "Waiting for approval" on Actions — a
maintainer unblocks them; subsequent runs are automatic.

## Reviews

At least one approving review from a collaborator is required.
[CODEOWNERS](CODEOWNERS) auto-requests reviewers. Review on GitHub or
[Reviewable](https://reviewable.io/reviews/openmoq/moxygen).

**Admin override** (`gh pr merge --admin`) is for:

- CI/infrastructure repairs blocked by branch protection itself.
- Release-critical merges under urgency.
- Docs-only or other low/no-risk changes.

Note the override in the PR description: `Admin override: <reason>`.

## CI

Every PR must pass:

- `check-format` — clang-format + license headers.
- `linux` — build + test on Ubuntu 22.04.
- `macos` — build + test on macOS.
- `asan debug` — build + test with AddressSanitizer.

See [.github/workflows/omoq-ci-pr.yml](.github/workflows/omoq-ci-pr.yml).
CI changes go in the same PR as the code that needs them.

## Branches

- `main` — working branch; all openmoq customizations live here. Releases are tagged from `main`.
- `upstream` — mirror of `facebookexperimental/moxygen:main`, advanced by the daily sync workflow. Do not push to it.
- `sync/<sha>` — sync PR branches from the sync bot. Push conflict fixes as needed.
- `devops/*`, `feature/*`, `fix/*`, `hotfix/*` — convention only, no enforcement.

The specific suffix branch name is up to the developer — something
informative is often helpful (please clean up stale branches).

## Upstream sync

[omoq-upstream-sync.yml](.github/workflows/omoq-upstream-sync.yml)
mirrors `facebookexperimental/moxygen:main` to `upstream` daily and
opens a `sync/<sha>` PR against `main`. Auto-merges on green CI;
conflicts are resolved on the `sync/<sha>` branch.

Files that conflict repeatedly — `cmake/moxygen-config.cmake.in`,
`moxygen/CMakeLists.txt` — prefer upstreaming a fix to Meta over a
local patch.

## Releases

`main` produces rolling `snapshot-latest` artifacts on every push.
Versioned releases are cut manually from `main` via
[omoq-version-release.yml](.github/workflows/omoq-version-release.yml):
Actions → *version release* → *Run workflow* → enter version. The
workflow tags `main` and promotes the current snapshot-latest
artifacts to a non-prerelease.

## Merge

By default, PRs are squash-merged on `main` — the PR title becomes the single commit message, so make the title descriptive and self-contained.

`Rebase merge` (commits land verbatim) or `Merge` (creates a merge commit) is available at the author's request, or when preserving history on `main` is warranted. For these modes, authors are expected to groom commits beforehand — squash fixups, rewrite vague messages, drop noise — since each commit lands on `main` as written.

Even for squash-merged PRs, a clean branch history aids review: small, logically-scoped commits make diffs easier to follow and bisect.

Merges are conducted by maintainers with merge privileges, in
accordance with the philosophy described in this document. The
`omoq-sync-bot` App merges sync PRs automatically.

> **Note:** *Delete branch on merge* is the current default setting.

## Security & License

Report security issues via [SECURITY.md](SECURITY.md) — not public
issues. Contributions are licensed under [LICENSE](LICENSE).
