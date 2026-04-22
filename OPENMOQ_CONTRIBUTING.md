# Contributing to openmoq/moxygen

Fork of
[facebookexperimental/moxygen](https://github.com/facebookexperimental/moxygen),
maintained with minimal divergence. Contributions welcome.

## Guiding principle

> Producing code in the era of AI is cheap. Reviewer attention is the
> scarce resource.

Because this is a fork: **prefer upstreaming over carrying local
changes.** Upstreamed patches have zero sync cost; fork-local ones
re-apply to every sync.

## Pull request scope

**One PR = one cohesive thesis.** A reviewer should read the title and
predict the diff.

- ✅ `fix: MoQForwarder::Subscriber::onPublishOk updates forwardingSubscribers_`
- ❌ `various fixes and cleanups`
- ❌ `feature X + refactor Y` (split it)

## PR state

Useful PRs with all checks green are merged when a maintainer is
available. Signal intent:

- **Draft** — not ready for review. No auto-reviewer request; CI still runs.
- **Ready** (non-draft, no `WIP:` prefix) — merge when green.
- **`WIP:` prefix** — ready for review and CI, not for merge.

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
- Docs-only changes when waiting costs more than reviewing.

Note the override in the PR description: `Admin override: <reason>`.

## CI

Every PR must pass:

- `check-format` — clang-format + license headers
- `linux` — build + test on Ubuntu 22.04
- `macos` — build + test on macOS
- `asan debug` — build + test with AddressSanitizer

See [.github/workflows/omoq-ci-pr.yml](.github/workflows/omoq-ci-pr.yml).
CI changes go in the same PR as the code that needs them.

## Branches

- `main` — working branch; all openmoq customizations live here.
  Releases are tagged from `main`.
- `upstream` — mirror of `facebookexperimental/moxygen:main`, advanced
  by the daily sync workflow. Do not push to it.
- `sync/<sha>` — sync PR branches from the sync bot. Push conflict
  fixes as needed.
- `devops/*`, `feature/*`, `fix/*`, `hotfix/*` — working branches.
  Convention only.

## Upstream sync

[omoq-upstream-sync.yml](.github/workflows/omoq-upstream-sync.yml)
mirrors `facebookexperimental/moxygen:main` to `upstream` daily and
opens a `sync/<sha>` PR against `main`. Auto-merges on green CI;
conflicts are resolved on the `sync/<sha>` branch.

Files that conflict repeatedly — `cmake/moxygen-config.cmake.in`,
`moxygen/CMakeLists.txt` — prefer upstreaming a fix to Meta over
a local patch.

## Releases

`main` produces rolling `snapshot-latest` artifacts on every push.
Versioned releases are cut manually from `main` via
[omoq-version-release.yml](.github/workflows/omoq-version-release.yml):
Actions → *version release* → *Run workflow* → enter version. The
workflow tags `main` and promotes the current snapshot-latest
artifacts to a non-prerelease.

## Merge

PRs are squash-merged; the PR title becomes the commit message on `main`.
Authors are encouraged to maintain a concise, informative commit
history on the branch — it aids review. Request a merge commit in the
PR description if preserving history on `main` is warranted.

Maintainers with merge rights: @afrind, @gmarzot, @suhasHere. The
`omoq-sync-bot` App merges sync PRs automatically.

## Security & License

Report security issues via [SECURITY.md](SECURITY.md) — not public
issues. Contributions are licensed under [LICENSE](LICENSE).
