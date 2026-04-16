# Contributing to openmoq/moxygen

Contributions to this fork of moxygen are welcome from anyone.

## Quick start

- **Not a collaborator?** Fork this repo, push to your fork, open a PR against
  `openmoq/moxygen:main`.
- **Are a collaborator?** Push a feature branch directly to this repo, open
  a PR against `main`.

Every PR runs the same CI gate regardless of origin. Merge is performed by
a maintainer once checks are green and at least one review is approving.

## For outside contributors (fork-based PRs)

1. **Fork** this repository on GitHub.
2. **Clone** your fork and create a feature branch:
   ```
   git clone git@github.com:<you>/moxygen.git
   cd moxygen
   git checkout -b my-change
   ```
3. **Make your change**, keeping commits small and focused.
4. **Push** to your fork and **open a PR** targeting `openmoq/moxygen:main`.
5. The CI pipeline runs on your PR without any secrets available — it can
   read public artifacts and the public repo, nothing more. If your change
   passes CI and review, a maintainer will merge it.

First-time contributors may see a "Waiting for approval" status on their
first PR's Actions run — this is a security gate on public repos. A
maintainer will click "Approve and run" once they've glanced at the PR.
Subsequent PRs from the same contributor run automatically.

## For openmoq org members (direct-branch PRs)

Org members with write access can push branches directly into this repo
instead of forking:

1. Clone this repo, create a branch, push it.
2. Open a PR against `main`.
3. CI runs with the same permissions as a fork PR — no org secrets are
   exposed to the PR's CI context. Publish/release/deploy steps only run
   on `push: main` after merge.

Use any descriptive branch name. The repo is configured to automatically
delete merged branches, so there is no need to clean up.

## What CI checks

All of the following must pass before a PR can be merged:

- `check-format` — clang-format + header/license checks (`scripts/format.sh --check`)
- `linux` — build + run tests on Ubuntu 22.04
- `macos` — build + run tests on macOS
- `asan debug` — build with AddressSanitizer + run tests

See [.github/workflows/omoq-ci-pr.yml](.github/workflows/omoq-ci-pr.yml)
for the full pipeline. Each job also publishes a test report that appears
as an annotated check on the PR.

If your change legitimately needs a CI update (new dep, new platform,
adjusted flags), include the workflow edit in the same PR.

## Reviews

At least **one approving review** is required before merge. Reviews can
come from any collaborator with write access — they do not have to come
from a designated code owner.

[CODEOWNERS](CODEOWNERS) lists the active reviewer pool; those users are
auto-requested for review when a PR opens. Anyone else in the collaborator
list can also review on their own initiative.

We use [Reviewable](https://reviewable.io/reviews/openmoq/moxygen) for
multi-round review discussion alongside GitHub's native review interface.
Either is acceptable.

## Merge process

Merging is restricted to a small rotation of maintainers via GitHub branch
protection. The current maintainers holding the merge button are:

- @afrind
- @gmarzot
- @suhasHere

In addition, the `omoq-sync-bot` GitHub App performs automated merges of
upstream-sync PRs after they pass CI.

**Merge style**: We use **squash merge**. The squashed commit uses the PR
title verbatim as its message (body is empty). Contributors do **not**
need to pre-squash commits in their branch — the squash happens server-side
when the maintainer clicks "Squash and merge".

If you want your commit history on main to read well, make sure your PR
title accurately summarizes the change. The PR body stays on the PR page
for future reference, but is not reflected in the commit message.

## Security

Please report security issues via the process described in
[SECURITY.md](SECURITY.md). Do not file public issues for security
reports.

## License

By contributing to this project, you agree that your contributions will be
licensed under the [LICENSE](LICENSE) file in the root directory of this
source tree.
