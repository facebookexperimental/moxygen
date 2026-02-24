# Moxygen Standalone Build

This directory provides a standalone build configuration that automatically
fetches Meta's OSS dependencies (folly, fizz, wangle, mvfst, proxygen) using
CMake's FetchContent.

**Note:** This build method is for the open-source GitHub version only. It reads
pinned commit hashes from `build/deps/github_hashes/` which are included in the
GitHub repository but not in fbsource.

## Prerequisites

System dependencies must be installed before building. Run:

```bash
./standalone/install-system-deps.sh
```

Or install manually:
- OpenSSL
- glog
- gflags
- double-conversion
- libevent
- libsodium
- zstd
- boost (context, filesystem, program_options)
- fmt
- c-ares
- gperf
- googletest (for tests)

## Building

```bash
# Clone the repo (github_hashes are included)
git clone https://github.com/facebookexperimental/moxygen.git
cd moxygen

# Install system deps
./standalone/install-system-deps.sh

# Build using standalone entry point
cmake -B _build -S standalone -G Ninja
cmake --build _build -j$(nproc)

# Run tests
ctest --test-dir _build
```

## How Version Pinning Works

The standalone build reads commit hashes from `build/deps/github_hashes/`:
```text
build/deps/github_hashes/
├── facebook/
│   ├── folly-rev.txt
│   ├── mvfst-rev.txt
│   ├── proxygen-rev.txt
│   └── wangle-rev.txt
└── facebookincubator/
    └── fizz-rev.txt
```

These are the same hashes used by getdeps.py, ensuring both build methods
use identical dependency versions.

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `BUILD_TESTS` | `ON` | Build test executables |
| `BUILD_SHARED_LIBS` | `OFF` | Build shared libraries (discouraged) |

## Build Caching

To avoid re-downloading dependencies on clean builds:

```bash
cmake -B _build -S standalone \
    -DFETCHCONTENT_BASE_DIR=$HOME/.cmake-fetchcontent
```

## Testing Local Changes to Dependencies

To test local modifications to any dependency (e.g., proxygen, mvfst), use
CMake's `FETCHCONTENT_SOURCE_DIR_<NAME>` variables. This skips fetching and
uses your local checkout instead:

```bash
# Test local proxygen changes
cmake -B _build -S standalone \
    -DFETCHCONTENT_SOURCE_DIR_PROXYGEN=/path/to/my/proxygen

# Test local mvfst changes
cmake -B _build -S standalone \
    -DFETCHCONTENT_SOURCE_DIR_MVFST=/path/to/my/mvfst

# Test multiple local deps at once
cmake -B _build -S standalone \
    -DFETCHCONTENT_SOURCE_DIR_PROXYGEN=/path/to/proxygen \
    -DFETCHCONTENT_SOURCE_DIR_MVFST=/path/to/mvfst \
    -DFETCHCONTENT_SOURCE_DIR_FOLLY=/path/to/folly
```

Available override variables:
- `FETCHCONTENT_SOURCE_DIR_FOLLY`
- `FETCHCONTENT_SOURCE_DIR_FIZZ`
- `FETCHCONTENT_SOURCE_DIR_WANGLE`
- `FETCHCONTENT_SOURCE_DIR_MVFST`
- `FETCHCONTENT_SOURCE_DIR_PROXYGEN`

**Note:** When using local sources, you're responsible for ensuring version
compatibility between dependencies.

## Troubleshooting

### "Rev file not found"
Make sure you cloned from GitHub (not fbsource). The github_hashes
directory is only present in the open-source repository.

### "Could not find package X"
Ensure system dependencies are installed. Run `./standalone/install-system-deps.sh`.

### First build is slow
FetchContent downloads ~500MB of source code on first run. Subsequent
builds use the cache.
