# moxygen Quickstart

This walks through running a MoQT publisher, a relay, and a subscriber locally. For
streaming real audio and video, see [READMOQMEDIA.md](./READMOQMEDIA.md).

## Fastest path: the relay in Docker

If you only want a running relay and do not need to build moxygen, the Docker image
needs no local toolchain and generates its own certificate:

```
docker build -t moqrelay -f docker/Dockerfile .
docker run --rm -p 4433:4433/udp moqrelay
```

That gives you a relay on UDP port 4433 at endpoint `/moq`. To also run the sample
clients below, build moxygen as described in [README.md](./README.md#building).

## Setup

The examples below invoke binaries as `$MOXYGEN_BIN/<name>`. Where they live depends on
which build you used — see [README.md](./README.md#building) for the two options.

For a standalone CMake build, the binaries sit in the build tree mirroring the source
layout, so there is no single directory to point at:

```
_build/moxygen/relay/moqrelayserver
_build/moxygen/samples/date/moqdateserver
_build/moxygen/samples/text-client/moqtextclient
```

Either substitute those paths for `$MOXYGEN_BIN/<name>` below, or install the build to
a prefix and point `MOXYGEN_BIN` at its `bin/`.

For a getdeps build, there is a single install directory:

```
MOXYGEN_BIN=$(./build/fbcode_builder/getdeps.py show-inst-dir moxygen)/bin
```

### A note on `--insecure`

Every server and client below is run with `--insecure`. On a server it means "generate
a throwaway self-signed certificate at startup"; on a client it means "skip certificate
validation", the equivalent of `curl -k`. It keeps local testing to a single flag, and
it must never be used in production.

To use real certificates instead, pass `--cert` and `--key` to the servers and drop
`--insecure` everywhere. If you need a self-signed pair,
`./moxygen/scripts/create-server-certs.sh` writes one to `moxygen/certs/`; it can be
run from anywhere.

## Publisher and subscriber

The date server publishes the current time as one object per second. It listens on
port 9667 at endpoint `/moq-date` by default, serving the track `date` in the
namespace `moq-date`.

Terminal 1 — the publisher:

```
$MOXYGEN_BIN/moqdateserver --insecure --logging DBG1
```

Terminal 2 — the subscriber:

```
$MOXYGEN_BIN/moqtextclient --insecure \
    --connect_url "https://localhost:9667/moq-date" \
    --track_namespace "moq-date" \
    --track_name "date"
```

The client prints each object as it arrives: the current minute, then one line per
second.

## Adding a relay

The relay listens on port 9668 at endpoint `/moq-relay` by default. Here the date
server publishes upstream into the relay instead of serving subscribers directly, and
the text client subscribes through the relay.

Terminal 1 — the relay:

```
$MOXYGEN_BIN/moqrelayserver --insecure --logging DBG1
```

Terminal 2 — the publisher, pointed at the relay:

```
$MOXYGEN_BIN/moqdateserver --insecure \
    --relay_url "https://localhost:9668/moq-relay"
```

Terminal 3 — the subscriber, also pointed at the relay:

```
$MOXYGEN_BIN/moqtextclient --insecure \
    --connect_url "https://localhost:9668/moq-relay" \
    --track_namespace "moq-date" \
    --track_name "date"
```

Start more subscribers against the relay to watch it fan the track out.

## Things to try next

The text client exercises most of the subscriber-side protocol surface:

| Flag | Effect |
|------|--------|
| `--fetch` | Use FETCH for historical objects instead of SUBSCRIBE |
| `--jrfetch` / `--jafetch` | Joining FETCH, relative or absolute, with `--join_start` |
| `--sg` / `--so` / `--eg` | Subscribe from a specific group and object, up to an end group |
| `--publish` | Act as the receiver for a PUBLISH from the peer |
| `--versions` | Restrict the offered draft versions, for example `--versions 18` |
| `--mlog_path` | Write structured protocol logs to a file |

The date server has matching options, including `--mode` to choose between
stream-per-group (`spg`), stream-per-object (`spo`), and `datagram` delivery, and
`--publish` to push the track to a subscriber rather than waiting to be subscribed to.

Pass `--help` to any binary for its full flag list, and see
[ARCHITECTURE.md](./ARCHITECTURE.md) for the API these samples are built on.
