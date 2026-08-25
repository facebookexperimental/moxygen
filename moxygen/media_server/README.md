# MoQMediaServer

A Media-over-QUIC (MoQ) origin that serves fragmented-MP4 media as MoQ tracks. It
boots content-agnostic: a client discovers a broadcast's tracks via a `catalog`
track, then subscribes to the media tracks. Namespaces are routed to a backend by
their first tuple field (`file` -> local disk today; `oil` -> FBMS later).

## Build

```
buck2 build fbcode//ti/experimental/moxygen/media_server:moq_media_server
```

## Start the server

```
buck2 run fbcode//ti/experimental/moxygen/media_server:moq_media_server -- --input /path/to/catalog.json --port 60100 --fragment_interval_ms 1000 --insecure
```

Flags:

- `--input` (required): catalog JSON for the `file` backend. Any namespace whose
  first tuple field is `file` (e.g. `file-<id>--video0`) is served from it.
- `--port` (default `9779`): QUIC/WebTransport listen port.
- `--fragment_interval_ms` (default `1000`): media-time window width used to
  pace fragments on the shared playback clock.
- `--loop` (default off): loop the source forever (live); omit for a finite
  one-shot that ends after one pass.

The catalog JSON lists tracks, each with a `sourceFile` (an fMP4 resolved next to
the catalog). A ready-made sample lives at
`/data/users/<unixname>/moq_media/bbb.catalog.json` (Big Buck Bunny) or
`test30s.catalog.json`.

## Test it (no device needed)

Start the server, then run the reference subscriber, which discovers the catalog
and writes each track to disk:

```
buck2 run fbcode//ti/experimental/moxygen/media_server:moq_mp4_receiver -- --connect_url moqt://localhost:60100 --track_namespace file/moq-media --track_namespace_delimiter / --output /tmp/moq_out --duration_s 0
```

It writes `/tmp/moq_out.<track>.mp4` per track (e.g. `video0`, `audio0`).

## Layout

- `MoQMediaServer` — MoQ transport (WebTransport + raw QUIC); hands each session to the dispatcher.
- `MoQBroadcastDispatcher` — namespace registry; routes SUBSCRIBE/FETCH to a broadcast.
- `MoQBroadcastFactory` — builds a broadcast per namespace; owns backend/resolver selection.
- `MoQBroadcast` — per-namespace serving unit; per-track stacks (source + forwarder + publish loop).
- `MediaSourceResolver` / `sources/` — resolve `(namespace, track)` to a `SegmentSource` (fMP4 media or the catalog).
- `PublishLoop` — drains a `SegmentSource` into a `MoQForwarder`.
- `MediaCatalog` — MSF/CMSF catalog metadata parse/serialize.
- `MoQMp4Receiver` — reference test subscriber.
