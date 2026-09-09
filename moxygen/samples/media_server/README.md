# MoQMediaServer

A Media-over-QUIC (MoQ) origin that serves fragmented-MP4 media as MoQ tracks. It
boots content-agnostic: a client discovers a broadcast's tracks via a `catalog`
track, then subscribes to the media tracks. Namespaces are routed to a backend by
their first tuple field (`file` -> reliable local disk, `file_pr` -> local disk
with simulated segment loss, `file_abr` -> progressive catalog updates).

## Build

```
cmake --build build --target moq_media_server
```

## Start the server

```
./build/moq_media_server --input /path/to/catalog.json --port 60100 --fragment_interval_ms 1000 --loop --insecure
```

Flags:

- `--input` (required): catalog JSON for the file-backed modes.
- `--port` (default `9779`): QUIC/WebTransport listen port.
- `--fragment_interval_ms` (default `1000`): media-time window width used to
  pace source fragments on the shared playback clock. Each source fragment is
  emitted as one MoQ group containing single-sample CMAF objects.
- `--catalog_update_interval` (default `10`): seconds between complete catalog
  snapshots in `file_abr` mode.
- `--loop` (default off): loop the source forever (live); omit for a finite
  one-shot that ends after one pass.
- `--file_pr_control_port` (default `60101`): HTTP port for the experimental
  `file_pr` fault-control UI; `0` disables it.

The catalog JSON lists tracks, each with a `sourceFile` (an fMP4 resolved next to
the catalog).

## Test it (no device needed)

Start the server, then run the reference subscriber, which discovers the catalog
and writes each track to disk:

```
./build/moq_mp4_receiver --connect_url moqt://localhost:60100 --track_namespace file/moq-media --track_namespace_delimiter / --output /tmp/moq_out --duration_s 0
```

It writes `/tmp/moq_out.<track>.mp4` per track (e.g. `video0`, `audio0`).
Use `file_pr/moq-media` as the namespace to enable the fault-control UI.
Use `file_abr/moq-media` to start with the first authored video track plus all
non-video tracks, then advertise one additional video track per catalog group.

For on-demand `file_pr` faults, open `http://127.0.0.1:60101`. The `Affect
I-frames` checkbox applies to both actions. When it is clear, Drop removes only
video P-frame objects and Hold sends video I-frame objects on time while
delaying the remaining objects on that subgroup stream. When it is selected,
Drop removes and Hold delays the entire video subgroup. Delayed objects retain
their original IDs and media timestamps. These actions affect every subscriber
to that `file_pr` track and never affect the reliable `file` namespace.

## Layout

- `MoQMediaServer` — MoQ transport (WebTransport + raw QUIC); hands each session to the dispatcher.
- `MoQBroadcastDispatcher` — namespace registry; routes SUBSCRIBE/FETCH to a broadcast.
- `MoQBroadcastFactory` — builds a broadcast per namespace; owns backend/resolver selection.
- `MoQBroadcast` — per-namespace serving unit; per-track stacks (source + forwarder + publish loop).
- `MediaSourceResolver` / `sources/` — resolve `(namespace, track)` to a `SegmentSource` (fMP4 media or the catalog).
- `CmafFrameChunker` — repackages each source fragment into one valid CMAF
  `moof+mdat` object per encoded video or audio frame.
- `FilePrControlServer` / `FilePrFaultState` — HTTP UI and process-local
  controls used only by the disposable `file_pr` fixture.
- `PublishLoop` — drains a `SegmentSource` into a `MoQForwarder`.
- `MediaCatalog` — catalog metadata parse/serialize.
- `MoQMp4Receiver` — reference test subscriber.
