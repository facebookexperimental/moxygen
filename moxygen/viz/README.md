# MoQ Transport Visualization Tool

An interactive timeline for the MLog records emitted by moxygen and MoQ QLOG traces.

## Usage

```bash
cd fbcode/ti/experimental/moxygen/viz
python3 -m http.server 8000
```

Open `http://localhost:8000/html/index.html`, then upload a `.mlog`, `.qlog`,
`.json`, `.jsonl`, or `.jsonseq` file. **Load Example Data** opens a sample session
with audio datagrams, video subgroups, padding, and a descending fetch.

- Client and server columns represent the endpoint that logged each event.
- Track checkboxes show or hide that endpoint's track columns.
- Event filters select control messages, datagrams, subgroups, fetches, or streams
  and padding.
- Ctrl/Cmd + mouse wheel zooms; dragging pans; **Reset View** resets the timeline.
- Hover for timing and identifiers; click for the detail panel and **Raw Event
  Data**, including setup options, parameters, properties, and range-end flags.

## Native MLog format

`FileMLogger` writes separate, pretty-printed JSON records with no enclosing
array or QLOG trace. Upload these files directly:

```json
{
  "vantagePoint": "client",
  "name": "moqt:control_message_created",
  "time": "100000000",
  "data": {
    "stream_id": 0,
    "message": {
      "type": "subscribe",
      "request_id": 0,
      "track_namespace": [{"value": "meeting1"}, {"value": "bob"}],
      "track_name": {"value": "video"},
      "number_of_parameters": 1,
      "parameters": [{"name": "group_order", "value": 1}]
    }
  }
}
```

The parser also accepts an array of records, JSONL, and JSON sequences with record
separators. Strings may contain braces, escaped quotes, and binary byte-string
representations (`value_bytes`).

MLog timestamps are relative `system_clock` ticks. **MLog time units** defaults to
nanoseconds for Linux logs; select microseconds for macOS logs. The timeline
normalizes timestamps to milliseconds. Explicit QLOG time units take precedence
over this setting.

## QLOG format

Existing QLOG traces with array events remain supported, as do object events and
multiple traces:

```json
{
  "qlog_version": "0.3",
  "traces": [{
    "vantage_point": {"type": "client"},
    "configuration": {"time_units": "ms"},
    "events": [
      [100, "transport", "moqt:control_message_created", {
        "message_type": "SUBSCRIBE",
        "subscribe_id": 0,
        "track_name": "video"
      }]
    ]
  }]
}
```

QLOG array timestamps default to milliseconds. `configuration.time_units` or
`common_fields.time_units` can specify `ns`, `us`, `ms`, or `s`. An explicit event
vantage point takes precedence over the trace's vantage point. Created/parsed
names indicate sending/receiving at that endpoint.

## Current MLog event support

| Events | Display |
|--------|---------|
| `control_message_created`, `control_message_parsed` | Nested `message.type`, request IDs, setup options and parameters |
| `object_datagram_created`, `object_datagram_parsed` | Track alias, group/object IDs, status and `object_payload` size |
| `padding_datagram_created`, `padding_datagram_parsed` | Padding length in the endpoint's transport column |
| `subgroup_header_created`, `subgroup_header_parsed` | Track/group context and subgroup ID mode |
| `subgroup_object_created`, `subgroup_object_parsed` | Stream context, decoded `object_id_delta`, properties and payload size |
| `fetch_header_created`, `fetch_header_parsed` | Request-to-track association |
| `fetch_object_created`, `fetch_object_parsed` | Group/object deltas, subgroup bits, datagram and range-end flags |
| `stream_type_set` | Stream type and owner, associated with track headers when available |

All event names have the `moqt:` prefix. Legacy datagram-status events and flat
control/object fields remain supported.

### Track and object reconstruction

Tracks use namespace and track names from control messages, including text and
binary byte strings. Track aliases are scoped to the publishing endpoint. Stream
state is isolated by trace, endpoint, and transfer direction, so sending and
receiving logs do not advance each other's object counters.

Subgroup objects inherit track, group, and subgroup information from their stream
header. Fetch objects inherit the request's track and group ordering; range-end
records carry absolute endpoints in the fields named `group_id_delta` and
`object_id_delta`.

When logs omit naming or ordering information, the tool retains
`alias:client:N` / `alias:server:N`, `request:N`, or `stream:N` labels and shows
unresolved deltas in the event data.

## Examples and validation

- `examples/sample_moq.mlog`: current native MLog records from a client viewpoint.
- `examples/sample_moq.qlog` and `examples/sample_serialized.qlog`: legacy QLOG samples.

From the `viz` directory:

```bash
node test_moq_parser.js
node generate_sample.js
```

The tests cover native file parsing, timestamp units, control messages, zero IDs,
track/stream association, subgroup and fetch deltas, range ends, legacy QLOG, and
timeline labels/details. The generator recreates `sample_moq.mlog` deterministically.

## References

- [MoQ Transport specification](https://moq-wg.github.io/moq-transport/draft-ietf-moq-transport.html)
- [MLog serializers](../mlog/MLogTypes.cpp)
- [MLog event creation](../mlog/MLogEvents.cpp)
