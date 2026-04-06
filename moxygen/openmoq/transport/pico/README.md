# PicoQuic Transport for MOQT

This directory contains the picoquic-based QUIC transport backend for MOQT,
supporting both QUIC transport (non-browser clients) and HTTP/3 WebTransport (browsers).

---

## Architecture Overview

```
┌─────────────────────────────┐               ┌─────────────────────────────┐
│  MoQPicoQuicServer          │               │ MoQPicoQuicEventBaseServer  │
│  (Threaded Model)           │               │ (EventBase Model)           │
│                             │               │                             │
│ • picoquic_start_network_   │               │ • PicoQuicSocketHandler     │
│   thread()                  │               │ • folly::EventBase          │
│ • PicoQuicExecutor          │               │ • MoQFollyExecutorImpl      │
│ • Single dedicated thread   │               │ • Shared event loop         │
└─────────────────────────────┘               └─────────────────────────────┘
              │                                               │
              └───────────────────────┬───────────────────────┘
                                      │  owns
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                               picoquic                                      │
│                    (QUIC protocol, congestion control)                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
                            ┌─────────────────┐
                            │   UDP Socket    │
                            └─────────────────┘

On each connection, picoquic drives one of two WebTransport adapters:

┌─────────────────────────────┐               ┌─────────────────────────────┐
│   PicoQuicWebTransport      │               │    PicoH3WebTransport       │
│   (QUIC - moqt-NN)          │               │    (HTTP/3 WebTransport)    │
│                             │               │                             │
│ • picoquic_callback_*       │               │ • picohttp_callback_*       │
│ • Direct stream IDs         │               │ • h3zero stream contexts    │
│ • Native QUIC clients       │               │ • Browser clients           │
└─────────────────────────────┘               └─────────────────────────────┘
              │                                               │
              └───────────────────────┬───────────────────────┘
                                      │  inherits
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          PicoWebTransportBase                               │
│           (shared: WtStreamManager, JIT send, egress events)                │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │  implements
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        proxygen::WebTransport Interface                     │
│              (streams, datagrams, flow control, session mgmt)               │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                             MOQT Application                                │
│                         (MoQSession, MoQRelay, etc.)                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Class Hierarchy

### Server Classes

```
MoQPicoServerBase                    <- Shared: QUIC context, ALPN, h3zero init
    │
    ├── MoQPicoQuicServer            <- Threaded: picoquic_start_network_thread
    │       └── PicoQuicExecutor     <- Executor for coroutines on packet thread
    │
    └── MoQPicoQuicEventBaseServer   <- EventBase: PicoQuicSocketHandler
            └── PicoQuicSocketHandler <- UDP I/O, wake timer on EventBase
            └── MoQFollyExecutorImpl  <- Executor backed by EventBase
```

### WebTransport Adapters

```
proxygen::WebTransport (interface)
        │
PicoWebTransportBase                 <- Shared base: WtStreamManager, JIT, egress
   ├── PicoQuicWebTransport          <- QUIC transport (moqt-NN ALPN)
   └── PicoH3WebTransport            <- HTTP/3 WebTransport (h3 ALPN)
```

---

## PicoWebTransportBase

The base class implements the full `proxygen::WebTransport` interface and
provides shared functionality for both transport variants:

### Shared Implementation

| Component | Description |
|-----------|-------------|
| `WtStreamManager` | Per-stream buffering, read/write handles, priority queue |
| `processEgressEvents()` | Drains WtStreamManager events (reset, stop-sending, close) |
| `onJitProvideData()` | JIT send path - dequeues data and provides to picoquic |
| `onStreamDataCommon()` | Ingress data delivery with deferred stream notification |
| `WakeTimeGuard` | RAII helper to reschedule wake timer on state changes |

### Pure Virtual Primitives (Subclass Implements)

| Method | PicoQuicWebTransport | PicoH3WebTransport |
|--------|---------------------|-------------------|
| `createStreamImpl()` | `picoquic_get_next_local_stream_id` | `picowt_create_local_stream` |
| `markStreamActiveImpl()` | `picoquic_mark_active_stream(id, nullptr)` | `picoquic_mark_active_stream(id, streamCtx)` |
| `markDatagramActiveImpl()` | `picoquic_mark_datagram_ready` | `h3zero_set_datagram_ready` |
| `resetStreamImpl()` | `picoquic_reset_stream` | `picowt_reset_stream` |
| `stopSendingImpl()` | `picoquic_stop_sending` | `picoquic_stop_sending` |
| `sendCloseImpl()` | `picoquic_close` | `picowt_send_close_session_message` |

---

## Protocol Selection (ALPN)

| ALPN | Protocol | WebTransport Adapter |
|------|----------|---------------------|
| `moqt-16`, `moqt-15`, etc. | QUIC transport | `PicoQuicWebTransport` |
| `h3` | HTTP/3 WebTransport | `PicoH3WebTransport` |

ALPN preference order: MOQT ALPNs first (preferred for non-browser clients),
`h3` last (fallback for browsers).

---

## Thread Models

### Threaded Model (MoQPicoQuicServer)

A single network thread spawned by `picoquic_start_network_thread`. All picoquic
I/O, callbacks, and coroutines run on this thread. `PicoQuicExecutor` integrates
coroutine execution into the packet loop.

```
┌────────────────────────────────────────────────────────────────┐
│                    Packet Loop Thread                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  picoquic_packet_loop()                                  │  │
│  │    • select/epoll on UDP socket                          │  │
│  │    • picoquic_incoming_packet() for received data        │  │
│  │    • picoquic_prepare_next_packet_ex() for outgoing      │  │
│  │    • Invokes picoCallback / h3zero_callback              │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              │                                 │
│                              ▼                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  PicoQuicExecutor (loopCallbackStatic)                   │  │
│  │    • Drains pending tasks (folly::Func)                  │  │
│  │    • Processes expired timers                            │  │
│  │    • Runs MoQSession coroutines                          │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

### EventBase Model (MoQPicoQuicEventBaseServer)

Caller supplies a `folly::EventBase`. `PicoQuicSocketHandler` drives picoquic I/O
via `AsyncUDPSocket` (notify-only mode with `recvmmsg` batching) and `AsyncTimeout`
for wake timer scheduling.

```
┌────────────────────────────────────────────────────────────────┐
│                    folly::EventBase Thread                     │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  EventBase::loopForever()                                │  │
│  └──────────────────────────────────────────────────────────┘  │
│              │                               │                 │
│  ┌───────────▼─────────────┐   ┌─────────────▼───────────────┐ │
│  │ PicoQuicSocketHandler   │   │ MoQFollyExecutorImpl        │ │
│  │  • onNotifyDataAvailable│   │  • Runs MoQSession coros    │ │
│  │  • recvmmsg batching    │   │  • Handles timers           │ │
│  │  • sendmsg with GSO     │   │                             │ │
│  └─────────────────────────┘   └─────────────────────────────┘ │
└────────────────────────────────────────────────────────────────┘
```

---

## JIT (Just-In-Time) Send Model

Both transport variants use picoquic's JIT send model - data is not pushed
proactively but provided on demand.

### Stream Writes

```
1. Application writes:
   MoQSession -> writeStreamData(id, IOBuf, fin)
     -> WtStreamManager buffers data
     -> markStreamActiveImpl(id)  // signals picoquic

2. Picoquic calls back when ready:
   picoquic_callback_prepare_to_send (QUIC) / picohttp_callback_provide_data (H3)
     -> PicoWebTransportBase::onJitProvideData(streamId, context, maxLength)
          -> streamManager_->dequeue(*handle, maxLength)
          -> picoquic_provide_stream_data_buffer(context, dataLen, fin, isActive)
          -> memcpy data into returned buffer
          -> Fire delivery callback (optimistic)
```

### Datagram Writes

```
1. Application queues:
   sendDatagram(IOBuf) -> datagramQueue_.push_back() -> markDatagramActiveImpl()

2. Picoquic calls back:
   picoquic_callback_prepare_datagram / picohttp_callback_provide_datagram
     -> Dequeue and copy to picoquic buffer
```

---

## Wake Timeout Optimization

When `markStreamActive` or `markDatagramActive` is called, picoquic's next wake
time may decrease. The `WakeTimeGuard` RAII helper captures the wake time before
and after, invoking `updateWakeTimeoutCallback` if it decreased. This ensures
the EventBase timer is rescheduled promptly, avoiding latency spikes.

---

## HTTP/3 WebTransport Specifics (PicoH3WebTransport)

### h3zero Callback Flow

```
Browser HTTP/3 CONNECT
  -> h3zero processes HTTP/3 frames
  -> wtPathCallback(picohttp_callback_connect, ...)
  -> MoQPicoServerBase creates PicoH3WebTransport
  -> 200 OK sent to browser

Subsequent events:
  -> h3zero decodes HTTP/3 frames
  -> wtPathCallback(picohttp_callback_*, ...)
  -> PicoH3WebTransport::handleWtEvent(...)
```

### Control Stream

HTTP/3 WebTransport uses a dedicated control stream for session management
capsules (`CLOSE_WEBTRANSPORT_SESSION`, `DRAIN_WEBTRANSPORT_SESSION`).

### Session Context

`PicoH3SessionContext` (in `PicoConnectionContext.h`) is stored in
`streamCtx->path_callback_ctx` and holds the `PicoH3WebTransport` and
`MoQSession`. `dispatchH3Event` routes all `picohttp_callback_*` events by
casting and checking the magic field. When `handleWtEvent` returns `kDeleteCtx`
(all streams freed after deregister), `dispatchH3Event` deletes the session
context.

### Stream Context Tracking

Unlike QUIC transport, H3 requires tracking `h3zero_stream_ctx_t*` per stream for
JIT callbacks. Stored in `streamContexts_` map. New streams must inherit
`path_callback` from the control stream context.

### Stream Lifecycle

`maybeDeleteStream` calls `h3zero_delete_stream` when both `is_fin_received`
and `is_fin_sent` are set on a stream context, prompting h3zero to free the
stream promptly rather than waiting until connection close. Triggered from
`post_fin`, `reset` (incoming), egress FIN (`provide_data`), and
`resetStreamImpl`.

---

## Connection Lifecycle

### QUIC transport (moqt-NN ALPN)

1. `picoquic_callback_almost_ready` — ALPN confirmed as QUIC transport; no callback switch needed
2. `picoquic_callback_ready` → `onNewConnectionImpl` creates `PicoQuicWebTransport` + `MoQSession`
3. Session coroutine starts via executor

### HTTP/3 WebTransport (h3 ALPN)

1. `picoquic_callback_almost_ready` — ALPN confirmed as `h3`; picoquic callback switched to `h3zero_callback`, `almost_ready` forwarded to h3zero
2. `picoquic_callback_ready` forwarded to h3zero; `onNewConnectionImpl` is a no-op for h3 connections
3. Browser sends HTTP/3 CONNECT → `wtPathCallback(picohttp_callback_connect)` → `onWebTransportConnectImpl` creates `PicoH3WebTransport` + `MoQSession`
4. Session coroutine starts via executor

### Close

| Trigger | Path |
|---------|------|
| Peer closes | Callback → `onSessionCloseCommon` → handler notification |
| Local close | `closeSession()` → `sendCloseImpl()` → handler notification → drain |

---

## Samples

| Binary | Class | Description |
|--------|-------|-------------|
| `pico_relay_server` | `MoQPicoQuicServer` | Thread-based MOQT relay |
| `pico_evb_relay_server` | `MoQPicoQuicEventBaseServer` | EventBase MOQT relay |

### Running

```bash
# Thread-based relay
./bin/pico_relay_server --port 4433 --cert cert.pem --key key.pem

# EventBase relay  
./bin/pico_evb_relay_server --port 4433 --cert cert.pem --key key.pem

# mvfst relay (for comparison)
./bin/moqrelayserver --port 4433 --cert cert.pem --key key.pem
```

---

## Files

| File | Description |
|------|-------------|
| `PicoWebTransportBase.h/cpp` | Shared WebTransport base class |
| `PicoQuicWebTransport.h/cpp` | QUIC transport WebTransport adapter |
| `PicoH3WebTransport.h/cpp` | HTTP/3 WebTransport adapter |
| `PicoConnectionContext.h` | Per-connection/session context structs and dispatch helpers |
| `PicoProtocolDispatcher.h` | ALPN → PicoProtocolType mapping |
| `MoQPicoServerBase.h/cpp` | Shared server base (ALPN, h3zero init) |
| `MoQPicoQuicServer.h/cpp` | Threaded server |
| `MoQPicoQuicEventBaseServer.h/cpp` | EventBase server |
| `PicoQuicSocketHandler.h/cpp` | EventBase UDP I/O engine |
| `PicoQuicExecutor.h/cpp` | Thread-based executor |
