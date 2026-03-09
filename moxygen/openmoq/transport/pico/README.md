# PicoQuic Transport Data Flow

This document describes the data flow introduced by the picoquic transport backend
for MoQ. It covers the class hierarchy, thread models, connection establishment,
stream and datagram reads and writes, and connection teardown.

## Architecture Overview

The picoquic stack adds a self-contained QUIC transport path using picoquic
instead of mvfst/proxygen-QUIC. Two I/O models are supported: a dedicated
background thread and a caller-supplied `folly::EventBase`.

```
MoQPicoServerBase              <- shared: picoquic context, ALPN, connection bootstrap
    ├── MoQPicoQuicServer      <- thread-based: picoquic_start_network_thread + PicoQuicExecutor
    └── MoQPicoQuicEventBaseServer  <- EventBase-based: PicoQuicSocketHandler + MoQFollyExecutorImpl

MoQPicoQuicEventBaseClient     <- outgoing connections on a caller EventBase

PicoQuicWebTransport           <- proxygen::WebTransport adaptor for picoquic (shared by all paths)
PicoQuicSocketHandler          <- EventBase-driven UDP I/O (shared by evb server and evb client)
PicoQuicExecutor               <- folly::Executor / timer integration into the packet loop thread
```

`MoQSession` and everything above it remain unchanged. This stack only
introduces a new transport backend.

---

## Thread Models

### Thread-based (`MoQPicoQuicServer`)

A **single network thread** is spawned by `picoquic_start_network_thread`.
All picoquic I/O, connection callbacks, stream callbacks, and executor tasks run
on this one thread. `PicoQuicExecutor` is the mechanism that lets
`folly::coro` coroutines and timer callbacks execute on that same thread.

### EventBase-based (`MoQPicoQuicEventBaseServer` / `MoQPicoQuicEventBaseClient`)

The caller supplies a `folly::EventBase`. `PicoQuicSocketHandler` drives picoquic
I/O directly on that EventBase: an `AsyncUDPSocket` in notify-only mode delivers
incoming datagrams via `recvmmsg`, and an `AsyncTimeout` fires when picoquic's
next wake time arrives. A `MoQFollyExecutorImpl` backed by the same EventBase
serves as the executor for session coroutines.

This model allows picoquic sessions to share an event loop with H3 connections or
other EventBase-based work.

---

## 1. Connection Establishment

### Server Startup — `MoQPicoServerBase::createQuicContext`

Called by both server variants before starting I/O:

1. `picoquic_create` allocates a QUIC context (`picoquic_quic_t`). The default
   callback is `picoCallback` with `this` as context.
2. `picoquic_set_alpn_select_fn_v2` installs `alpnSelectCallback`, which matches
   client-proposed ALPNs against `getDefaultMoqtProtocols(true)` in
   server-preference order.

#### Thread-based variant (`MoQPicoQuicServer::start`)

1. A `PicoQuicExecutor` is created — one shared across all future connections.
2. `createQuicContext()` is called.
3. `picoquic_start_network_thread` launches the I/O thread, passing
   `PicoQuicExecutor::loopCallbackStatic` so the executor integrates into the
   packet loop.

#### EventBase variant (`MoQPicoQuicEventBaseServer::start`)

1. The caller supplies `MoQFollyExecutorImpl*` (backed by its EventBase).
   `executor_` is set to a `shared_ptr` with a no-op deleter (caller retains
   ownership).
2. `createQuicContext()` is called.
3. A `PicoQuicSocketHandler` is created and `start(addr)` is called on it,
   binding the UDP socket and beginning EventBase-driven I/O.

### New Server Connection (`picoquic_callback_ready` → `onNewConnectionImpl`)

Until the connection is `ready`, `callback_ctx` is the raw `MoQPicoServerBase*`.
On `picoquic_callback_ready`:

1. Local and peer addresses are extracted via `picoquic_get_local_addr` /
   `picoquic_get_peer_addr`.
2. A `PicoQuicWebTransport` is constructed with the `picoquic_cnx_t*` and
   addresses. Its constructor:
   - Determines client vs. server role via `picoquic_is_client`.
   - Creates a `WtStreamManager` with all flow-control limits set to `max()`
     (picoquic handles flow control internally and never calls back with
     `MAX_STREAMS`/`MAX_DATA` events — see `awaitUniStreamCredit` comments).
   - Calls `picoquic_set_callback(cnx, picoCallback, this)`.
3. `onWebTransportCreated(wt)` is called — the EventBase server overrides this
   to install the `updateWakeTimeoutCallback` (see §Wake Timeout Optimization).
4. `MoQServerBase::createSession(webTransport, executor_)` constructs a
   `MoQSession`.
5. `webTransport->setHandler(moqSession.get())` — the session becomes the
   `WebTransportHandler`.
6. The negotiated ALPN is handed to `moqSession->validateAndSetVersionFromAlpn`.
7. A `ConnectionContext` (containing both `webTransport` and `moqSession`) is
   heap-allocated. A magic value (`0xC099EC71`) distinguishes it from the old
   server pointer in subsequent callbacks.
8. `picoquic_set_callback(cnx, picoCallback, ctx)` switches the per-connection
   callback context from the server pointer to the `ConnectionContext*`.
9. `folly::coro::co_withExecutor(executor_, handleClientSession(moqSession)).start()`
   schedules the MoQ session coroutine.

### Outgoing Client Connection (`MoQPicoQuicEventBaseClient`)

1. The caller subclasses `MoQPicoQuicEventBaseClient` and overrides `onSession`.
2. `connect(addr, sni, alpn)` creates a `picoquic_quic_t` client context,
   constructs a `PicoQuicSocketHandler`, and calls `picoquic_create_client_cnx`.
3. On `picoquic_callback_ready`:
   - A `PicoQuicWebTransport` is created (client role).
   - `MoQSession` is created via the overridable `createSession()` factory.
   - The ALPN is validated.
   - `onSession(moqSession)` is called so the caller can configure handlers and
     start subscribe/publish work.
4. `close()` tears down the connection and the socket handler.

---

## 2a. PicoQuicExecutor — Thread-based Event Loop Integration

`PicoQuicExecutor` hooks into four picoquic packet loop phases:

| Phase | Action |
|---|---|
| `picoquic_packet_loop_ready` | Enables `do_time_check` so the executor can control epoll timeout |
| `picoquic_packet_loop_time_check` | Sets `delta_t = 0` if tasks are pending; otherwise uses next timer deadline; caps at `kMaxWakeDelayUs` (200 ms) |
| `picoquic_packet_loop_after_receive` | Calls `drainTasks()` and `processExpiredTimers()` |
| `picoquic_packet_loop_after_send` | Same — `drainTasks()` and `processExpiredTimers()` |

`add(folly::Func)` is thread-safe (mutex-protected), so coroutines scheduled
from other threads are picked up within at most 200 ms, or immediately on the
next I/O event.

Timers use `picoquic_current_time()` (microseconds). Each scheduled timer gets a
`TimerCallbackImplHandle` (a `QuicTimerCallback::TimerCallbackImpl`) for
cancellation.

---

## 2b. PicoQuicSocketHandler — EventBase-driven I/O Engine

`PicoQuicSocketHandler` drives picoquic from a `folly::EventBase`. It is shared
by `MoQPicoQuicEventBaseServer` and `MoQPicoQuicEventBaseClient`.

### Receiving packets

`AsyncUDPSocket` is used in **notify-only** mode: rather than receiving data
through the callback, `onNotifyDataAvailable` calls `recvmmsg` directly with a
hand-crafted `mmsghdr` array. This preserves `IP_PKTINFO` (local destination
address) and TOS/ECN ancillary data that `AsyncUDPSocket`'s standard path does
not expose. Each received packet is passed to `picoquic_incoming_packet`.

### Sending packets

`drainOutgoing` calls `picoquic_prepare_next_packet_ex` in a loop. Each packet
is sent via `::sendmsg` with:
- `IP_PKTINFO` for per-packet source address selection (multi-homed hosts).
- `UDP_SEGMENT` for **GSO** coalescing when the kernel supports it.

### Wake timer

`AsyncTimeout::scheduleTimeoutHighRes` is used to schedule the picoquic wake
timer. `rescheduleTimer` reads `picoquic_get_next_wake_delay` and programs the
next timeout. On expiry, `timeoutExpired` drains outgoing packets and
reschedules.

---

## 3. Wake Timeout Optimization — `WakeTimeGuard`

When `markStreamActive` or `markDatagramActive` is called, picoquic's next wake
time may decrease (e.g. because a new stream becomes ready to send). In the
EventBase model the socket handler's timer would otherwise not fire until the
previously scheduled deadline, introducing up to `kMaxWakeDelayUs` of latency.

To avoid this, a `WakeTimeGuard` RAII helper snapshots `picoquic_get_next_wake_delay`
**before** the mark call and compares it **after**. If the wake time decreased,
it invokes `updateWakeTimeoutCallback` — a functor set by
`MoQPicoQuicEventBaseServer::onWebTransportCreated` that calls
`PicoQuicSocketHandler::updateWakeTimeout()`. That method cancels the pending
timer, drains any already-ready packets, and reschedules at the new (earlier)
deadline.

`MoQPicoQuicEventBaseClient` sets the same callback on its socket handler.

---

## 4. Stream Reads (Ingress)

```
picoquic I/O thread (or EventBase)
  -> picoCallback (picoquic_callback_stream_data / _stream_fin)
  -> PicoQuicWebTransport::onStreamData(stream_id, bytes, length, fin)
       -> streamManager_->getOrCreateIngressHandle(stream_id)   // creates handle if new
       -> IOBuf::copyBuffer(bytes, length)                      // copy into IOBuf
       -> streamManager_->enqueue(*readHandle, {data, fin})     // push into per-stream buffer
       -> if stream_id in pendingStreamNotifications_:
            -> handler_->onNewUniStream(readHandle)   OR
            -> handler_->onNewBidiStream(bidiHandle)            // notify MoQSession once, on first data
```

**Deferred handler notification:** `WtStreamManager::IngressCallback::onNewPeerStream`
is called during `getOrCreateIngressHandle` for peer-initiated streams, but the
handle is not fully inserted yet at that point (recursion risk). The stream ID is
stashed in `pendingStreamNotifications_` and the handler is called only after
`enqueue` succeeds on the first `onStreamData` invocation.

`MoQSession` reads stream data by calling `readStreamData(id)`, which delegates
to `WtStreamManager::StreamReadHandle::readStreamData()` and returns a
`SemiFuture<StreamData>`. The future resolves when `enqueue` fulfills the pending
promise, delivering the `IOBuf` chain to the session coroutine.

---

## 5. Stream Writes (Egress)

The path uses picoquic's **Just-In-Time (JIT)** send model — no data is pushed
into picoquic proactively.

### Step 1 — Application writes data

```
MoQSession -> writeStreamData(id, IOBuf, fin)
  -> streamManager_->getOrCreateEgressHandle(id)
  -> handle->writeStreamData(data, fin, callback)   // buffer in WtStreamManager
  -> markStreamActive(id)
       -> [WakeTimeGuard snapshots wake time]
       -> picoquic_mark_active_stream(cnx_, id, 1, nullptr)
       -> [WakeTimeGuard fires updateWakeTimeoutCallback if wake time decreased]
```

This only signals to picoquic that stream `id` has data. No bytes move yet.

### Step 2 — Picoquic calls back when ready to send

```
picoquic I/O thread (or EventBase)
  -> picoCallback (picoquic_callback_prepare_to_send)
  -> PicoQuicWebTransport::onPrepareToSend(streamId, context, maxLength, ...)
       -> streamManager_->getOrCreateEgressHandle(streamId)
       -> streamManager_->dequeue(*handle, maxLength)      // dequeue up to maxLength bytes
       -> picoquic_provide_stream_data_buffer(context, dataSize, fin, is_still_active)
            // returns a picoquic-owned buffer of `dataSize` bytes
       -> memcpy IOBuf chain into that buffer
       -> handler_->onByteEvent(streamId, offset)          // fired optimistically on send
       -> if fin or no more data:
            picoquic_mark_active_stream(cnx_, streamId, 0, nullptr)   // deactivate
            if priorityQueue_ non-empty: markStreamActive(nextStreamId) // wake next
```

`picoquic_provide_stream_data_buffer` both allocates the wire buffer and informs
picoquic of the stream's active state for the next round.

**`onByteEvent` is fired optimistically** — immediately when data is handed to
picoquic, not on ACK (picoquic has no ACK delivery callback). This ensures
`keepaliveForDeliveryCallbacks_` in `StreamPublisherImpl` is cleared promptly and
does not leak.

### Stream Creation

- **Outgoing uni:** `picoquic_get_next_local_stream_id(cnx_, 1)` allocates an
  ID, `picoquic_set_app_stream_ctx` reserves it in picoquic's internal state,
  then `WtStreamManager::getOrCreateEgressHandle` creates the write handle.
- **Outgoing bidi:** Same but `is_unidir=0`, and `getOrCreateBidiHandle` is used.

### Egress Control Events (RESET, STOP_SENDING, session close)

`WtStreamManager::EgressCallback::eventsAvailable()` triggers
`processEgressEvents()`, which drains the `WtStreamManager` event queue and
translates each event to the corresponding picoquic API:

| WtStreamManager event | picoquic call |
|---|---|
| `ResetStream` | `picoquic_reset_stream` |
| `StopSending` | `picoquic_stop_sending` |
| `CloseSession` | `picoquic_close` |

After processing events, `processEgressEvents` also checks `priorityQueue_` and
marks the highest-priority waiting stream active.

---

## 6. Datagram Reads (Ingress)

```
picoquic I/O thread (or EventBase)
  -> picoCallback (picoquic_callback_datagram)
  -> PicoQuicWebTransport::onReceiveDatagram(bytes, length)
       -> IOBuf::copyBuffer(bytes, length)
       -> handler_->onDatagram(datagram)       // direct delivery to MoQSession
```

No buffering — datagrams are delivered immediately to the handler.

---

## 7. Datagram Writes (Egress)

Same JIT model as streams, but simpler since datagrams have no stream state.

### Step 1 — Application queues datagram

```
MoQSession -> sendDatagram(IOBuf)
  -> datagramQueue_.push_back(datagram)
  -> markDatagramActive()
       -> [WakeTimeGuard snapshots wake time]
       -> picoquic_mark_datagram_ready(cnx_, 1)
       -> [WakeTimeGuard fires updateWakeTimeoutCallback if wake time decreased]
```

### Step 2 — Picoquic calls back when it can fit a datagram

```
picoquic I/O thread (or EventBase)
  -> picoCallback (picoquic_callback_prepare_datagram)
  -> PicoQuicWebTransport::onPrepareDatagram(context, maxLength, written)
       -> datagramQueue_.front()                     // peek at head
       -> if datagramLen > maxLength: return          // can't fragment, skip this slot
       -> picoquic_provide_datagram_buffer(context, datagramLen)   // get wire buffer
       -> memcpy IOBuf chain into buffer
       -> datagramQueue_.pop_front()
       -> if queue empty: picoquic_mark_datagram_ready(cnx_, 0)    // deactivate
```

Datagrams are **atomic** — they must fit entirely within `maxLength`. If they
don't, the callback returns without writing anything and picoquic will retry with
a larger slot in a subsequent packet.

---

## 8. Connection Close

| Trigger | Path |
|---|---|
| Peer closes | `picoCallback(close/application_close)` → `onConnectionClose` → `streamManager_->onCloseSession` → `handler_->onSessionEnd` |
| Local close | `closeSession(error)` → `streamManager_->shutdown` → `picoquic_close` → `handler_->onSessionEnd` → [3xPTO drain] → `picoCallback(close)` → `onConnectionClose` → `clearPicoquicCallback` |
| Server shutdown (thread) | `picoquic_delete_network_thread` + `picoquic_free` → triggers close callbacks for all active connections |
| Server shutdown (EventBase) | `PicoQuicSocketHandler::stop()` + `destroyQuicContext()` → triggers close callbacks |

For local close, `closeSession()` does **not** clear the picoquic callback immediately.
Picoquic retransmits `CONN_CLOSE` for up to 3×PTO before firing `picoquic_callback_close`,
which routes to `onConnectionClose()` → `clearPicoquicCallback()`. `handler_` is exchanged
to null in `closeSession()` so the subsequent `onConnectionClose()` does not trigger a
double `onSessionEnd`. `std::exchange` is also used there to guard against re-entrancy.

On the server side, the heap-allocated `ConnectionContext` is deleted inside
`picoCallback` on `close`/`application_close`/`stateless_reset`, after
`moqSession->onSessionEnd` has been called. The magic field is overwritten with
`0xDEADBEEF` before deletion to catch any subsequent use-after-free.

---

## 9. Samples

| Binary | Class | Description |
|---|---|---|
| `pico_relay_server` | `MoQPicoQuicServer` | Thread-based MoQ relay |
| `pico_evb_relay_server` | `MoQPicoQuicEventBaseServer` | EventBase MoQ relay (shares event loop with other folly async work) |
| `pico_evb_text_client` | `MoQPicoQuicEventBaseClient` | EventBase subscriber; prints received text objects to stdout |

### `pico_relay_server`

Minimal thread-based relay. Passes `--cert`, `--key`, `--port`, `--endpoint`,
and `--versions`. The picoquic packet loop runs in its own thread; main blocks on
`server.start()`.

### `pico_evb_relay_server`

EventBase relay. Owns a `folly::EventBase` and a `MoQFollyExecutorImpl` backed by
it. Lifetimes are managed carefully: `relay` and `server` are declared before
`evb` so their destructors run after `evb`'s destructor drains pending coroutine
continuations. Signal handlers call `evb.terminateLoopSoon()`.

### `pico_evb_text_client`

Connects to a MoQ server and subscribes to a track, printing each received object
to stdout. Parses `--connect_url` (e.g. `moqt://host:9668`) with `proxygen::URL`
to extract host and port. Calls `getMoqtProtocols(FLAGS_versions)` to pick the
ALPN. On `onPublishDone` or SIGINT, calls `stop()` which unsubscribes, closes the
session, and terminates the EventBase loop.
