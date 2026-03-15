# PicoQuic Transport Data Flow

This document describes the data flow introduced by the picoquic transport backend
for MoQ. It covers the three-layer architecture, thread model, connection
establishment, stream and datagram reads and writes, and connection teardown.

## Architecture Overview

The picoquic stack adds a self-contained QUIC transport path using picoquic
instead of mvfst/proxygen-QUIC. The three layers are:

```
MoQPicoQuicServer              <- server lifecycle, ALPN negotiation, connection bootstrap
    └── PicoQuicWebTransport   <- proxygen::WebTransport adaptor for picoquic
            └── PicoQuicExecutor   <- folly::Executor / timer integration into picoquic's packet loop
```

`MoQSession` and everything above it remain unchanged. This stack only
introduces a new transport backend.

## Thread Model

There is a **single network thread** spawned by `picoquic_start_network_thread`.
All picoquic I/O, connection callbacks, stream callbacks, and executor tasks run
on this one thread. `PicoQuicExecutor` is the mechanism that lets
`folly::coro` coroutines and timer callbacks execute on that same thread.

---

## 1. Connection Establishment

### Server Startup (`MoQPicoQuicServer::start`)

1. A `PicoQuicExecutor` is created — one shared across all future connections.
2. `picoquic_create` allocates a QUIC context (`picoquic_quic_t`). The default
   callback is `MoQPicoQuicServer::picoCallback` with `this` as context.
3. `picoquic_set_alpn_select_fn_v2` installs `alpnSelectCallback`, which matches
   client-proposed ALPNs against `getDefaultMoqtProtocols(true)` in
   server-preference order.
4. `picoquic_start_network_thread` launches the I/O thread, passing
   `PicoQuicExecutor::loopCallbackStatic` so the executor integrates into the
   packet loop.

### New Connection (`picoquic_callback_ready` → `onNewConnection`)

Until the connection is `ready`, `callback_ctx` is the raw `MoQPicoQuicServer*`.
On `picoquic_callback_ready`:

1. Local and peer addresses are extracted via `picoquic_get_local_addr` /
   `picoquic_get_peer_addr`.
2. A `PicoQuicWebTransport` is constructed with the `picoquic_cnx_t*` and
   addresses. Its constructor:
   - Determines client vs. server role via `picoquic_is_client`.
   - Creates a `WtStreamManager` with all flow-control limits set to `max()`
     (picoquic handles flow control internally and never calls back with
     `MAX_STREAMS`/`MAX_DATA` events — see `awaitUniStreamCredit` comments).
   - Calls `picoquic_set_callback(cnx, picoCallback, this)` — overwritten below.
3. `MoQServerBase::createSession(webTransport, executor_)` constructs a
   `MoQSession`.
4. `webTransport->setHandler(moqSession.get())` — the session becomes the
   `WebTransportHandler`.
5. The negotiated ALPN is read and handed to
   `moqSession->validateAndSetVersionFromAlpn`.
6. A `ConnectionContext` (containing both `webTransport` and `moqSession`) is
   heap-allocated. A magic value (`0xC099EC71`) in the struct distinguishes it
   from the old server pointer in subsequent callbacks.
7. `picoquic_set_callback(cnx, picoCallback, ctx)` switches the per-connection
   callback context from the server pointer to the `ConnectionContext*`.
8. `folly::coro::co_withExecutor(executor_, handleClientSession(moqSession)).start()`
   schedules the MoQ session coroutine onto the picoquic network thread.

---

## 2. The Executor and Event Loop Integration

`PicoQuicExecutor` hooks into four picoquic packet loop phases:

| Phase | Action |
|---|---|
| `picoquic_packet_loop_ready` | Enables `do_time_check` so the executor can control epoll timeout |
| `picoquic_packet_loop_time_check` | Sets `delta_t = 0` if tasks are pending; otherwise uses next timer deadline; caps at 200 ms |
| `picoquic_packet_loop_after_receive` | Calls `drainTasks()` and `processExpiredTimers()` |
| `picoquic_packet_loop_after_send` | Same — `drainTasks()` and `processExpiredTimers()` |

`add(folly::Func)` is thread-safe (mutex-protected), so coroutines scheduled
from other threads are picked up within at most 200 ms, or immediately on the
next I/O event.

Timers use `picoquic_current_time()` (microseconds). Each scheduled timer gets a
`TimerCallbackImplHandle` (a `QuicTimerCallback::TimerCallbackImpl`) for
cancellation. Expired timers are fired in `processExpiredTimers` by calling
`callback->timeoutExpired()`.

---

## 3. Stream Reads (Ingress)

```
picoquic I/O thread
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

## 4. Stream Writes (Egress)

The path uses picoquic's **Just-In-Time (JIT)** send model — no data is pushed
into picoquic proactively.

### Step 1 — Application writes data

```
MoQSession -> writeStreamData(id, IOBuf, fin)
  -> streamManager_->getOrCreateEgressHandle(id)
  -> handle->writeStreamData(data, fin, callback)   // buffer in WtStreamManager
  -> markStreamActive(id)
       -> picoquic_mark_active_stream(cnx_, id, 1, nullptr)
```

This only signals to picoquic that stream `id` has data. No bytes move yet.

### Step 2 — Picoquic calls back when ready to send

```
picoquic I/O thread
  -> picoCallback (picoquic_callback_prepare_to_send)
  -> PicoQuicWebTransport::onPrepareToSend(streamId, context, maxLength, ...)
       -> streamManager_->getOrCreateEgressHandle(streamId)
       -> streamManager_->dequeue(*handle, maxLength)      // dequeue up to maxLength bytes
       -> picoquic_provide_stream_data_buffer(context, dataSize, fin, is_still_active)
            // returns a picoquic-owned buffer of `dataSize` bytes
       -> memcpy IOBuf chain into that buffer
       -> if fin or no more data:
            picoquic_mark_active_stream(cnx_, streamId, 0, nullptr)   // deactivate
            if priorityQueue_ non-empty: markStreamActive(nextStreamId) // wake next
```

`picoquic_provide_stream_data_buffer` both allocates the wire buffer and informs
picoquic of the stream's active state for the next round. `is_still_active` is
set only when there is data to send (size > 0) or this chunk carries the FIN.

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

## 5. Datagram Reads (Ingress)

```
picoquic I/O thread
  -> picoCallback (picoquic_callback_datagram)
  -> PicoQuicWebTransport::onReceiveDatagram(bytes, length)
       -> IOBuf::copyBuffer(bytes, length)
       -> handler_->onDatagram(datagram)       // direct delivery to MoQSession
```

No buffering — datagrams are delivered immediately to the handler.

---

## 6. Datagram Writes (Egress)

Same JIT model as streams, but simpler since datagrams have no stream state.

### Step 1 — Application queues datagram

```
MoQSession -> sendDatagram(IOBuf)
  -> datagramQueue_.push_back(datagram)
  -> markDatagramActive()
       -> picoquic_mark_datagram_ready(cnx_, 1)
```

### Step 2 — Picoquic calls back when it can fit a datagram

```
picoquic I/O thread
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

## 7. Connection Close

| Trigger | Path |
|---|---|
| Peer closes | `picoCallback(close/application_close)` → `onConnectionClose` → `streamManager_->onCloseSession` → `handler_->onSessionEnd` |
| Local close | `closeSession(error)` → `streamManager_->shutdown` → `picoquic_close` → `clearPicoquicCallback` → `handler_->onSessionEnd` |
| Server shutdown | `picoquic_delete_network_thread` + `picoquic_free` → triggers close callbacks for all active connections |

`clearPicoquicCallback` sets the picoquic callback to `nullptr` and nulls
`cnx_`, preventing use-after-free if picoquic fires further events during
teardown. `std::exchange` is used when calling `handler_->onSessionEnd` to guard
against re-entrancy if the handler destroys the transport object.

On the server side, the heap-allocated `ConnectionContext` is deleted inside
`picoCallback` on `close`/`application_close`/`stateless_reset`, after
`moqSession->onSessionEnd` has been called. The magic field is overwritten with
`0xDEADBEEF` before deletion to catch any subsequent use-after-free.
