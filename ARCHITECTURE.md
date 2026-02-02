# Moxygen Architecture

## Overview

**moxygen** is a general-purpose C++ library for implementing [MOQT (Media over QUIC Transport)](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/) applications. It provides a complete implementation of the MOQT protocol with flexible, composable APIs for building publishers, subscribers, relays, and custom media delivery applications.

The library is designed around two key principles:

1. **Symmetric APIs**: Publisher and Subscriber interfaces are symmetric, enabling components to be easily chained together for filtering, proxying, and relay scenarios
2. **Transport Agnostic**: Core protocol logic is decoupled from transport via the `proxygen::WebTransport` interface, supporting HTTP/3 WebTransport, native QUIC, and alternative implementations like PicoQuic

## Core Architecture

### Control Plane API

The control plane defines how publishers and subscribers establish communication and manage tracks:

#### Publisher Interface (`Publisher.h`)

The `Publisher` interface represents a component that can respond to subscription requests. Key methods include:

- **`subscribe()`**: Handles incoming SUBSCRIBE requests, returns a `SubscriptionHandle` for managing the subscription lifecycle
- **`fetch()`**: Handles FETCH requests for historical data, returns a `FetchHandle`
- **`subscribeNamespace()`**: Handles SUBSCRIBE_NAMESPACE requests (relay-specific)
- **`trackStatus()`**: Responds to TRACK_STATUS_REQUEST messages

The `SubscriptionHandle` returned from `subscribe()` provides:
- **`unsubscribe()`**: Terminates the subscription
- **`subscribeUpdate()`**: Updates subscription parameters (range, priority, forward preference)

#### Subscriber Interface (`Subscriber.h`)

The `Subscriber` interface represents a component that can receive track namespace publishes and handle publish requests. Key methods include:

- **`publishNamespace()`**: Publishes availability of a track namespace, returns an `PublishNamespaceHandle`
- **`publish()`**: Handles incoming PUBLISH requests from peers (synchronous API returning both consumer and async reply)

The symmetric design means:
- **Subscribers call `subscribe()`** on sessia publisher to request data
- **Publishers implement `subscribe()`** to handle incoming subscription requests
- This symmetry enables **chaining**: output from one component feeds into the next

**Correctness by design**

The control plane APIs are coroutines that allow for asynchronous completion, but must return the correct message
(for example: `Expected<SubscribeOk, RequestError>`).


### Data Plane API

The data plane defines how objects flow through the system via consumer interfaces (`MoQConsumers.h`):

#### TrackConsumer Interface

Represents a subscription to a track, receiving objects across all groups and subgroups:

- **`setTrackAlias()`**: Associates a numeric alias with this track
- **`beginSubgroup()`**: Starts a new subgroup, returns a `SubgroupConsumer`
- **`objectStream()`**: Delivers a complete object on its own stream
- **`datagram()`**: Delivers an object via unreliable datagram
- **`subscribeDone()`**: Signals completion of the subscription
- **`awaitStreamCredit()`**: Backpressure mechanism for flow control

#### SubgroupConsumer Interface

Handles object delivery within a single subgroup (group + subgroup ID):

- **`object()`**: Delivers a complete object with payload
- **`beginObject()` / `objectPayload()`**: Streams large objects in chunks
- **`endOfGroup()` / `endOfTrackAndGroup()`**: Status messages signaling boundaries
- **`endOfSubgroup()`**: Closes the subgroup stream
- **`reset()`**: Terminates with error
- **`checkpoint()`**: Marks reliable delivery boundary

#### FetchConsumer Interface

Similar to `SubgroupConsumer` but for FETCH requests, with added backpressure support:

- Supports the same object delivery methods
- **`awaitReadyToConsume()`**: Explicit backpressure control (can return `BLOCKED` error)
- Enforces strict ordering: objects must be delivered in increasing group/object order

### Symmetry and Composition

The symmetric design enables powerful composition patterns:

```
Subscriber calls:                Publisher implements:
session->subscribe(...)    →     myPublisher.subscribe(...)
                           ←     returns SubscriptionHandle

Publisher calls:                 Subscriber implements:
session->publishNamespace(...)  →  mySubscriber.publishNamespace(...)
                                ←  returns PublishNamespaceHandle
```

This symmetry allows components to be chained:
- **Filters**: Subscriber receives data, processes it, republishes to downstream consumers
- **Relays**: Implement both Publisher and Subscriber to forward data between sessions
- **Caches**: Store objects and replay them to new subscribers
- **Transcoders**: Receive one format, transform, publish another format

Example chain: `Publisher → Filter → Relay → Cache → Subscriber`

Each component in the chain:
1. Implements the consumer interface to receive data from upstream
2. Implements the publisher interface to serve data to downstream
3. Can modify, filter, or cache data in between


## Core Session Management

### MoQSession

`MoQSession` is the primary class that implements the MOQT protocol. It inherits from both `Publisher` and `Subscriber`, making it the key component for MOQT communication.

**Key Features:**

- **Dual Role**: Implements both publisher and subscriber interfaces
- **Transport Agnostic**: Operates over any `proxygen::WebTransport` implementation
- **Extensible**: Applications set custom handlers via `setPublishHandler()` and `setSubscribeHandler()`
- **Coroutine-Based**: Uses folly coroutines for async operations
- **Version Negotiation**: Supports multiple MOQT protocol versions

**Setup Flow:**

1. Create `MoQSession` with a `WebTransport` instance and executor
2. Optionally set custom Publisher/Subscriber handlers
3. Call `start()` to begin protocol negotiation
4. Call `setup()` to exchange CLIENT_SETUP/SERVER_SETUP messages
5. Begin subscribing and publishing

**Request Management:**

- Tracks pending requests (subscribe, fetch, etc.) by `RequestID`
- Enforces request ID parity and limits
- Handles graceful shutdown via `drain()` and `goaway()`

### MoQRelaySession

`MoQRelaySession` extends `MoQSession` with full namespace publishing functionality. While the base `MoQSession` returns `NOT_SUPPORTED` for namespace operations (suitable for simple clients), `MoQRelaySession` provides real implementations of:

- **`publishNamespace()`**: Forward namespace availability to subscribers
- **`subscribeNamespace()`**: Subscribe to namespace prefixes
- Namespace state management (namespace tracking, publish callbacks)

**Use Cases:**
- Relay servers that forward namespace availability between publishers and subscribers
- Applications that need to handle dynamic track discovery
- Multi-hop delivery scenarios

## Transport Layer

### Transport Abstraction

The core protocol logic is separated from transport via the `proxygen::WebTransport` interface, which provides:

- Bidirectional and unidirectional streams
- Datagrams
- Flow control and backpressure

This abstraction allows moxygen to work with multiple QUIC implementations:

### HTTP/3 WebTransport (Default)

**MoQClient** and **MoQServer** use Proxygen's HTTP/3 WebTransport implementation:

- **MoQClient**: Connects to HTTP/3 servers via `proxygen::URL`
  - Performs QUIC connection and HTTP/3 WebTransport session setup
  - Returns a `MoQSession` ready for MOQT communication

- **MoQServer**: Accepts HTTP/3 WebTransport connections
  - Listens on a socket address
  - Handles `/connect` requests for WebTransport
  - Creates `MoQSession` for each accepted connection
  - Based on Proxygen's HQServer infrastructure

### Native QUIC via QuicWebTransport

For scenarios requiring direct QUIC transport without HTTP/3:

- **QuicWebTransport**: Wraps mvfst (Meta's QUIC implementation) as a `WebTransport`
- Used internally by MoQClient/MoQServer
- Enables native QUIC support without HTTP layer overhead

## Relay and Forwarding

### MoQRelay

`MoQRelay` is a relay implementation that routes subscriptions and publishes between sessions:

**Architecture:**
- Implements both `Publisher` and `Subscriber` interfaces
- Maintains a namespace tree for tracking published namespaces
- Routes subscribe requests to appropriate publisher sessions
- Optionally integrates with `MoQCache` for object caching

**Namespace Routing:**
- `subscribeNamespace()`: Clients subscribe to namespace prefixes
- `publishNamespace()`: Publishers publish track namespace availability
- Relay maintains mappings and forwards namespace availability to interested subscribers
- Supports hierarchical namespaces with prefix matching

**Subscription Routing:**
- Incoming `subscribe()` request looks up announcing session via namespace
- Creates `MoQForwarder` to manage data flow
- Forwards subscription upstream if needed
- Returns consumer that writes to downstream subscribers

**Publish Handling:**
- `publish()` requests register the publishing session for a track
- Future subscriptions are routed to this publisher
- Supports multiple publishers for different tracks

### MoQForwarder

`MoQForwarder` is a **reusable component** for fan-out data forwarding:

**Functionality:**
- Implements `TrackConsumer` to receive data from upstream
- Manages multiple downstream `Subscriber` instances
- Forwards objects to all active subscribers
- Handles subscription lifecycle (subscribe, unsubscribe, subscribeDone)

**Subscription Management:**
- Tracks each subscriber's session, request ID, range, and consumer
- Filters objects based on each subscriber's range and forward preference
- Handles per-subscriber backpressure and errors
- Gracefully drains subscribers when upstream completes

**Flow Control:**
- Clones payload handles for multi-subscriber delivery
- Opens subgroups on-demand for each subscriber
- Removes subscribers on fatal errors
- Signals `onEmpty()` callback when all subscribers are gone

**Use Outside Relay:**

`MoQForwarder` can be used by publishers to multicast data:
```cpp
auto forwarder = std::make_shared<MoQForwarder>(trackName);
// Add subscribers as they arrive
forwarder->addSubscriber(session1, subscribeReq1, consumer1);
forwarder->addSubscriber(session2, subscribeReq2, consumer2);

// Publish data once, forwarded to all
forwarder->beginSubgroup(groupId, subgroupId, priority);
forwarder->object(objectId, payload, extensions);
```

### MoQCache

Caching implementation (`moxygen/relay/MoQCache.h`):

- Stores published objects in memory
- Serves cached objects to new subscribers
- Integrates with `MoQRelay` via `getSubscribeWriteback()` and `getFetchWriteback()`
- Wraps consumer to intercept and cache objects

## Sample Applications

### Date Publisher (moqdateserver)

**Location:** `moxygen/samples/date/`

Simple publisher that broadcasts the current date/time:
- Publishes to `moq-date/date` track
- Generates new object every second
- Demonstrates basic publishing pattern
- Good starting point for learning the API

**Usage:**
```bash
moqdateserver -port 4433 -cert cert.pem -key key.pem
```

### Text Client (moqtextclient)

**Location:** `moxygen/samples/text-client/`

Command-line subscriber for text-based tracks:
- Connects to a publisher or relay
- Subscribes to specified namespace/track
- Prints received objects to stdout
- Supports various subscription parameters (latest group, object ranges, etc.)

**Usage:**
```bash
moqtextclient --connect_url "https://relay:4433/moq" \
              --track_namespace "moq-date" \
              --track_name "date"
```

### Chat Client (moqchatclient)

**Location:** `moxygen/samples/chat/`

Interactive chat application demonstrating bidirectional communication:
- Acts as both publisher and subscriber
- Publishes user messages to a chat track
- Subscribes to messages from other participants
- Shows how to combine publishing and subscribing

### FLV Streamer and Receiver

**Location:** `moxygen/samples/flv_streamer_client/` and `moxygen/samples/flv_receiver_client/`

Media streaming applications for testing with real video/audio:

- **MoQFlvStreamerClient**: Reads FLV files (H.264 + AAC), publishes as MOQT using MoqMi packaging
- **MoQFlvReceiverClient**: Subscribes to MOQT tracks, converts back to FLV output
- Enables testing with ffmpeg-generated content
- Demonstrates media packaging integration

## Executors and Event Loops

### MoQExecutor Interface

Moxygen uses an executor abstraction for event-driven operations:

**Interface (`moxygen/events/MoQExecutor.h`):**
- `schedule()`: Schedule a function to run on the executor
- `scheduleTimeout()`: Schedule delayed execution
- Enables integration with different event loop implementations

### Event-Driven Flow

MoQSession operations are event-driven:
- Control messages processed in `controlReadLoop()` and `controlWriteLoop()` coroutines
- Stream data handled via `unidirectionalReadLoop()` callbacks
- Datagrams processed in `onDatagram()` callback
- All operations run on the session's executor

## Threading Model

**Critical Constraint:**

> **Each `MoQSession` MUST only be accessed from the thread in which it was created.**

This constraint simplifies synchronization and avoids locking overhead:

- Sessions are bound to an executor's event loop thread
- All callbacks and coroutines run on the session's thread
- No internal locking required
- Applications using multiple threads must:
  - Create separate sessions per thread, OR
  - Ensure all session access is dispatched to the correct thread

**Multi-Threaded Server Example:**

```cpp
// Each worker thread has its own event base
std::vector<folly::EventBase*> evbs = server->getWorkerEvbs();

// Each accepted connection is assigned to a worker thread
// The MoQSession for that connection lives on that thread
// All operations for that session happen on its thread
```

**Implications:**
- Simple and lock-free within a session
- Natural scaling via multiple sessions across threads
- Care needed when coordinating across sessions (use thread-safe queues, etc.)

## Statistics and Monitoring

### MoQStats Interface

Built-in stats callbacks for monitoring:

- **MoQPublisherStatsCallback**: Track publishing metrics
  - Subscription stream lifecycle
  - Object delivery events
  - Errors and failures

- **MoQSubscriberStatsCallback**: Track subscription metrics
  - Stream open/close events
  - Object reception
  - Buffering and flow control

Applications can implement these interfaces and register them with `MoQSession`:
```cpp
session->setPublisherStatsCallback(myPublisherStats);
session->setSubscriberStatsCallback(mySubscriberStats);
```

### MLogger Integration

Structured logging via MLogger (`moxygen/mlog/MLogger.h`):
- Records protocol events, state transitions, errors
- Can be serialized for offline analysis
- Useful for debugging complex relay scenarios

## Advanced Features

### Flow Control and Backpressure

Moxygen provides multiple levels of flow control:

1. **Transport Level**: QUIC stream and connection flow control (automatic)

2. **Buffering Thresholds** (`MoQSettings`):
   - `perSubscription`: Maximum bytes buffered per subscription
   - `perSession`: Maximum bytes buffered per session
   - Subscriptions exceeding limits are terminated with `TOO_FAR_BEHIND`

3. **Application Backpressure**:
   - `FetchConsumer::awaitReadyToConsume()`: Explicit backpressure for fetch
   - `TrackConsumer::awaitStreamCredit()`: Wait for stream credit
   - Return `MoQPublishError::BLOCKED` to signal consumer can't accept more data

### Extensions and Parameters

MOQT supports extensions via parameter/extension fields:

- **TrackRequestParameters**: Passed with subscribe/fetch (delivery timeout, authorization, etc.)
- **Extensions**: Attached to individual objects (custom metadata)
- Moxygen provides helpers to encode/decode custom parameters

### Version Negotiation

Supports multiple MOQT protocol versions:
- Version negotiation during setup
- `setVersion()` to configure supported versions
- Protocol adapts behavior based on negotiated version
- Some features (like request IDs for publishes) vary by version
