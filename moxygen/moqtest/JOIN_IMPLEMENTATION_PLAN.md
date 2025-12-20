# Join Feature Implementation Plan

## Executive Summary

This document outlines the plan to add support for the `join` operation to the MoQTest conformance suite. The join operation combines fetch (historical data) with subscribe (live updates) in a single request, which is critical for real-world MoQT applications.

**Status**: Awaiting approval before implementation

## Background

### What is Join?

The `MoQSession::join()` method provides a unified way to:
1. **Fetch** historical data from a specified starting point
2. **Subscribe** to live updates seamlessly
3. **Avoid gaps** or duplicates at the transition boundary

This is essential for clients that need to "catch up" on missed data while staying live.

### Current State

The MoQTest suite currently supports:
- ✅ `subscribe` - Live subscription to tracks
- ✅ `fetch` - Retrieval of historical data
- ❌ `join` - Combined fetch + subscribe (MISSING)

### Why Join Matters

Join is critical for production scenarios:
- **Late joiners**: Users joining an ongoing live stream
- **Reconnection**: Recovering from brief network interruptions
- **Resilience**: Ensuring no data loss during catchup
- **User experience**: Seamless transition from historical to live

## Technical Details

### MoQSession::join() API

Located in: `fbcode/ti/experimental/moxygen/MoQSession.h`

```cpp
struct JoinResult {
  SubscribeResult subscribeResult;
  FetchResult fetchResult;
};

folly::coro::Task<JoinResult> join(
    SubscribeRequest subscribe,
    std::shared_ptr<TrackConsumer> subscribeCallback,
    uint64_t joiningStart,              // Where to start fetching from
    uint8_t fetchPri,                   // Fetch priority
    GroupOrder fetchOrder,              // Fetch order
    std::vector<TrackRequestParameter> fetchParams,
    std::shared_ptr<FetchConsumer> fetchCallback,
    FetchType fetchType
);
```

**Key Parameters:**
- `joiningStart`: Group number where fetch begins and subscribe takes over
- Two separate callbacks: one for historical data (fetch), one for live (subscribe)
- Returns both results, allowing validation of each component

## Implementation Plan

### Phase 1: Client Code Changes

#### 1.1 MoQTestClient.h

**Add new method:**
```cpp
folly::coro::Task<moxygen::TrackNamespace> join(
    MoQTestParameters params,
    uint64_t joiningStart
);
```

**Add new member variables:**
```cpp
// Separate receivers for join operation
std::shared_ptr<ObjectReceiver> joinSubReceiver_;
std::shared_ptr<ObjectReceiver> joinFetchReceiver_;

// Tracking for join validation
uint64_t fetchObjectsReceived_{};
uint64_t subscribeObjectsReceived_{};
bool fetchComplete_{};
```

**Update enum:**
```cpp
enum ReceivingType : int {
  SUBSCRIBE = 0,
  FETCH = 1,
  JOIN = 2,           // NEW
  UNKNOWN_RECEIVING_TYPE = 3
};
```

#### 1.2 MoQTestClient.cpp

**Implement join() method:**

```cpp
folly::coro::Task<moxygen::TrackNamespace> MoQTestClient::join(
    MoQTestParameters params,
    uint64_t joiningStart) {

  auto trackNamespace = convertMoqTestParamToTrackNamespace(&params);

  // Create SubscribeRequest
  SubscribeRequest sub;
  sub.requestID = kDefaultRequestId;
  sub.fullTrackName = {trackNamespace.value(), kDefaultTrackName};
  sub.groupOrder = kDefaultGroupOrder;
  sub.locType = LocationType::AbsoluteStart;  // Start at joiningStart
  sub.startGroup = joiningStart;

  // Create Fetch parameters
  uint8_t fetchPri = 128;  // Medium priority
  GroupOrder fetchOrder = GroupOrder::OldestFirst;
  std::vector<TrackRequestParameter> fetchParams;

  // Add delivery timeout if configured
  if (params.deliveryTimeout > 0) {
    fetchParams.push_back({
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
      "", params.deliveryTimeout, {}
    });
  }

  // Set current request
  receivingType_ = ReceivingType::JOIN;
  initializeExpecteds(params);
  fetchComplete_ = false;
  fetchObjectsReceived_ = 0;
  subscribeObjectsReceived_ = 0;

  // Call join
  auto res = co_await moqClient_->moqSession_->join(
      std::move(sub),
      joinSubReceiver_,
      joiningStart,
      fetchPri,
      fetchOrder,
      std::move(fetchParams),
      joinFetchReceiver_,
      FetchType::Group
  );

  // Validate results
  if (res.subscribeResult.hasError()) {
    XLOG(ERR) << "Join subscribe failed: "
              << res.subscribeResult.error();
  }

  if (res.fetchResult.hasError()) {
    XLOG(ERR) << "Join fetch failed: "
              << res.fetchResult.error();
  }

  co_return trackNamespace;
}
```

**Add validation logic:**
- Track which receiver callbacks are invoked
- Verify fetch completes before subscribe delivers live data
- Check for gaps or duplicates at the joiningStart boundary
- Validate object counts match expectations

#### 1.3 MoQTestClientMain.cpp

**Add command-line flags:**

```cpp
DEFINE_string(
    request,
    "subscribe",
    "Request Type: must be one of \"subscribe\", \"fetch\", or \"join\"");

DEFINE_uint64(
    joining_start,
    0,
    "For join requests: group number where fetch ends and subscribe begins");

DEFINE_uint64(
    fetch_priority,
    128,
    "For join requests: priority for fetch portion");

DEFINE_string(
    fetch_order,
    "oldest_first",
    "For join requests: order for fetch (\"oldest_first\" or \"newest_first\")");
```

**Add join handling in main():**

```cpp
} else if (FLAGS_request == "join") {
  XLOG(INFO) << "Joining from " << FLAGS_joining_start
             << " to " << url.getHostAndPort();
  folly::coro::blockingWait(co_withExecutor(
      evb.getEventBase(),
      client->join(defaultMoqParams, FLAGS_joining_start)));
} else {
  XLOG(ERR) << "Invalid Request Type: " << FLAGS_request;
}
```

### Phase 2: Conformance Test Updates

#### 2.1 Add Section 8: Join Tests

Add to `conformance_test.sh`:

```bash
# ============================================================================
# SECTION 8: Join Operations (Tests 51-60)
# ============================================================================
echo -e "\n${YELLOW}=== SECTION 8: Join Operations ===${NC}"
set_section "Join Operations"

# Test 51: Basic join
run_test "Basic join from group 5" \
    "join" \
    --joining_start=5 \
    --last_group=10 \
    --objects_per_group=5

# Test 52: Join with ONE_SUBGROUP_PER_OBJECT
run_test "Join with ONE_SUBGROUP_PER_OBJECT" \
    "join" \
    --forwarding_preference=1 \
    --joining_start=3 \
    --last_group=8 \
    --objects_per_group=4

# Test 53: Join at start (no historical data)
run_test "Join at start (group 0)" \
    "join" \
    --joining_start=0 \
    --last_group=5 \
    --objects_per_group=5

# Test 54: Join near current (minimal fetch)
run_test "Join near current (one group back)" \
    "join" \
    --joining_start=9 \
    --last_group=10 \
    --objects_per_group=5

# Test 55: Join with TWO_SUBGROUPS
run_test "Join with TWO_SUBGROUPS_PER_GROUP" \
    "join" \
    --forwarding_preference=2 \
    --joining_start=4 \
    --last_group=10 \
    --objects_per_group=6

# Test 56: Join with end of group markers
run_test "Join with end of group markers" \
    "join" \
    --joining_start=3 \
    --last_group=8 \
    --objects_per_group=5 \
    --send_end_of_group_markers=true

# Test 57: Join with extensions
run_test "Join with both extensions" \
    "join" \
    --joining_start=2 \
    --last_group=6 \
    --objects_per_group=4 \
    --test_integer_extension=1 \
    --test_variable_extension=1

# Test 58: Join with large historical data
run_test "Join with large historical gap" \
    "join" \
    --start_group=0 \
    --joining_start=50 \
    --last_group=100 \
    --objects_per_group=10 \
    --object_frequency=100

# Test 59: Join with object increments
run_test "Join with object increment" \
    "join" \
    --joining_start=5 \
    --last_group=15 \
    --objects_per_group=8 \
    --object_increment=2

# Test 60: Complex join scenario
run_test "Complex join with all features" \
    "join" \
    --forwarding_preference=2 \
    --joining_start=10 \
    --last_group=20 \
    --group_increment=2 \
    --objects_per_group=8 \
    --object_increment=2 \
    --send_end_of_group_markers=true \
    --test_integer_extension=1
```

### Phase 3: Validation Strategy

#### 3.1 Correctness Checks

The implementation must verify:

1. **Fetch portion completes first**
   - All historical objects delivered before live objects
   - fetchComplete_ flag set correctly

2. **No duplicates at boundary**
   - Objects at joiningStart not delivered twice
   - Group/object IDs are strictly increasing

3. **No gaps at boundary**
   - Last fetch object ID + increment = first subscribe object ID
   - No missing groups or objects

4. **Correct object counts**
   - fetchObjectsReceived_ matches expected historical count
   - subscribeObjectsReceived_ matches expected live count
   - Total matches overall expectations

5. **Success/failure propagation**
   - Both portions can succeed independently
   - Either failure reported correctly

#### 3.2 Test Validation Output

Enhanced logging:
```
MoQTest DEBUGGING: Join operation starting
MoQTest DEBUGGING: Fetch portion: groups 0-5, expecting X objects
MoQTest DEBUGGING: Subscribe portion: from group 5, expecting Y objects
MoQTest DEBUGGING: Fetch complete: received X objects
MoQTest DEBUGGING: Subscribe started: first object at group 5
MoQTest verification result: SUCCESS! Join completed - fetch: X objects, subscribe: Y objects, no gaps/duplicates
```

Or on failure:
```
MoQTest verification result: FAILURE! reason: Join boundary error - duplicate object at group 5, object 0
```

## Testing Strategy

### Unit Testing
- Mock MoQSession to test join logic independently
- Verify boundary conditions (joiningStart at 0, at max, etc.)
- Test error handling (fetch fails, subscribe fails, both fail)

### Integration Testing
- Run against real MoQTest server
- Verify with different server implementations
- Test under load and with delays

### Conformance Testing
- All 10 new join tests must pass
- Existing 50 tests must still pass (no regression)
- Cross-check with other MoQT implementations

## Timeline

Estimated effort: 2-3 days

1. **Day 1**: Client code changes (MoQTestClient.h/cpp)
2. **Day 2**: Main binary changes and basic testing
3. **Day 3**: Conformance tests and validation

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Boundary detection complexity | Medium | Extensive unit tests for edge cases |
| Server doesn't support join | High | Fallback tests or skip gracefully |
| Race conditions in callbacks | Medium | Proper synchronization in receivers |
| Performance impact | Low | Join inherently combines two operations |

## Success Criteria

Implementation is complete when:

✅ All 10 new join tests pass consistently
✅ No regression in existing 50 tests
✅ Boundary validation correctly detects duplicates/gaps
✅ Error handling works for all failure modes
✅ Documentation updated (README, inline comments)
✅ Code reviewed and approved

## Approval Required

Please review this plan and provide feedback on:

1. **Approach**: Is the overall strategy sound?
2. **Scope**: Are 10 join tests sufficient coverage?
3. **Validation**: Is the boundary checking robust enough?
4. **Priority**: When should this be implemented?

Once approved, implementation can begin immediately.

---

**Author**: Confucius AI
**Date**: 2024-11-01
**Status**: AWAITING APPROVAL
