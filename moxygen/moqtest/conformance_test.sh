#!/bin/bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the Apache 2.0 license found in the
# LICENSE file in the root directory of this source tree.

#
# MoQTest Conformance Test Suite
#
# This script exercises the MoQTest client against a relay server
# and produces a conformance report covering a wide range of functionality.
#
# Usage: ./conformance_test.sh <relay_url>
# Example: ./conformance_test.sh http://localhost:9999

set -e

SKIP_FETCH=${SKIP_FETCH:-0}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Results array
declare -a TEST_RESULTS

# Section tracking
declare -a SECTION_NAMES
declare -a SECTION_TOTAL
declare -a SECTION_PASSED
declare -a SECTION_FAILED
CURRENT_SECTION=""

# Check arguments
if [ $# -eq 0 ]; then
    echo "Usage: $0 <relay_url> [versions] [Q|X]"
    echo "Example: $0 http://localhost:9999"
    echo "         $0 http://localhost:9999 16 Q   # raw QUIC"
    echo "         $0 http://localhost:9999 16 X   # QMUX-on-TCP"
    echo "         $0 http://localhost:9999 14,16"
    exit 1
fi

RELAY_URL="$1"

# Transport flag: default to HTTP/3 + WebTransport (Q = raw QUIC, X = QMUX-on-TCP)
# Versions flag: default to empty (all supported)
TRANSPORT_FLAG="--transport=h3wt"
VERSION_FLAG=()
shift 1
for arg in "$@"; do
    if [ "$arg" = "Q" ]; then
        TRANSPORT_FLAG="--transport=quic"
    elif [ "$arg" = "X" ]; then
        TRANSPORT_FLAG="--transport=qmux"
    elif [[ "$arg" =~ ^[0-9,]+$ ]]; then
        VERSION_FLAG=(--versions="$arg")
    fi
done

CLIENT_BINARY="${MOXYGEN_DIR:-.}/moxygen/moqtest/moqtest_client"

# Check if the client binary exists
if [ ! -x "$CLIENT_BINARY" ]; then
    echo -e "${RED}Error: MoQTest client binary not found at $CLIENT_BINARY${NC}"
    exit 1
fi

# If the getdeps.py script exists, set up the moxygen environment
if [ -x "./build/fbcode_builder/getdeps.py" ]; then
    eval "$("./build/fbcode_builder/getdeps.py" env moxygen)"
fi

# Function to set the current test section
set_section() {
    SECTION_NAMES[$1]="$2"
    CURRENT_SECTION="$1"
    if [ -z "${SECTION_TOTAL[$CURRENT_SECTION]}" ]; then
        SECTION_TOTAL[$CURRENT_SECTION]=0
        SECTION_PASSED[$CURRENT_SECTION]=0
        SECTION_FAILED[$CURRENT_SECTION]=0
    fi
}

# Function to run a test case
# SECTIONS="9 10" runs only those sections; unset runs everything.
section_enabled() {
    [ -z "${SECTIONS:-}" ] && return 0
    case " $SECTIONS " in
        *" $CURRENT_SECTION "*) return 0 ;;
        *) return 1 ;;
    esac
}

run_test() {
    local test_name="$1"
    local request_type="$2"
    shift 2
    local args=("$@")  # SC2124: assign as array

    section_enabled || return 0
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    echo -e "\n${BLUE}[Test $TOTAL_TESTS] $test_name${NC}"
    echo "  Request: $request_type"
    echo "  Args: ${args[*]}"

    # Run the test and capture output
    local output
    local exit_code

    if output=$("$CLIENT_BINARY" --url="$RELAY_URL" --request="$request_type" \
        "$TRANSPORT_FLAG" "${VERSION_FLAG[@]}" "${args[@]}" 2>&1); then
        exit_code=0
    else
        exit_code=$?
    fi

    # Check for success or failure in output
    echo "$output"
    local reason
    reason=$(verdict_of "$output" "$exit_code") || true
    record_result "$test_name" "$reason"
}

# The client's own verdict, as an empty string when it passed and a reason when
# it did not.
verdict_of() {
    local output="$1"
    local exit_code="$2"

    if echo "$output" | grep -q "MoQTest verification result: SUCCESS"; then
        return 0
    fi
    # Guarded: the script runs under `set -e` and grep exits non-zero when the
    # client died before printing a verdict at all.
    local reason
    reason=$(echo "$output" | grep "MoQTest verification result: FAILURE" | sed 's/.*reason: //') || true
    if [ -z "$reason" ]; then
        reason="Unexpected output (exit code: $exit_code)"
    fi
    echo "$reason"
}

# Records one outcome.  An empty `reason` means the test passed.
record_result() {
    local test_name="$1"
    local reason="$2"

    if [ -z "$reason" ]; then
        echo -e "${GREEN}  ✓ PASSED${NC}"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        TEST_RESULTS+=("PASS|$test_name|$CURRENT_SECTION")
        if [ -n "$CURRENT_SECTION" ]; then
            SECTION_PASSED[$CURRENT_SECTION]=$((SECTION_PASSED[$CURRENT_SECTION] + 1))
            SECTION_TOTAL[$CURRENT_SECTION]=$((SECTION_TOTAL[$CURRENT_SECTION] + 1))
        fi
    else
        echo -e "${RED}  ✗ FAILED: $reason${NC}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        TEST_RESULTS+=("FAIL|$test_name|$CURRENT_SECTION|$reason")
        if [ -n "$CURRENT_SECTION" ]; then
            SECTION_FAILED[$CURRENT_SECTION]=$((SECTION_FAILED[$CURRENT_SECTION] + 1))
            SECTION_TOTAL[$CURRENT_SECTION]=$((SECTION_TOTAL[$CURRENT_SECTION] + 1))
        fi
    fi
}

# Runs a joining FETCH.  A background subscriber holds the track open first,
# because a join against a track that starts with it has nothing to catch up
# on.  `expect` is "backfill" or "empty".
run_join_test() {
    local test_name="$1"
    local join_start="$2"
    local expect="$3"
    shift 3
    local args=("$@")

    section_enabled || return 0
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    echo -e "\n${BLUE}[Test $TOTAL_TESTS] $test_name${NC}"
    echo "  Request: subscribe --join_start=$join_start (expect $expect)"
    echo "  Args: ${args[*]}"

    local holder=""
    if [ "$expect" = "backfill" ]; then
        "$CLIENT_BINARY" --url="$RELAY_URL" --request=subscribe \
            "$TRANSPORT_FLAG" "${VERSION_FLAG[@]}" "${args[@]}" >/dev/null 2>&1 &
        holder=$!
        sleep "$JOIN_DELAY"
    fi

    local output
    local exit_code
    if output=$("$CLIENT_BINARY" --url="$RELAY_URL" --request=subscribe \
        --join_start="$join_start" "$TRANSPORT_FLAG" "${VERSION_FLAG[@]}" \
        "${args[@]}" 2>&1); then
        exit_code=0
    else
        exit_code=$?
    fi

    if [ -n "$holder" ]; then
        kill "$holder" 2>/dev/null || true
        wait "$holder" 2>/dev/null || true
    fi

    echo "$output"
    local reason
    reason=$(verdict_of "$output" "$exit_code") || true
    # A passing verdict is not enough: if the track had already finished the
    # join would have started a fresh one and validated it without ever
    # backfilling, so check which range the two halves actually met on.
    local backfill
    backfill=$(echo "$output" | grep -o "joining FETCH backfills.*" | tail -1) || true
    if [ -z "$reason" ]; then
        if [ "$expect" = "backfill" ] && ! echo "$backfill" | grep -q "groups"; then
            reason="Join backfilled nothing, so it did not land mid-track"
        elif [ "$expect" = "empty" ] && echo "$backfill" | grep -q "groups"; then
            reason="Join backfilled a track that had published nothing"
        fi
    fi
    if [ -z "$reason" ] && [ -n "$backfill" ]; then
        echo "  ${backfill}"
    fi
    record_result "$test_name" "$reason"
}


echo -e "\n${YELLOW}========================================${NC}"
echo -e "${YELLOW}  MoQTest Conformance Test Suite${NC}"
echo -e "${YELLOW}========================================${NC}"
echo "Relay URL: $RELAY_URL"
echo "Start Time: $(date)"

# ============================================================================
# SECTION 1: Basic Forwarding Preferences (Tests 1-8)
# ============================================================================
echo -e "\n${YELLOW}=== SECTION 1: Basic Forwarding Preferences ===${NC}"
set_section "1" "Basic Forwarding Preferences"

# Test 1: Default parameters (ONE_SUBGROUP_PER_GROUP)
run_test "Basic subscribe with default parameters" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=2 \
    --objects_per_group=5

# Test 2: ONE_SUBGROUP_PER_OBJECT
run_test "ONE_SUBGROUP_PER_OBJECT forwarding" \
    "subscribe" \
    --forwarding_preference=1 \
    --last_group=2 \
    --objects_per_group=5

# Test 3: TWO_SUBGROUPS_PER_GROUP
run_test "TWO_SUBGROUPS_PER_GROUP forwarding" \
    "subscribe" \
    --forwarding_preference=2 \
    --last_group=2 \
    --objects_per_group=6

# Test 4: DATAGRAM forwarding with small objects
run_test "DATAGRAM forwarding with small objects" \
    "subscribe" \
    --forwarding_preference=3 \
    --last_group=2 \
    --objects_per_group=5 \
    --size_of_object_zero=100 \
    --size_of_object_greater_than_zero=50

# Test 5-8: Fetch requests with different forwarding preferences
if [ "$SKIP_FETCH" -ne 1 ]; then
run_test "FETCH with ONE_SUBGROUP_PER_GROUP" \
    "fetch" \
    --forwarding_preference=0 \
    --last_group=2 \
    --objects_per_group=3

run_test "FETCH with ONE_SUBGROUP_PER_OBJECT" \
    "fetch" \
    --forwarding_preference=1 \
    --last_group=1 \
    --objects_per_group=4

run_test "FETCH with TWO_SUBGROUPS_PER_GROUP" \
    "fetch" \
    --forwarding_preference=2 \
    --last_group=1 \
    --objects_per_group=6

run_test "FETCH with DATAGRAM" \
    "fetch" \
    --forwarding_preference=3 \
    --last_group=1 \
    --objects_per_group=3 \
    --size_of_object_zero=80 \
    --size_of_object_greater_than_zero=40
fi

# ============================================================================
# SECTION 2: Object and Group Counts (Tests 9-16)
# ============================================================================
echo -e "\n${YELLOW}=== SECTION 2: Object and Group Counts ===${NC}"
set_section "2" "Object and Group Counts"

# Test 9: Single object per group
# last_object_in_track=0 makes object 0 the group's only object, so each
# datagram is both object-ID-zero and end-of-group.
run_test "Single object per group" \
    "subscribe" \
    --forwarding_preference=3 \
    --last_group=3 \
    --objects_per_group=1 \
    --last_object_in_track=0

# Test 10: Many objects per group
run_test "Many objects per group (20)" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=1 \
    --objects_per_group=20

# Test 11: Single group, multiple objects
run_test "Single group with 10 objects" \
    "subscribe" \
    --forwarding_preference=1 \
    --last_group=0 \
    --objects_per_group=10

# Test 12: Multiple groups, varying start
run_test "Start from group 5" \
    "subscribe" \
    --forwarding_preference=0 \
    --start_group=5 \
    --last_group=7 \
    --objects_per_group=3

# Test 13: Start from object 3
run_test "Start from object 3" \
    "subscribe" \
    --forwarding_preference=0 \
    --start_object=3 \
    --last_group=1 \
    --objects_per_group=8

# Test 14: Partial group delivery
run_test "Partial group (first 5 objects of 10)" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=1 \
    --objects_per_group=10 \
    --last_object_in_track=5

# Test 15: Fetch specific range
if [ "$SKIP_FETCH" -ne 1 ]; then
run_test "FETCH specific range" \
    "fetch" \
    --forwarding_preference=0 \
    --start_group=2 \
    --start_object=1 \
    --last_group=4 \
    --objects_per_group=5
fi

# Test 16: Single object fetch
if [ "$SKIP_FETCH" -ne 1 ]; then
run_test "FETCH single object" \
    "fetch" \
    --forwarding_preference=1 \
    --start_group=0 \
    --start_object=0 \
    --last_group=0 \
    --objects_per_group=5 \
    --last_object_in_track=1
fi


# ============================================================================
# SECTION 3: Object Sizes (Tests 17-24)
# ============================================================================
echo -e "\n${YELLOW}=== SECTION 3: Object Sizes ===${NC}"
set_section "3" "Object Sizes"

# Test 17: Tiny objects
run_test "Tiny objects (10 bytes)" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=2 \
    --objects_per_group=5 \
    --size_of_object_zero=10 \
    --size_of_object_greater_than_zero=10

# Test 18: Large object 0
run_test "Large object 0 (10KB)" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=1 \
    --objects_per_group=3 \
    --size_of_object_zero=10240 \
    --size_of_object_greater_than_zero=100

# Test 19: Large non-zero objects
run_test "Large non-zero objects (5KB)" \
    "subscribe" \
    --forwarding_preference=1 \
    --last_group=1 \
    --objects_per_group=4 \
    --size_of_object_zero=1024 \
    --size_of_object_greater_than_zero=5120

# Test 20: All large objects
run_test "All large objects (8KB)" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=1 \
    --objects_per_group=3 \
    --size_of_object_zero=8192 \
    --size_of_object_greater_than_zero=8192

# Test 21: Mixed sizes with TWO_SUBGROUPS
# Starting at object 3 puts subgroup 1's first object at 3, so its ID matches
# neither zero nor the first object and has to be written explicitly.  The
# group's last object is 6, so subgroup 0 is the one that ends the group.
run_test "Mixed sizes with TWO_SUBGROUPS_PER_GROUP" \
    "subscribe" \
    --forwarding_preference=2 \
    --start_object=3 \
    --last_group=1 \
    --objects_per_group=6 \
    --size_of_object_zero=2048 \
    --size_of_object_greater_than_zero=512

# Test 22: Single byte objects
run_test "Single byte objects" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=1 \
    --objects_per_group=5 \
    --size_of_object_zero=1 \
    --size_of_object_greater_than_zero=1

# Test 23: FETCH with large objects
if [ "$SKIP_FETCH" -ne 1 ]; then
run_test "FETCH with large objects" \
    "fetch" \
    --forwarding_preference=0 \
    --last_group=1 \
    --objects_per_group=3 \
    --size_of_object_zero=4096 \
    --size_of_object_greater_than_zero=4096
fi

# Test 24: Unequal object sizes
run_test "Very different object sizes" \
    "subscribe" \
    --forwarding_preference=1 \
    --last_group=1 \
    --objects_per_group=5 \
    --size_of_object_zero=10000 \
    --size_of_object_greater_than_zero=50


# ============================================================================
# SECTION 4: Increments (Tests 25-30)
# ============================================================================
echo -e "\n${YELLOW}=== SECTION 4: Group and Object Increments ===${NC}"
set_section "4" "Group and Object Increments"

# Test 25: Group increment of 2
run_test "Group increment of 2" \
    "subscribe" \
    --forwarding_preference=0 \
    --start_group=0 \
    --last_group=4 \
    --group_increment=2 \
    --objects_per_group=3

# Test 26: Group increment of 5
run_test "Group increment of 5" \
    "subscribe" \
    --forwarding_preference=0 \
    --start_group=10 \
    --last_group=20 \
    --group_increment=5 \
    --objects_per_group=3

# Test 27: Object increment of 2
run_test "Object increment of 2" \
    "subscribe" \
    --forwarding_preference=1 \
    --last_group=1 \
    --objects_per_group=8 \
    --object_increment=2

# Test 28: Object increment of 3
run_test "Object increment of 3" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=1 \
    --objects_per_group=9 \
    --object_increment=3

# Test 29: Both increments (group=2, object=2)
run_test "Group and object increment of 2" \
    "subscribe" \
    --forwarding_preference=2 \
    --start_group=0 \
    --last_group=4 \
    --group_increment=2 \
    --objects_per_group=6 \
    --object_increment=2

# Test 30: Large increments
run_test "Large increments (group=10, object=5)" \
    "subscribe" \
    --forwarding_preference=0 \
    --start_group=0 \
    --last_group=20 \
    --group_increment=10 \
    --objects_per_group=15 \
    --object_increment=5

# ============================================================================
# SECTION 5: End of Group Markers (Tests 31-37)
# ============================================================================
echo -e "\n${YELLOW}=== SECTION 5: End of Group Markers ===${NC}"
set_section "5" "End of Group Markers"

# Test 31: End of group markers - basic
run_test "End of group markers - basic" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=2 \
    --objects_per_group=5 \
    --send_end_of_group_markers=true

# Test 32: End of group markers with ONE_SUBGROUP_PER_OBJECT
run_test "End of group markers with ONE_SUBGROUP_PER_OBJECT" \
    "subscribe" \
    --forwarding_preference=1 \
    --last_group=2 \
    --objects_per_group=4 \
    --send_end_of_group_markers=true

# Test 33: End of group markers with TWO_SUBGROUPS
# As in test 21, starting at object 3 forces an explicit subgroup ID on
# subgroup 1.  Here the group's last object is 7, so subgroup 1 is also the
# one that signals end of group.
run_test "End of group markers with TWO_SUBGROUPS_PER_GROUP" \
    "subscribe" \
    --forwarding_preference=2 \
    --start_object=3 \
    --last_group=2 \
    --objects_per_group=6 \
    --send_end_of_group_markers=true

# Test 34: FETCH with end of group markers
if [ "$SKIP_FETCH" -ne 1 ]; then
run_test "FETCH with end of group markers" \
    "fetch" \
    --forwarding_preference=0 \
    --last_group=2 \
    --objects_per_group=3 \
    --send_end_of_group_markers=true
fi


# Test 35: End of group markers with object increment
run_test "End of group markers with object increment" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=1 \
    --objects_per_group=8 \
    --object_increment=2 \
    --send_end_of_group_markers=true

# Test 36: End of group markers with single object per group
run_test "End of group markers with single object" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=3 \
    --objects_per_group=1 \
    --send_end_of_group_markers=true

# Test 37: End of group markers with DATAGRAM.  A datagram type byte carries
# the end-of-group bit or an object status, so the marker arrives as a status
# datagram with the bit clear -- and carries no extensions, unlike the payload
# datagrams around it, which is what puts it on the wire differently.
run_test "End of group markers with DATAGRAM and extensions" \
    "subscribe" \
    --forwarding_preference=3 \
    --last_group=2 \
    --objects_per_group=4 \
    --test_integer_extension=1 \
    --test_variable_extension=1 \
    --send_end_of_group_markers=true

# ============================================================================
# SECTION 6: Extensions (Tests 38-43)
# ============================================================================
echo -e "\n${YELLOW}=== SECTION 6: Extensions ===${NC}"
set_section "6" "Extensions"

# Test 37: Integer extension only
run_test "Integer extension (ID=2)" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=1 \
    --objects_per_group=5 \
    --test_integer_extension=1

# Test 38: Variable extension only, on single-object datagram groups
run_test "Variable extension (ID=3)" \
    "subscribe" \
    --forwarding_preference=3 \
    --last_group=5 \
    --objects_per_group=1 \
    --last_object_in_track=0 \
    --test_variable_extension=1

# Test 39: Both extensions
# Subgroup 1 starts at object 3, so its ID is written explicitly; the group's
# last object is 6, which leaves the end of group signal on subgroup 0.
run_test "Both integer and variable extensions" \
    "subscribe" \
    --forwarding_preference=2 \
    --start_object=3 \
    --last_group=1 \
    --objects_per_group=6 \
    --test_integer_extension=1 \
    --test_variable_extension=1

# Test 40: Extensions with different IDs
run_test "Extensions with higher IDs" \
    "subscribe" \
    --forwarding_preference=1 \
    --last_group=1 \
    --objects_per_group=4 \
    --test_integer_extension=5 \
    --test_variable_extension=3

# Test 41: Extensions with FETCH
if [ "$SKIP_FETCH" -ne 1 ]; then
run_test "FETCH with extensions" \
    "fetch" \
    --forwarding_preference=0 \
    --last_group=1 \
    --objects_per_group=3 \
    --test_integer_extension=2 \
    --test_variable_extension=2
fi


# Test 42: Extensions with end of group markers
run_test "Extensions with end of group markers" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=2 \
    --objects_per_group=4 \
    --test_integer_extension=1 \
    --test_variable_extension=1 \
    --send_end_of_group_markers=true

# ============================================================================
# SECTION 7: Complex Scenarios (Tests 44-51)
# ============================================================================
echo -e "\n${YELLOW}=== SECTION 7: Complex Scenarios ===${NC}"
set_section "7" "Complex Scenarios"

# Test 43: High frequency updates
run_test "High frequency updates (100ms)" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=2 \
    --objects_per_group=5 \
    --object_frequency=100

# Test 44: Low frequency updates
run_test "Low frequency updates (2000ms)" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=1 \
    --objects_per_group=3 \
    --object_frequency=2000

# Test 45: Everything combined
# An odd start object with an increment of 2 means every object lands in
# subgroup 1, which starts at object 3 and so carries an explicit subgroup ID,
# extensions and the end of group signal all at once.  last_object_in_track
# has to be named explicitly: the default is object_increment *
# (objects_per_group + 1) = 18, which an increment of 2 never lands on from an
# odd start, so no object would ever be the group's last.  The group increment
# is odd so the groups alternate parity, and with them the publisher priority,
# which an even increment would pin to one value for the whole track.
# last_group has the same constraint as last_object_in_track: the increment has
# to divide the range, or the track ends on a group the subscriber isn't
# waiting for.
run_test "Complex: All features combined" \
    "subscribe" \
    --forwarding_preference=2 \
    --start_group=10 \
    --start_object=3 \
    --last_group=13 \
    --group_increment=3 \
    --objects_per_group=8 \
    --object_increment=2 \
    --last_object_in_track=17 \
    --size_of_object_zero=2048 \
    --size_of_object_greater_than_zero=512 \
    --send_end_of_group_markers=true \
    --test_integer_extension=1 \
    --test_variable_extension=1

# Test 46: Large scale test
run_test "Large scale: Many groups and objects" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=10 \
    --objects_per_group=15 \
    --object_frequency=100

# Test 47: Sparse groups (large increment)
run_test "Sparse groups with large increment" \
    "subscribe" \
    --forwarding_preference=0 \
    --start_group=100 \
    --last_group=500 \
    --group_increment=100 \
    --objects_per_group=3

# Test 48: Delivery timeout test
run_test "Delivery timeout (500ms)" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=2 \
    --objects_per_group=5 \
    --delivery_timeout=500

# Test 49: Publisher delivery timeout
run_test "Publisher delivery timeout (1000ms)" \
    "subscribe" \
    --forwarding_preference=0 \
    --last_group=2 \
    --objects_per_group=5 \
    --publisher_delivery_timeout=1000

# Test 50: Stress test - DATAGRAM with small payload
# Test 4 covers datagrams without extensions; this one carries them.
run_test "Stress: DATAGRAM rapid delivery" \
    "subscribe" \
    --forwarding_preference=3 \
    --last_group=5 \
    --objects_per_group=10 \
    --size_of_object_zero=64 \
    --size_of_object_greater_than_zero=32 \
    --object_frequency=50 \
    --test_integer_extension=1 \
    --test_variable_extension=1

# ============================================================================
# SECTION 8: PUBLISH (Tests 52-57)
# ============================================================================
# These ask the relay for the track with SUBSCRIBE_TRACKS and PUBLISH it on a
# second session, so they exercise the relay's PUBLISH forwarding rather than
# its SUBSCRIBE path. Object generation is shared with the subscribe cases
# above, so this is a spot check of each forwarding preference rather than a
# rerun of every parameter combination.
echo -e "\n${YELLOW}=== SECTION 8: PUBLISH ===${NC}"
set_section "8" "PUBLISH"

# Test 51: ONE_SUBGROUP_PER_GROUP via PUBLISH
run_test "PUBLISH with ONE_SUBGROUP_PER_GROUP" \
    "publish" \
    --forwarding_preference=0 \
    --last_group=2 \
    --objects_per_group=5

# Test 52: ONE_SUBGROUP_PER_OBJECT via PUBLISH
run_test "PUBLISH with ONE_SUBGROUP_PER_OBJECT" \
    "publish" \
    --forwarding_preference=1 \
    --last_group=2 \
    --objects_per_group=5

# Test 53: TWO_SUBGROUPS_PER_GROUP via PUBLISH
run_test "PUBLISH with TWO_SUBGROUPS_PER_GROUP" \
    "publish" \
    --forwarding_preference=2 \
    --last_group=2 \
    --objects_per_group=6

# Test 54: DATAGRAM via PUBLISH
run_test "PUBLISH with DATAGRAM" \
    "publish" \
    --forwarding_preference=3 \
    --last_group=2 \
    --objects_per_group=5 \
    --size_of_object_zero=100 \
    --size_of_object_greater_than_zero=50

# Test 55: End of group markers via PUBLISH
run_test "PUBLISH with end of group markers" \
    "publish" \
    --forwarding_preference=0 \
    --last_group=2 \
    --objects_per_group=4 \
    --send_end_of_group_markers=true

# Test 56: Extensions via PUBLISH
run_test "PUBLISH with both extensions" \
    "publish" \
    --forwarding_preference=0 \
    --last_group=2 \
    --objects_per_group=4 \
    --test_integer_extension=1 \
    --test_variable_extension=1

# Tests 57-58: PUBLISH before SUBSCRIBE_TRACKS. The relay answers PUBLISH_OK
# with forward=0 and only asks for data once the SUBSCRIBE_TRACKS arrives, so
# these cover its backfill path rather than its PUBLISH fan-out.
run_test "PUBLISH before SUBSCRIBE_TRACKS" \
    "publish" \
    --publish_order=publish_first \
    --forwarding_preference=0 \
    --last_group=2 \
    --objects_per_group=5

run_test "PUBLISH before SUBSCRIBE_TRACKS with DATAGRAM" \
    "publish" \
    --publish_order=publish_first \
    --forwarding_preference=3 \
    --last_group=2 \
    --objects_per_group=5 \
    --size_of_object_zero=100 \
    --size_of_object_greater_than_zero=50

# ============================================================================
echo -e "\n${YELLOW}=== SECTION 9: FETCH Ranges ===${NC}"
set_section "9" "FETCH Ranges"
# ============================================================================
# The range is carried in the FETCH, not the track namespace, so every test in
# a group below addresses the same track.  A relay that caches will serve the
# overlapping part of each later fetch out of cache, and the objects delivered
# have to be identical either way.  Groups A-E are 0-4; the fetches walk
# C, then B-C, then C-D, then A-E so each one overlaps what the last cached.

# Every test here is a FETCH, so the whole section goes behind the guard.
if [ "$SKIP_FETCH" -ne 1 ]; then

# Objects 0-9 per group, so group N is [N,0 - N,10).
FETCH_TRACK=(--forwarding_preference=0 --last_group=4 --objects_per_group=10 \
    --last_object_in_track=9)

run_test "FETCH range C" "fetch" \
    "${FETCH_TRACK[@]}" --start_location=2,0 --end_location=2,10

run_test "FETCH range B-C, C cached" "fetch" \
    "${FETCH_TRACK[@]}" --start_location=1,0 --end_location=2,10

run_test "FETCH range C-D, C cached" "fetch" \
    "${FETCH_TRACK[@]}" --start_location=2,0 --end_location=3,10

run_test "FETCH range A-E, B-D cached" "fetch" \
    "${FETCH_TRACK[@]}" --start_location=0,0 --end_location=4,10

# Same walk against a track that ends each group with a marker, so group N is
# [N,0 - N,11) with the marker at object 10.
FETCH_TRACK_EOG=(--forwarding_preference=0 --last_group=4 \
    --objects_per_group=10 --last_object_in_track=10 \
    --send_end_of_group_markers=true)

run_test "FETCH range C, end of group markers" "fetch" \
    "${FETCH_TRACK_EOG[@]}" --start_location=2,0 --end_location=2,11

run_test "FETCH range B-C cached, end of group markers" "fetch" \
    "${FETCH_TRACK_EOG[@]}" --start_location=1,0 --end_location=2,11

run_test "FETCH range C-D cached, end of group markers" "fetch" \
    "${FETCH_TRACK_EOG[@]}" --start_location=2,0 --end_location=3,11

run_test "FETCH range A-E cached, end of group markers" "fetch" \
    "${FETCH_TRACK_EOG[@]}" --start_location=0,0 --end_location=4,11

fi

# ============================================================================
echo -e "\n${YELLOW}=== SECTION 10: Joining FETCH ===${NC}"
set_section "10" "Joining FETCH"
# ============================================================================
# A joining FETCH backfills what ran before the subscription started, so the
# two together cover the track.  Every test but the last runs against a track
# a background subscriber is already holding open.

# 16 groups of 6 objects at 50ms is a little under five seconds of track, so
# it is still running when the join lands a second and a half in.
JOIN_TRACK=(--last_group=15 --objects_per_group=5 --object_frequency=50)
JOIN_DELAY=1.5

run_join_test "Join from the start of the track" 0 backfill \
    "${JOIN_TRACK[@]}"

run_join_test "Join two groups back" -2 backfill \
    "${JOIN_TRACK[@]}"

run_join_test "Join from an absolute group partway in" 3 backfill \
    "${JOIN_TRACK[@]}"

run_join_test "Join with ONE_SUBGROUP_PER_OBJECT" 0 backfill \
    "${JOIN_TRACK[@]}" --forwarding_preference=1

run_join_test "Join with TWO_SUBGROUPS_PER_GROUP" 0 backfill \
    "${JOIN_TRACK[@]}" --forwarding_preference=2

# The halves disagree on shape here: the backfill arrives as one subgroup per
# group while the subscription arrives as datagrams.
run_join_test "Join with DATAGRAM" 0 backfill \
    "${JOIN_TRACK[@]}" --forwarding_preference=3

run_join_test "Join with end of group markers" 0 backfill \
    "${JOIN_TRACK[@]}" --send_end_of_group_markers=true

run_join_test "Join with both extensions" 0 backfill \
    "${JOIN_TRACK[@]}" --test_integer_extension=1 --test_variable_extension=1

# No holder, so the track starts with this subscription and has published
# nothing to anchor a join on.
run_join_test "Join a track that has not started" 0 empty \
    --last_group=4

# ============================================================================
# Generate Report
# ============================================================================
echo -e "\n${YELLOW}========================================${NC}"
echo -e "${YELLOW}  Conformance Test Results${NC}"
echo -e "${YELLOW}========================================${NC}"
echo "End Time: $(date)"
echo ""
echo "Total Tests: $TOTAL_TESTS"
echo -e "${GREEN}Passed: $PASSED_TESTS${NC}"
echo -e "${RED}Failed: $FAILED_TESTS${NC}"
echo ""

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}✓ ALL TESTS PASSED!${NC}"
    SUCCESS_RATE=100
else
    if [ "$TOTAL_TESTS" -gt 0 ]; then
        SUCCESS_RATE=$(awk "BEGIN {printf \"%.1f\", ($PASSED_TESTS/$TOTAL_TESTS)*100}")
        echo -e "${YELLOW}Success Rate: ${SUCCESS_RATE}%${NC}"
    else
        SUCCESS_RATE=0
        echo -e "${YELLOW}No tests run. Success Rate: N/A${NC}"
    fi
fi


# Section summaries
echo -e "\n${BLUE}Results by Test Section:${NC}"
echo "----------------------------------------"
for section in "${!SECTION_TOTAL[@]}"; do
    section_name=${SECTION_NAMES[$section]}
    total=${SECTION_TOTAL[$section]}
    passed=${SECTION_PASSED[$section]}
    failed=${SECTION_FAILED[$section]}
    section_rate=$(awk "BEGIN {printf \"%.1f\", ($passed/$total)*100}")

    if [ "$failed" -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $section ($section_name): $passed/$total (${section_rate}%)"
    else
        echo -e "${YELLOW}◐${NC} $section ($section_name): $passed/$total (${section_rate}%) - $failed failed"
    fi

done | sort


# Detailed results
echo -e "\n${BLUE}Detailed Results:${NC}"
echo "----------------------------------------"
for result in "${TEST_RESULTS[@]}"; do
    IFS='|' read -r status name section reason <<< "$result"
    if [ "$status" = "PASS" ]; then
        echo -e "${GREEN}✓${NC} $name"
    else
        echo -e "${RED}✗${NC} $name"
        if [ -n "$reason" ]; then
            echo -e "    Reason: $reason"
        fi
    fi
done

# Save report to file
REPORT_FILE="moqtest_conformance_report_$(date +%Y%m%d_%H%M%S).txt"
{
    echo "MoQTest Conformance Report"
    echo "=========================="
    echo "Date: $(date)"
    echo "Relay URL: $RELAY_URL"
    echo ""
    echo "Summary:"
    echo "--------"
    echo "Total Tests: $TOTAL_TESTS"
    echo "Passed: $PASSED_TESTS"
    echo "Failed: $FAILED_TESTS"
    echo "Success Rate: ${SUCCESS_RATE}%"
    echo ""
    echo "Results by Test Section:"
    echo "------------------------"
    for section in "${!SECTION_TOTAL[@]}"; do
        total=${SECTION_TOTAL[$section]}
        passed=${SECTION_PASSED[$section]}
        failed=${SECTION_FAILED[$section]}
        section_rate=$(awk "BEGIN {printf \"%.1f\", ($passed/$total)*100}")

        if [ "$failed" -eq 0 ]; then
            echo "✓ $section: $passed/$total (${section_rate}%)"
        else
            echo "◐ $section: $passed/$total (${section_rate}%) - $failed failed"
        fi
    done | sort
    echo ""
    echo "Detailed Results:"
    echo "-----------------"
    for result in "${TEST_RESULTS[@]}"; do
        IFS='|' read -r status name section reason <<< "$result"
        if [ "$status" = "PASS" ]; then
            echo "✓ PASS: $name"
        else
            echo "✗ FAIL: $name"
            if [ -n "$reason" ]; then
                echo "    Reason: $reason"
            fi
        fi
    done
} > "$REPORT_FILE"

echo ""
echo -e "${BLUE}Report saved to: $REPORT_FILE${NC}"

# Exit with appropriate code
if [ $FAILED_TESTS -eq 0 ]; then
    exit 0
else
    exit 1
fi
