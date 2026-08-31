# MoQTest Conformance Test Suite

## Overview

The conformance test suite exercises the MoQTest client against a relay server to validate protocol compliance. It tests 56 different scenarios covering a wide range of MoQT functionality.

Sections 1-7 pull tracks from an upstream `moqtest_server` with SUBSCRIBE and FETCH, so a server must be attached to the relay. Section 8 instead has the client PUBLISH the track itself on a second session, so it needs only the relay.

## Usage

### Running the Tests

```bash
MOXYGEN_DIR=`./build/fbcode_builder/getdeps.py show-build-dir moxygen` ./conformance_test.sh http://localhost:9999 [Q]
```

The script will:
- Build the test client automatically
- Run all 56 test cases
- Display progress with color-coded results
- Generate a timestamped report file
- Exit with code 0 if all tests pass, 1 if any fail

### Output

The test suite provides:

1. **Real-time output**: Color-coded test results as they run
   - Green ✓ for passed tests
   - Red ✗ for failed tests
   - Blue for section headers and info

2. **Summary statistics**:
   - Total tests run
   - Pass/fail counts
   - Overall success rate
   - **Per-section breakdown** showing results for each test category

3. **Detailed results**: Complete list of all test outcomes with failure reasons

4. **Report file**: Timestamped text file (e.g., `moqtest_conformance_report_20241101_123456.txt`)

## Test Coverage

The 56 test cases are organized into 8 sections:

### Section 1: Basic Forwarding Preferences (8 tests)
Tests all four forwarding preferences with both subscribe and fetch:
- ONE_SUBGROUP_PER_GROUP (0)
- ONE_SUBGROUP_PER_OBJECT (1)
- TWO_SUBGROUPS_PER_GROUP (2)
- DATAGRAM (3)

### Section 2: Object and Group Counts (8 tests)
Tests various combinations of groups and objects:
- Single/multiple objects per group
- Single/multiple groups
- Custom start positions
- Partial group delivery
- Range fetching

### Section 3: Object Sizes (8 tests)
Tests different object sizes:
- Tiny objects (10 bytes)
- Large objects (up to 10KB)
- Mixed sizes
- Single byte objects
- Unequal object sizes

### Section 4: Group and Object Increments (6 tests)
Tests non-sequential numbering:
- Group increments (2, 5, 10)
- Object increments (2, 3, 5)
- Combined increments
- Sparse group distributions

### Section 5: End of Group Markers (6 tests)
Tests the optional end-of-group marker feature:
- Basic end-of-group markers
- With different forwarding preferences
- With fetch requests
- With object increments

### Section 6: Extensions (6 tests)
Tests MoQT extensions:
- Integer extensions
- Variable extensions
- Both extensions combined
- Different extension IDs
- Extensions on subgroups and on datagrams
- Extensions with other features

### Section 7: Complex Scenarios (8 tests)
Tests challenging combinations:
- High/low frequency updates
- All features combined
- Large scale tests
- Delivery timeouts
- Stress testing

### Section 8: PUBLISH (6 tests)
Tests the relay forwarding a PUBLISH rather than answering a SUBSCRIBE. The
client sends SUBSCRIBE_TRACKS, then opens a second session to the same relay
endpoint and PUBLISHes the requested track, so the relay has to match the two
and forward the objects back:
- Each of the four forwarding preferences
- End of group markers
- Integer and variable extensions

Object generation is shared with the subscribe path, so these are a spot check
of the PUBLISH plumbing rather than a rerun of every parameter combination.
These tests need only a relay; no upstream `moqtest_server` is involved.

## MoQTest Protocol Parameters

The test suite exercises all 16 tuple fields of the moq-test-00 protocol:

| Field | Parameter | Description |
|-------|-----------|-------------|
| 0 | Protocol Version | "moq-test-00" |
| 1 | Forwarding Preference | 0-3 (subgroup/object/two-subgroups/datagram) |
| 2 | Start Group | Starting group number |
| 3 | Start Object | Starting object number per group |
| 4 | Last Group in Track | Final group number |
| 5 | Last Object in Track | Final object number per group |
| 6 | Objects per Group | Number of objects in each group |
| 7 | Size of Object 0 | Size in bytes of first object |
| 8 | Size of Objects > 0 | Size in bytes of other objects |
| 9 | Object Frequency | Milliseconds between objects |
| 10 | Group Increment | Step between group numbers |
| 11 | Object Increment | Step between object numbers |
| 12 | Send End of Group Markers | Boolean flag |
| 13 | Test Integer Extension | Extension ID (or -1 for none) |
| 14 | Test Variable Extension | Extension ID (or -1 for none) |
| 15 | Publisher Delivery Timeout | Timeout in milliseconds |

## Known issues

FETCH tests are currently skipped

## Join Support

The test suite covers:
- `subscribe` - Live subscription to a track
- `fetch` - Retrieve historical data from a track

A joining FETCH combines the two: it backfills the part of the track that ran
before the subscription started, so a client that arrives mid-track still sees
the whole thing. The server serves joining FETCHes; the client does not send
one yet, so there are no join tests in the suite.

## Troubleshooting

### Test Failures

1. **Connection refused**: Ensure relay server is running
2. **Timeout errors**: Server may be overloaded or network issues
3. **Validation failures**: Check server implements moq-test-00 correctly

### Debugging

Add verbose logging to the client:
```bash
# Edit MoQTestClientMain.cpp to increase XLOG level
# Rebuild and run individual tests manually:
`./build/fbcode_builder/getdeps.py show-build-dir moxygen`/moxygen/moqtest:moqtest_client -- \
  --url=http://localhost:9999 \
  --request=subscribe \
  --forwarding_preference=0 \
  --last_group=2 \
  --objects_per_group=5
```

## Contributing

When adding new test cases:
1. Add them to the appropriate section (or create a new section)
2. Use descriptive test names
3. Update this README with new coverage
4. Ensure tests are deterministic and fast
5. Add `set_section()` calls for new sections
