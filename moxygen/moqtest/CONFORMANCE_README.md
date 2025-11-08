# MoQTest Conformance Test Suite

## Overview

The conformance test suite exercises the MoQTest client against a relay server to validate protocol compliance. It tests 50 different scenarios covering a wide range of MoQT functionality.

## Usage

### Running the Tests

```bash
MOXYGEN_DIR=`./build/fbcode_builder/getdeps.py show-build-dir moxygen` ./conformance_test.sh http://localhost:9999 [Q]
```

The script will:
- Build the test client automatically
- Run all 50 test cases
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

The 50 test cases are organized into 7 sections:

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
- Extensions with other features

### Section 7: Complex Scenarios (8 tests)
Tests challenging combinations:
- High/low frequency updates
- All features combined
- Large scale tests
- Delivery timeouts
- Stress testing

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

## Future Enhancements: Join Support

### Planned Feature

Currently the test suite covers:
- `subscribe` - Live subscription to a track
- `fetch` - Retrieve historical data from a track

**Planned addition**: Support for `join` - Combines fetch + subscribe to catch up on historical data while subscribing to live updates.

### Join Implementation Plan

See detailed plan in the memory file, but key changes include:

1. **MoQTestClient.h** - Add join() method and separate receivers
2. **MoQTestClient.cpp** - Implement join() using MoQSession::join()
3. **MoQTestClientMain.cpp** - Add --request=join and joining_start parameter
4. **conformance_test.sh** - Add Section 8 with join-specific tests

### Join Test Scenarios (Proposed)

Section 8 would add 10-15 tests covering:
- Basic join (fetch historical + subscribe live)
- Join with different forwarding preferences
- Join at various start points (early, middle, recent)
- Join with end of group markers
- Join with extensions
- Join with large historical data sets
- Validation of proper boundary handling (no gaps/duplicates)

### Why Join is Important

The join operation is critical for:
- **Late joiners**: Clients connecting mid-stream who need context
- **Resilience**: Recovering from brief disconnections without data loss
- **Efficient catchup**: Getting historical data while staying live
- **Real-world scenarios**: Most production uses need both past and present data

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
