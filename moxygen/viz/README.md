# MoQ Transport Visualization Tool

A web-based interactive timeline visualization tool for analyzing MoQ (Media over QUIC) Transport data transfers using QLOG event files.

## Overview

This tool visualizes MoQ Transport sessions by parsing QLOG JSON files containing MoQ events and displaying them as an interactive timeline. It provides insights into data flow patterns, timing relationships, and message exchanges between client and server endpoints.

## Features

### Timeline Visualization
- **Two-column layout**: Client messages (left) and server messages (right)
- **Track-based organization**: Events grouped by track (video, audio, metadata, etc.)
- **Time-based positioning**: Events positioned chronologically with precise timestamps

### Event Types Supported
- **Control Messages**: SETUP, PUBLISH_NAMESPACE, SUBSCRIBE, UNSUBSCRIBE, etc.
- **Object Datagrams**: Data object transfers with size information
- **Subgroup Operations**: Subgroup headers and object transfers
- **Fetch Operations**: Fetch requests and responses
- **Stream Settings**: Stream type configurations

### Interactive Features
- **Pan & Zoom**: Mouse wheel zoom, drag to pan timeline
- **Event Filtering**: Show/hide specific event types
- **Hover Details**: Detailed tooltips with event information
- **Click Details**: Persistent detail panel with full event data
- **Size Visualization**: Bar height proportional to object size

### Visual Encoding
- **Color Coding**:
  - Blue: Control messages
  - Green: Object datagrams
  - Orange: Subgroup operations
  - Purple: Fetch operations
  - Gray: Stream settings
- **Track Colors**: Each track has a unique border color
- **Object IDs**: Optional display of group/subgroup/object identifiers

## Getting Started

### File Structure
```
viz/
├── html/
│   ├── index.html          # Main visualization interface
│   ├── moq-timeline.js     # Timeline visualization logic
│   ├── moq-parser.js       # QLOG parsing logic
│   └── style.css           # Styling and layout
└── examples/
    ├── sample_moq.qlog     # Sample MoQ QLOG file
    └── README.md           # This documentation
```

### Usage Instructions

1. **Open the visualization tool**:
   ```bash
   # Navigate to the viz directory
   cd fbcode/ti/experimental/moxygen/viz/html

   # Open index.html in a web browser
   open index.html
   # or
   python3 -m http.server 8000  # Serve locally
   ```

2. **Load QLOG data**:
   - Click "Upload MoQ QLOG JSON file" and select your `.qlog` file
   - Or click "Load Example Data" to see sample visualization

3. **Interact with the timeline**:
   - **Zoom**: Mouse wheel over timeline
   - **Pan**: Click and drag timeline
   - **Reset**: Double-click outside events or use "Reset View" button
   - **Filter**: Use checkboxes to show/hide event types
   - **Details**: Hover over events for tooltips, click for persistent details

### QLOG File Format

The tool expects QLOG JSON files conforming to the MoQ QLOG Events specification:

```json
{
  "qlog_format": "JSON-SEQ",
  "qlog_version": "0.3",
  "title": "MoQ Transport Session",
  "traces": [{
    "title": "MoQ Trace",
    "vantage_point": { "type": "client" },
    "events": [
      [timestamp, "transport", "moqt:event_name", { event_data }],
      ...
    ]
  }]
}
```

### Supported MoQ Events

Based on the MoQ Transport specification and MLogEvents implementation:

| Event Name | Description | Visualization |
|------------|-------------|---------------|
| `moqt:control_message_created` | Control message sent | Blue bar (client side) |
| `moqt:control_message_parsed` | Control message received | Blue bar (server side) |
| `moqt:object_datagram_created` | Object datagram sent | Green bar with size |
| `moqt:object_datagram_parsed` | Object datagram received | Green bar with size |
| `moqt:subgroup_header_created` | Subgroup header sent | Orange bar |
| `moqt:subgroup_header_parsed` | Subgroup header received | Orange bar |
| `moqt:subgroup_object_created` | Subgroup object sent | Orange bar with size |
| `moqt:subgroup_object_parsed` | Subgroup object received | Orange bar with size |
| `moqt:fetch_header_created` | Fetch request sent | Purple bar |
| `moqt:fetch_header_parsed` | Fetch request received | Purple bar |
| `moqt:fetch_object_created` | Fetch response sent | Purple bar with size |
| `moqt:fetch_object_parsed` | Fetch response received | Purple bar with size |
| `moqt:stream_type_set` | Stream type configured | Gray marker |

## Event Data Fields

The parser extracts and displays the following fields from event data:

### Common Fields
- `vantage_point`: Client or server perspective
- `timestamp`: Event timing information
- `track_id/track_name`: Track identifier
- `track_alias`: Numeric track alias

### Object-specific Fields
- `group_id`: Group identifier
- `subgroup_id`: Subgroup identifier
- `object_id`: Object identifier
- `object_size`: Object size in bytes
- `payload_length`: Payload size

### Control-specific Fields
- `message_type`: Control message type (SETUP, SUBSCRIBE, etc.)
- `subscribe_id`: Subscription identifier
- `track_namespace`: Track namespace
- `publisher_priority`: Object priority

## Implementation Details

### Architecture
- **MoQParser**: Parses QLOG JSON and extracts MoQ events
- **MoQTimeline**: Handles visualization, interaction, and UI
- **Event Processing**: Transforms raw events into timeline data
- **Interactive Controls**: Pan, zoom, filter, and detail viewing

### Performance Considerations
- Efficiently handles large datasets with virtual rendering
- Smooth animations using CSS transforms
- Optimized DOM manipulation for interactive updates

### Browser Compatibility
- Modern browsers with ES6+ support
- Uses jQuery for DOM manipulation
- CSS3 features for styling and animations

## Example Use Cases

### Video Conference Analysis
Visualize MoQ events for a video conference session:
- Track setup and subscription handshakes
- Video and audio data object flows
- Timing relationships between tracks
- Object sizes and transfer patterns

### Live Streaming Debug
Debug live streaming issues:
- Control message timing
- Object delivery patterns
- Subgroup organization
- Fetch operation efficiency

### Protocol Development
Analyze protocol behavior:
- Message exchange patterns
- Timing characteristics
- Event ordering validation
- Performance bottleneck identification

## Customization

### Adding New Event Types
To support additional MoQ events:

1. Update `eventTypes` in `moq-parser.js`
2. Add categorization logic in `categorizeEvent()`
3. Define visual styling in `style.css`
4. Update legend and documentation

### Visual Customization
- Modify color schemes in `style.css`
- Adjust sizing algorithms in `renderEventBar()`
- Customize tooltip content in `generateTooltipContent()`

## Troubleshooting

### Common Issues

**File loading errors**:
- Ensure QLOG file is valid JSON
- Check that traces array contains events
- Verify event format: `[time, category, name, data]`

**No events displayed**:
- Check that events match MoQ event names
- Verify vantage_point detection logic
- Ensure timestamps are valid numbers

**Performance issues**:
- Large files (>1000 events) may be slow
- Consider filtering events before visualization
- Use browser dev tools to identify bottlenecks

### Debug Mode
Enable console logging for detailed parsing information:
```javascript
// In browser console
window.moqTimeline.parser.debug = true;
```

## Integration with MoQ Infrastructure

This tool integrates with the broader MoQ ecosystem:

- **MLog System**: Consumes MLog JSON exports from MoQ sessions
- **QLOG Events**: Follows MoQ QLOG Events specification
- **MoQ Transport**: Visualizes events from moxygen C++ implementation
- **Development Workflow**: Supports debugging and analysis workflows

## Future Enhancements

Potential improvements and extensions:

- **Real-time visualization**: Live event streaming
- **Statistical analysis**: Bandwidth, latency, throughput metrics
- **Comparison mode**: Side-by-side session comparison
- **Export capabilities**: PDF, PNG, CSV export options
- **Advanced filtering**: Complex query-based filtering
- **Performance metrics**: Built-in analysis calculations

## Related Documentation

- [MoQ Transport Specification](https://moq-wg.github.io/moq-transport/draft-ietf-moq-transport.html)
- [MoQ QLOG Events](https://www.ietf.org/archive/id/draft-pardue-moq-qlog-moq-events-00.html)
- [QLOG Specification](https://www.rfc-editor.org/rfc/rfc9473.html)
- [Moxygen Implementation](../README.md)

## Contributing

To contribute improvements:

1. Test with various QLOG files
2. Follow existing code patterns
3. Update documentation for new features
4. Ensure cross-browser compatibility
5. Consider performance implications

---

For questions or issues, consult the MoQ Transport team or file issues in the appropriate repository.
