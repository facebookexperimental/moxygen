/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/**
 * MoQ QLOG Parser
 * Parses MoQ (Media over QUIC) Transport QLOG JSON files
 * Based on MoQ QLOG Events specification and MLogEvents structure
 */

class MoQParser {
    constructor() {
        // MoQ Event Types from MLogEvents.h
        this.eventTypes = {
            CONTROL_MESSAGE_CREATED: 'moqt:control_message_created',
            CONTROL_MESSAGE_PARSED: 'moqt:control_message_parsed',
            OBJECT_DATAGRAM_CREATED: 'moqt:object_datagram_created',
            OBJECT_DATAGRAM_PARSED: 'moqt:object_datagram_parsed',
            OBJECT_DATAGRAM_STATUS_CREATED: 'moqt:object_datagram_status_created',
            OBJECT_DATAGRAM_STATUS_PARSED: 'moqt:object_datagram_status_parsed',
            SUBGROUP_HEADER_CREATED: 'moqt:subgroup_header_created',
            SUBGROUP_HEADER_PARSED: 'moqt:subgroup_header_parsed',
            SUBGROUP_OBJECT_CREATED: 'moqt:subgroup_object_created',
            SUBGROUP_OBJECT_PARSED: 'moqt:subgroup_object_parsed',
            FETCH_HEADER_CREATED: 'moqt:fetch_header_created',
            FETCH_HEADER_PARSED: 'moqt:fetch_header_parsed',
            FETCH_OBJECT_CREATED: 'moqt:fetch_object_created',
            FETCH_OBJECT_PARSED: 'moqt:fetch_object_parsed',
            STREAM_TYPE_SET: 'moqt:stream_type_set'
        };

        // VantagePoint enum values
        this.vantagePoints = {
            CLIENT: 'client',
            SERVER: 'server'
        };

        // Event category mapping for visualization
        this.eventCategories = {
            CONTROL: 'control',
            OBJECT: 'object',
            SUBGROUP: 'subgroup',
            FETCH: 'fetch',
            STREAM: 'stream'
        };
    }

    /**
     * Parse MoQ log data in serialized NDJSON format.
     * Each line is a JSON object: {vantagePoint, name, time, data}
     * @param {string} input - NDJSON string (newline-separated JSON objects)
     * @returns {Object} Parsed timeline data
     */
    parseQLog(input) {
        try {
            // Parse NDJSON: split by newlines and parse each line
            const rawEvents = this.parseNDJSON(input);

            if (rawEvents.length === 0) {
                throw new Error('No valid MoQ events found in input');
            }

            // Extract metadata from events
            const metadata = this.extractMetadata(rawEvents);

            // Process events into normalized format
            const events = this.processEvents(rawEvents);

            // Group events by tracks
            const tracks = this.groupEventsByTracks(events);

            // Calculate timeline bounds
            const timelineBounds = this.calculateTimelineBounds(events);

            return {
                metadata,
                events,
                tracks,
                timelineBounds,
                totalEvents: events.length,
                eventStats: this.calculateEventStats(events)
            };
        } catch (error) {
            console.error('Error parsing MoQ log data:', error);
            throw error;
        }
    }

    /**
     * Parse NDJSON string into array of event objects
     * @param {string} input - NDJSON string
     * @returns {Array} Array of parsed event objects
     */
    parseNDJSON(input) {
        if (typeof input !== 'string') {
            throw new Error('Expected NDJSON string input');
        }

        const lines = input.split('\n').filter(line => line.trim().length > 0);
        const events = [];

        for (let i = 0; i < lines.length; i++) {
            try {
                const parsed = JSON.parse(lines[i]);
                if (parsed && parsed.name && parsed.time !== undefined) {
                    events.push(parsed);
                }
            } catch (e) {
                console.warn(`Skipping malformed JSON at line ${i + 1}:`, e.message);
            }
        }

        return events;
    }

    /**
     * Extract metadata from parsed event objects
     * @param {Array} events - Array of parsed NDJSON event objects
     * @returns {Object} Metadata object
     */
    extractMetadata(events) {
        // Determine vantage point from first event
        const firstVantage = events.length > 0 ? events[0].vantagePoint : 'client';

        return {
            title: 'MoQ Transport Session',
            description: 'Media over QUIC data transfer visualization',
            vantage_point: { type: firstVantage },
            configuration: {},
            start_time: this.extractStartTime(events)
        };
    }

    /**
     * Extract start time from events
     * @param {Array} events - Array of parsed NDJSON event objects
     * @returns {number} Start time in milliseconds
     */
    extractStartTime(events) {
        if (events.length === 0) return Date.now();

        // Find the earliest timestamp
        let minTime = Number.MAX_SAFE_INTEGER;
        for (const event of events) {
            const timestamp = this.parseEventTime(event);
            if (timestamp < minTime) {
                minTime = timestamp;
            }
        }

        return minTime;
    }

    /**
     * Parse event timestamp from serialized MLog event.
     * The "time" field is nanoseconds as a string.
     * @param {Object} event - Parsed event object with "time" field
     * @returns {number} Timestamp in milliseconds
     */
    parseEventTime(event) {
        if (!event || event.time === undefined) return 0;

        const timeValue = event.time;

        // time is nanoseconds as a string in MLog format
        if (typeof timeValue === 'string') {
            const ns = parseInt(timeValue, 10);
            if (!isNaN(ns)) {
                // Convert nanoseconds to milliseconds
                return ns / 1000000;
            }
        } else if (typeof timeValue === 'number') {
            // If already a number, treat as nanoseconds
            return timeValue / 1000000;
        }

        return 0;
    }

    /**
     * Process serialized MLog events and extract MoQ-specific data.
     * Each rawEvent is an object: {vantagePoint, name, time, data}
     *
     * The "data" field structure depends on event type and matches the
     * toDynamic() output from C++ MLogTypes (see MLogTypes.cpp).
     *
     * @param {Array} rawEvents - Array of parsed NDJSON event objects
     * @returns {Array} Processed MoQ events
     */
    processEvents(rawEvents) {
        const processedEvents = [];

        // Build stream_id -> track_alias mapping from subgroup/fetch headers.
        // Subgroup headers contain both stream_id and track_alias, but
        // subgroup objects only contain streamId. We need the mapping to
        // assign subgroup objects to the correct track.
        const streamToTrackAlias = {};

        // Reconstruct track names for track_alias values.
        // Track-based events often only carry track_alias (or only streamId),
        // so we bridge via control messages:
        //   subscribe (request_id -> track_name/namespace)
        //   subscribe_ok (track_alias -> request_id)
        // Then: track_alias -> track_name.
        const requestIdToTrackInfo = {};
        const trackAliasToRequestId = {};

        for (const rawEvent of rawEvents) {
            const eventName = rawEvent.name;
            const eventData = rawEvent.data;
            if (!eventName || !eventData) continue;

            if (eventName.includes('subgroup_header')) {
                // SubgroupHeader toDynamic uses snake_case: stream_id, track_alias
                const sid = eventData.stream_id;
                const ta = eventData.track_alias;
                if (sid !== undefined && ta !== undefined) {
                    streamToTrackAlias[String(sid)] = ta;
                }
            }

            if (eventName.includes('control_message') && eventData.message) {
                const msg = eventData.message;

                if (msg.type === 'subscribe' && msg.request_id !== undefined) {
                    requestIdToTrackInfo[String(msg.request_id)] = {
                        track_name: msg.track_name,
                        track_namespace: msg.track_namespace,
                    };
                }

                if (msg.type === 'subscribe_ok' && msg.track_alias !== undefined && msg.request_id !== undefined) {
                    trackAliasToRequestId[String(msg.track_alias)] = String(msg.request_id);
                }
            }
        }

        this._streamToTrackAlias = streamToTrackAlias;

        const trackAliasToDisplayName = {};
        const trackAliasToFullName = {};
        Object.keys(trackAliasToRequestId).forEach(trackAlias => {
            const requestId = trackAliasToRequestId[trackAlias];
            const info = requestIdToTrackInfo[requestId];
            if (info && info.track_name) {
                const ns = Array.isArray(info.track_namespace) ? info.track_namespace : [];
                const trackName = String(info.track_name);
                const fullName = ns.length > 0 ? ns.join('/') + '/' + trackName : trackName;
                trackAliasToFullName[trackAlias] = fullName;
                const MAX_DISPLAY_LEN = 50;
                if (fullName.length <= MAX_DISPLAY_LEN || ns.length <= 4) {
                    trackAliasToDisplayName[trackAlias] = fullName;
                } else {
                    // Truncate namespace elements in the middle, keeping
                    // the first 2 and last 2 elements with "..." between.
                    var keepStart = 2;
                    var keepEnd = 2;
                    var truncatedNs = ns.slice(0, keepStart).concat(['...']).concat(ns.slice(ns.length - keepEnd)).join('/');
                    var truncated = truncatedNs + '/' + trackName;
                    if (truncated.length > MAX_DISPLAY_LEN && ns.length > 2) {
                        // Still too long; keep only first 1 and last 1
                        truncatedNs = ns.slice(0, 1).concat(['...']).concat(ns.slice(ns.length - 1)).join('/');
                        trackAliasToDisplayName[trackAlias] = truncatedNs + '/' + trackName;
                    } else {
                        trackAliasToDisplayName[trackAlias] = truncated;
                    }
                }
            }
        });
        this._trackAliasToDisplayName = trackAliasToDisplayName;
        this._trackAliasToFullName = trackAliasToFullName;

        for (let i = 0; i < rawEvents.length; i++) {
            const rawEvent = rawEvents[i];

            const eventName = rawEvent.name;
            const eventData = rawEvent.data || {};
            const vantagePoint = rawEvent.vantagePoint || 'client';

            // Only process MoQ events, skip stream_type_set events
            if (!eventName || !this.isMoQEvent(eventName) || eventName === this.eventTypes.STREAM_TYPE_SET) {
                continue;
            }

            const processedEvent = {
                id: `event_${i}`,
                originalIndex: i,
                timestamp: this.parseEventTime(rawEvent),
                relative_time: 0, // Will be calculated later
                category: 'transport',
                name: eventName,
                data: eventData,
                vantage_point: vantagePoint.toLowerCase(),
                event_category: this.categorizeEvent(eventName, eventData),
                track_id: this.extractTrackId(eventName, eventData),
                group_id: this.extractGroupId(eventName, eventData),
                subgroup_id: this.extractSubgroupId(eventName, eventData),
                object_id: this.extractObjectId(eventName, eventData),
                object_size: this.extractObjectSize(eventName, eventData),
                control_message_type: this.extractControlMessageType(eventName, eventData),
                raw_event: rawEvent
            };

            processedEvents.push(processedEvent);
        }

        // Sort by timestamp first, then by original file order as tiebreaker
        processedEvents.sort((a, b) => {
            if (a.timestamp !== b.timestamp) {
                return a.timestamp - b.timestamp;
            }
            return a.originalIndex - b.originalIndex;
        });

        const startTime = processedEvents.length > 0 ? processedEvents[0].timestamp : 0;
        processedEvents.forEach(event => {
            event.relative_time = event.timestamp - startTime;
        });

        return processedEvents;
    }

    /**
     * Check if event is a MoQ event
     * @param {string} eventName - Event name
     * @returns {boolean} True if MoQ event
     */
    isMoQEvent(eventName) {
        return Object.values(this.eventTypes).includes(eventName);
    }

    /**
     * Categorize event for visualization.
     * Control messages go in control columns; everything else goes in track columns.
     * @param {string} eventName - Event name (e.g. "moqt:control_message_created")
     * @param {Object} eventData - Event data from toDynamic()
     * @returns {string} Event category
     */
    categorizeEvent(eventName, eventData) {
        // All control messages go in the control column
        if (eventName.includes('control_message')) {
            return this.eventCategories.CONTROL;
        }

        if (eventName.includes('object_datagram')) {
            return this.eventCategories.OBJECT;
        } else if (eventName.includes('subgroup')) {
            return this.eventCategories.SUBGROUP;
        } else if (eventName.includes('fetch')) {
            return this.eventCategories.FETCH;
        } else if (eventName.includes('stream_type')) {
            return this.eventCategories.STREAM;
        }

        return 'track_data';
    }

    /**
     * Extract track ID from event data.
     * Uses track_alias as the track identifier.
     *
     * Field locations by event type (from MLogTypes.cpp toDynamic()):
     * - subgroup_header_created/parsed: data.track_alias (number)
     * - object_datagram_created/parsed: data.track_alias (number)
     * - subgroup_object_created/parsed: looked up via stream_id -> track_alias mapping
     * - fetch_object_created/parsed: looked up via stream_id -> track_alias mapping
     * - control_message: null (goes in control column)
     * - stream_type_set: null
     * - fetch_header: null
     *
     * @param {string} eventName - Event name
     * @param {Object} eventData - Event data
     * @returns {string|null} Track ID (track_alias as string)
     */
    extractTrackId(eventName, eventData) {
        if (!eventData) return null;

        // Subgroup headers and object datagrams have track_alias directly
        if (eventData.track_alias !== undefined) {
            return String(eventData.track_alias);
        }

        // Subgroup objects and fetch objects: look up via stream_id mapping
        if (eventName.includes('subgroup_object') || eventName.includes('fetch_object')) {
            // These use camelCase "streamId" as a string
            const streamId = eventData.streamId;
            if (streamId !== undefined && this._streamToTrackAlias) {
                const trackAlias = this._streamToTrackAlias[String(streamId)];
                if (trackAlias !== undefined) {
                    return String(trackAlias);
                }
            }
        }

        return null;
    }

    /**
     * Extract group ID from event data.
     *
     * Field locations (from MLogTypes.cpp toDynamic()):
     * - subgroup_header: data.group_id (number)
     * - subgroup_object: data.groupId (string)
     * - object_datagram: data.group_id (number)
     * - fetch_object: data.groupId (string)
     *
     * @param {string} eventName - Event name
     * @param {Object} eventData - Event data
     * @returns {number|null} Group ID
     */
    extractGroupId(eventName, eventData) {
        if (!eventData) return null;

        // snake_case number field (subgroup_header, object_datagram)
        if (eventData.group_id !== undefined) {
            return Number(eventData.group_id);
        }
        // camelCase string field (subgroup_object, fetch_object)
        if (eventData.groupId !== undefined) {
            return Number(eventData.groupId);
        }

        return null;
    }

    /**
     * Extract subgroup ID from event data.
     *
     * Field locations (from MLogTypes.cpp toDynamic()):
     * - subgroup_header: data.subgroup_id (number, optional)
     * - subgroup_object: data.subgroupId (string, optional)
     * - fetch_object: data.subgroupId (string)
     *
     * @param {string} eventName - Event name
     * @param {Object} eventData - Event data
     * @returns {number|null} Subgroup ID
     */
    extractSubgroupId(eventName, eventData) {
        if (!eventData) return null;

        if (eventData.subgroup_id !== undefined) {
            return Number(eventData.subgroup_id);
        }
        if (eventData.subgroupId !== undefined) {
            return Number(eventData.subgroupId);
        }

        return null;
    }

    /**
     * Extract object ID from event data.
     *
     * Field locations (from MLogTypes.cpp toDynamic()):
     * - subgroup_object: data.objectId (string)
     * - object_datagram: data.object_id (number, optional)
     * - fetch_object: data.objectId (string)
     *
     * @param {string} eventName - Event name
     * @param {Object} eventData - Event data
     * @returns {number|null} Object ID
     */
    extractObjectId(eventName, eventData) {
        if (!eventData) return null;

        if (eventData.object_id !== undefined) {
            return Number(eventData.object_id);
        }
        if (eventData.objectId !== undefined) {
            return Number(eventData.objectId);
        }

        return null;
    }

    /**
     * Extract object size from event data.
     *
     * Field locations (from MLogTypes.cpp toDynamic()):
     * - subgroup_object: data.objectPayloadLength (string)
     * - fetch_object: data.objectPayloadLength (string)
     * - object_datagram: no explicit size field, but has object_payload
     *
     * @param {string} eventName - Event name
     * @param {Object} eventData - Event data
     * @returns {number} Object size in bytes
     */
    extractObjectSize(eventName, eventData) {
        if (!eventData) return 0;

        // subgroup_object and fetch_object use objectPayloadLength (string)
        if (eventData.objectPayloadLength !== undefined) {
            return Number(eventData.objectPayloadLength) || 0;
        }

        // object_datagram: estimate from payload string if present
        if (eventData.object_payload) {
            return eventData.object_payload.length;
        }

        return 0;
    }

    /**
     * Extract control message type from event data.
     *
     * For control messages, the data structure is:
     *   { streamId: "0", message: { type: "subscribe_ok", ... } }
     * The message type is at data.message.type
     *
     * @param {string} eventName - Event name
     * @param {Object} eventData - Event data
     * @returns {string|null} Control message type
     */
    extractControlMessageType(eventName, eventData) {
        if (!eventData) return null;

        // Control messages: type is nested under message.type
        if (eventData.message && eventData.message.type) {
            return eventData.message.type;
        }

        return null;
    }

    /**
     * Group events by tracks for visualization
     * @param {Array} events - Processed events
     * @returns {Object} Events grouped by track
     */
    groupEventsByTracks(events) {
        const tracks = {};

        for (const event of events) {
            // Skip control events - they don't belong to tracks
            if (event.event_category === 'control') {
                continue;
            }

            const trackId = event.track_id || 'unknown';

            if (!tracks[trackId]) {
                tracks[trackId] = {
                    id: trackId,
                    name: this.getTrackDisplayName(trackId),
                    fullName: this.getTrackFullName(trackId),
                    className: this.getTrackClassName(trackId),
                    events: [],
                    client_events: [],
                    server_events: []
                };
            }

            tracks[trackId].events.push(event);

            if (event.vantage_point === this.vantagePoints.CLIENT) {
                tracks[trackId].client_events.push(event);
            } else {
                tracks[trackId].server_events.push(event);
            }
        }

        return tracks;
    }

    /**
     * Get display name for track (may be truncated for long names)
     * @param {string} trackId - Track ID
     * @returns {string} Display name
     */
    getTrackDisplayName(trackId) {
        if (!trackId || typeof trackId !== 'string' || trackId === 'unknown') return 'Unknown';

        // Prefer reconstructed track name when trackId is a track_alias.
        if (this._trackAliasToDisplayName && this._trackAliasToDisplayName[trackId]) {
            return this._trackAliasToDisplayName[trackId];
        }

        // For comma-separated names like "alice,audio", return as-is
        if (trackId.includes(',')) {
            return trackId;
        }

        // Capitalize simple names
        return trackId.charAt(0).toUpperCase() + trackId.slice(1);
    }

    /**
     * Get the full (untruncated) track name including namespace
     * @param {string} trackId - Track ID
     * @returns {string} Full track name
     */
    getTrackFullName(trackId) {
        if (!trackId || typeof trackId !== 'string' || trackId === 'unknown') return 'Unknown';

        if (this._trackAliasToFullName && this._trackAliasToFullName[trackId]) {
            return this._trackAliasToFullName[trackId];
        }

        return this.getTrackDisplayName(trackId);
    }

    /**
     * Get CSS-safe class name for track
     * @param {string} trackId - Track ID
     * @returns {string} CSS-safe class name
     */
    getTrackClassName(trackId) {
        if (!trackId || typeof trackId !== 'string') return 'unknown';
        // Replace commas and other CSS-unsafe characters with hyphens
        return trackId.replace(/[,\s\.]/g, '-').toLowerCase();
    }

    /**
     * Calculate timeline bounds
     * @param {Array} events - Processed events
     * @returns {Object} Timeline bounds
     */
    calculateTimelineBounds(events) {
        if (events.length === 0) {
            return { start: 0, end: 1000, duration: 1000 };
        }

        const timestamps = events.map(e => e.relative_time);
        const start = Math.min(...timestamps);
        const end = Math.max(...timestamps);

        return {
            start,
            end,
            duration: end - start
        };
    }

    /**
     * Calculate event statistics
     * @param {Array} events - Processed events
     * @returns {Object} Event statistics
     */
    calculateEventStats(events) {
        const stats = {
            total: events.length,
            by_category: {},
            by_vantage_point: { client: 0, server: 0 },
            by_track: {}
        };

        for (const event of events) {
            // Count by category
            const category = event.event_category;
            stats.by_category[category] = (stats.by_category[category] || 0) + 1;

            // Count by vantage point
            stats.by_vantage_point[event.vantage_point]++;

            // Count by track
            const trackId = event.track_id || 'default';
            stats.by_track[trackId] = (stats.by_track[trackId] || 0) + 1;
        }

        return stats;
    }

    /**
     * Generate example MoQ QLOG data from sample_serialized.qlog mLogs
     * @returns {Array<string>} Array of NDJSON strings, one per serialized MoQ event
     */
    generateExampleData() {
        // Return the mLogs from sample_serialized.qlog as an array of strings.
        return [
            '{"data":{"message":{"number_of_parameters":0,"content_exists":0,"group_order":1,"expires":0,"track_alias":0,"request_id":0,"type":"subscribe_ok"},"streamId":"0"},"time":"10933134","name":"moqt:control_message_created","vantagePoint":"server"}',
            '{"data":{"message":{"setup_parameters":[{"value":100,"name":"max_request_id"},{"value":1024,"name":"max_auth_token_cache_size"}],"number_of_parameters":2,"selected_version":4278190094,"type":"server_setup"},"streamId":"0"},"time":"6316988","name":"moqt:control_message_created","vantagePoint":"server"}',
            '{"time":"62056206","data":{"message":{"stream_count":1,"reason":"Testing","status_code":2,"request_id":0,"type":"publish_done"},"streamId":"0"},"name":"moqt:control_message_created","vantagePoint":"server"}',
            '{"data":{"message":{"end_group":0,"number_of_parameters":0,"forward":0,"filter_type":1,"group_order":1,"subscriber_priority":128,"track_namespace":["moq-test-00","0","0","0","0","0","1","1024","100","50","1","1","0","-1","-1","0"],"track_name":"test","request_id":0,"type":"subscribe"},"streamId":"0"},"time":"10389062","name":"moqt:control_message_parsed","vantagePoint":"server"}',
            '{"time":"6255364","data":{"message":{"number_of_parameters":2,"setup_parameters":[{"value":100,"name":"max_request_id"},{"value":1024,"name":"max_auth_token_cache_size"}],"supported_versions":[4278190094],"number_of_supported_versions":1,"type":"client_setup"},"streamId":"0"},"name":"moqt:control_message_parsed","vantagePoint":"server"}',
            '{"data":{"streamType":"0","streamId":"4","owner":"1"},"time":"4667082","name":"moqt:stream_type_set","vantagePoint":"server"}',
            '{"data":{"streamType":"1","streamId":"15","owner":"0"},"time":"11221510","name":"moqt:stream_type_set","vantagePoint":"server"}',
            '{"data":{"extensions_present":true,"subgroup_id":0,"contains_end_of_group":false,"publisher_priority":128,"group_id":0,"track_alias":0,"stream_id":15},"time":"11231870","name":"moqt:subgroup_header_created","vantagePoint":"server"}',
            '{"data":{"objectPayload":"tttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt","subgroupId":"18446744073709551615","objectPayloadLength":"1024","objectId":"18446744073709551615","extensionHeadersLength":"0","groupId":"0","streamId":"15"},"time":"11303190","name":"moqt:subgroup_object_created","vantagePoint":"server"}'
        ];
    }
}

// Global parser instance
window.MoQParser = MoQParser;
