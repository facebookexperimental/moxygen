/*
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
     * Parse QLOG JSON data and extract MoQ events
     * @param {Object} qlogData - Raw QLOG JSON data
     * @returns {Object} Parsed timeline data
     */
    parseQLog(qlogData) {
        try {
            // Validate QLOG structure
            if (!qlogData || !qlogData.traces || qlogData.traces.length === 0) {
                throw new Error('Invalid QLOG format: No traces found');
            }

            const trace = qlogData.traces[0];
            if (!trace.events || !Array.isArray(trace.events)) {
                throw new Error('Invalid QLOG format: No events found in trace');
            }

            // Extract metadata
            const metadata = this.extractMetadata(trace);

            // Process events
            const events = this.processEvents(trace.events);

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
            console.error('Error parsing QLOG:', error);
            throw error;
        }
    }

    /**
     * Extract metadata from QLOG trace
     * @param {Object} trace - QLOG trace object
     * @returns {Object} Metadata object
     */
    extractMetadata(trace) {
        return {
            title: trace.title || 'MoQ Transport Session',
            description: trace.description || 'Media over QUIC data transfer visualization',
            vantage_point: trace.vantage_point || { type: 'client' },
            configuration: trace.configuration || {},
            start_time: this.extractStartTime(trace.events)
        };
    }

    /**
     * Extract start time from events
     * @param {Array} events - Array of QLOG events
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
     * Parse event timestamp
     * @param {Array} event - QLOG event array [time, category, name, data]
     * @returns {number} Timestamp in milliseconds
     */
    parseEventTime(event) {
        if (!Array.isArray(event) || event.length < 1) return 0;

        const timeValue = event[0];

        // Handle different time formats
        if (typeof timeValue === 'number') {
            // Assume microseconds if > 1e12, otherwise milliseconds
            return timeValue > 1e12 ? timeValue / 1000 : timeValue;
        } else if (typeof timeValue === 'string') {
            return new Date(timeValue).getTime();
        }

        return 0;
    }

    /**
     * Process QLOG events and extract MoQ-specific data
     * @param {Array} rawEvents - Raw QLOG events
     * @returns {Array} Processed MoQ events
     */
    processEvents(rawEvents) {
        const processedEvents = [];

        for (let i = 0; i < rawEvents.length; i++) {
            const rawEvent = rawEvents[i];

            if (!Array.isArray(rawEvent) || rawEvent.length < 3) {
                console.warn(`Skipping malformed event at index ${i}:`, rawEvent);
                continue;
            }

            const [time, category, eventName, eventData] = rawEvent;

            // Only process MoQ events
            if (!this.isMoQEvent(eventName)) {
                continue;
            }

            const processedEvent = {
                id: `event_${i}`,
                originalIndex: i, // Track file order for secondary sorting
                timestamp: this.parseEventTime(rawEvent),
                relative_time: 0, // Will be calculated later
                category: category || 'transport',
                name: eventName,
                data: eventData || {},
                vantage_point: this.determineVantagePoint(eventName, eventData),
                event_category: this.categorizeEvent(eventName, eventData),
                track_id: this.extractTrackId(eventData),
                group_id: this.extractGroupId(eventData),
                subgroup_id: this.extractSubgroupId(eventData),
                object_id: this.extractObjectId(eventData),
                object_size: this.extractObjectSize(eventData),
                control_message_type: this.extractControlMessageType(eventData),
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
     * Determine vantage point from event data
     * @param {string} eventName - Event name
     * @param {Object} eventData - Event data
     * @returns {string} Vantage point (client or server)
     */
    determineVantagePoint(eventName, eventData) {
        // Check if explicitly specified in data
        if (eventData && eventData.vantage_point) {
            return eventData.vantage_point.toLowerCase();
        }

        // Infer from event name patterns
        if (eventName.includes('_created')) {
            // Created events are typically from the sender's perspective
            return this.vantagePoints.CLIENT;
        } else if (eventName.includes('_parsed')) {
            // Parsed events are typically from the receiver's perspective
            return this.vantagePoints.SERVER;
        }

        return this.vantagePoints.CLIENT; // Default
    }

    /**
     * Categorize event for visualization
     * @param {string} eventName - Event name
     * @param {Object} eventData - Event data to check message type
     * @returns {string} Event category
     */
    categorizeEvent(eventName, eventData) {
        // Only real control messages (SETUP, SUBSCRIBE, PUBLISH_NAMESPACE, etc.) go in control columns
        if (eventName.includes('control_message')) {
            const msgType = eventData?.message_type || '';
            if (msgType.match(/^(SETUP|SUBSCRIBE|PUBLISH_NAMESPACE|UNSUBSCRIBE|GOAWAY)/)) {
                return this.eventCategories.CONTROL;
            }
        }

        // Everything else goes in track columns
        if (eventName.includes('object_datagram')) {
            return this.eventCategories.OBJECT;
        } else if (eventName.includes('subgroup')) {
            return this.eventCategories.SUBGROUP;
        } else if (eventName.includes('fetch')) {
            return this.eventCategories.FETCH;
        } else if (eventName.includes('stream_type')) {
            return this.eventCategories.STREAM;
        }

        return 'track_data'; // Default to track data, not control
    }

    /**
     * Extract track ID from event data
     * @param {Object} eventData - Event data
     * @returns {string|null} Track ID
     */
    extractTrackId(eventData) {
        if (!eventData) return null;

        return eventData.track_id ||
               eventData.trackId ||
               eventData.track_alias ||
               eventData.track_name ||
               null;
    }

    /**
     * Extract group ID from event data
     * @param {Object} eventData - Event data
     * @returns {number|null} Group ID
     */
    extractGroupId(eventData) {
        if (!eventData) return null;

        return eventData.group_id ||
               eventData.groupId ||
               eventData.group ||
               null;
    }

    /**
     * Extract subgroup ID from event data
     * @param {Object} eventData - Event data
     * @returns {number|null} Subgroup ID
     */
    extractSubgroupId(eventData) {
        if (!eventData) return null;

        return eventData.subgroup_id ||
               eventData.subgroupId ||
               eventData.subgroup ||
               null;
    }

    /**
     * Extract object ID from event data
     * @param {Object} eventData - Event data
     * @returns {number|null} Object ID
     */
    extractObjectId(eventData) {
        if (!eventData) return null;

        return eventData.object_id ||
               eventData.objectId ||
               eventData.object ||
               null;
    }

    /**
     * Extract object size from event data
     * @param {Object} eventData - Event data
     * @returns {number} Object size in bytes
     */
    extractObjectSize(eventData) {
        if (!eventData) return 0;

        return eventData.object_size ||
               eventData.objectSize ||
               eventData.size ||
               eventData.length ||
               eventData.payload_length ||
               0;
    }

    /**
     * Extract control message type from event data
     * @param {Object} eventData - Event data
     * @returns {string|null} Control message type
     */
    extractControlMessageType(eventData) {
        if (!eventData) return null;

        return eventData.message_type ||
               eventData.messageType ||
               eventData.type ||
               eventData.control_type ||
               null;
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
     * Get display name for track
     * @param {string} trackId - Track ID
     * @returns {string} Display name
     */
    getTrackDisplayName(trackId) {
        if (!trackId || typeof trackId !== 'string' || trackId === 'unknown') return 'Unknown';

        // For comma-separated names like "alice,audio", return as-is
        if (trackId.includes(',')) {
            return trackId;
        }

        // Capitalize simple names
        return trackId.charAt(0).toUpperCase() + trackId.slice(1);
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
     * Generate realistic Alice/Bob meeting MoQ QLOG data
     * @returns {Object} Example QLOG data
     */
    generateExampleData() {
        const now = Date.now();
        const events = [];
        let currentTime = now;

        // === PUBLISH_NAMESPACE PHASE ===
        // Client publishes namespace=(meeting1, alice)
        events.push([
            currentTime,
            'transport',
            this.eventTypes.CONTROL_MESSAGE_CREATED,
            {
                vantage_point: 'client',
                message_type: 'PUBLISH_NAMESPACE',
                namespace: 'meeting1',
                track_namespace: 'alice'
            }
        ]);
        currentTime += 50;

        events.push([
            currentTime,
            'transport',
            this.eventTypes.CONTROL_MESSAGE_PARSED,
            {
                vantage_point: 'server',
                message_type: 'PUBLISH_NAMESPACE_OK',
                namespace: 'meeting1',
                track_namespace: 'alice'
            }
        ]);
        currentTime += 100;

        // Server publishes namespace=(meeting1, bob)
        events.push([
            currentTime,
            'transport',
            this.eventTypes.CONTROL_MESSAGE_CREATED,
            {
                vantage_point: 'server',
                message_type: 'PUBLISH_NAMESPACE',
                namespace: 'meeting1',
                track_namespace: 'bob'
            }
        ]);
        currentTime += 50;

        events.push([
            currentTime,
            'transport',
            this.eventTypes.CONTROL_MESSAGE_PARSED,
            {
                vantage_point: 'client',
                message_type: 'PUBLISH_NAMESPACE_OK',
                namespace: 'meeting1',
                track_namespace: 'bob'
            }
        ]);
        currentTime += 200;

        // === SUBSCRIPTION PHASE ===
        // Client subscribes to bob,audio and bob,video
        const clientSubscriptions = ['bob,audio', 'bob,video'];
        clientSubscriptions.forEach(trackName => {
            events.push([
                currentTime,
                'transport',
                this.eventTypes.CONTROL_MESSAGE_CREATED,
                {
                    vantage_point: 'client',
                    message_type: 'SUBSCRIBE',
                    track_id: trackName,
                    track_namespace: trackName
                }
            ]);
            currentTime += 20;

            events.push([
                currentTime,
                'transport',
                this.eventTypes.CONTROL_MESSAGE_PARSED,
                {
                    vantage_point: 'server',
                    message_type: 'SUBSCRIBE_OK',
                    track_id: trackName,
                    track_namespace: trackName
                }
            ]);
            currentTime += 30;
        });

        // Server subscribes to alice,audio and alice,video
        const serverSubscriptions = ['alice,audio', 'alice,video'];
        serverSubscriptions.forEach(trackName => {
            events.push([
                currentTime,
                'transport',
                this.eventTypes.CONTROL_MESSAGE_CREATED,
                {
                    vantage_point: 'server',
                    message_type: 'SUBSCRIBE',
                    track_id: trackName,
                    track_namespace: trackName
                }
            ]);
            currentTime += 20;

            events.push([
                currentTime,
                'transport',
                this.eventTypes.CONTROL_MESSAGE_PARSED,
                {
                    vantage_point: 'client',
                    message_type: 'SUBSCRIBE_OK',
                    track_id: trackName,
                    track_namespace: trackName
                }
            ]);
            currentTime += 30;
        });

        currentTime += 500; // Pause before media starts

        // === MEDIA STREAMING PHASE ===
        const streamDuration = 10000; // 10 seconds of streaming
        const endTime = currentTime + streamDuration;
        let groupId = 0;

        while (currentTime < endTime) {
            const secondStart = currentTime;

            // Video: New group every 1 second - Start with subgroup header, then objects
            // Create subgroup headers first
            events.push([
                secondStart,
                'transport',
                this.eventTypes.SUBGROUP_HEADER_CREATED,
                {
                    vantage_point: 'client',
                    track_id: 'alice,video',
                    group_id: groupId,
                    subgroup_id: 0,
                    object_count: 30
                }
            ]);

            events.push([
                secondStart + 2,
                'transport',
                this.eventTypes.SUBGROUP_HEADER_CREATED,
                {
                    vantage_point: 'server',
                    track_id: 'bob,video',
                    group_id: groupId,
                    subgroup_id: 0,
                    object_count: 30
                }
            ]);

            // Then create subgroup objects (30 objects, first large, rest small)
            for (let objId = 0; objId < 30; objId++) {
                const objectTime = secondStart + 10 + (objId * 33); // Start 10ms after header
                const isFirstObject = objId === 0;
                const objectSize = isFirstObject ? 40000 : Math.floor(Math.random() * 1000 + 1000); // 40KB vs 1-2KB

                // Alice video (client sends)
                events.push([
                    objectTime,
                    'transport',
                    this.eventTypes.SUBGROUP_OBJECT_CREATED,
                    {
                        vantage_point: 'client',
                        track_id: 'alice,video',
                        group_id: groupId,
                        subgroup_id: 0,
                        object_id: objId,
                        object_size: objectSize
                    }
                ]);

                // Bob video (server sends)
                events.push([
                    objectTime + 5,
                    'transport',
                    this.eventTypes.SUBGROUP_OBJECT_CREATED,
                    {
                        vantage_point: 'server',
                        track_id: 'bob,video',
                        group_id: groupId,
                        subgroup_id: 0,
                        object_id: objId,
                        object_size: objectSize
                    }
                ]);
            }

            // Audio: Datagrams every 20ms (aligned with video groups)
            for (let audioFrame = 0; audioFrame < 50; audioFrame++) { // 50 frames per second
                const audioTime = secondStart + (audioFrame * 20);
                const audioSize = Math.floor(Math.random() * 200 + 200); // 200-400 bytes

                // Alice audio (client sends)
                events.push([
                    audioTime,
                    'transport',
                    this.eventTypes.OBJECT_DATAGRAM_CREATED,
                    {
                        vantage_point: 'client',
                        track_id: 'alice,audio',
                        group_id: groupId,
                        subgroup_id: 0,
                        object_id: audioFrame,
                        object_size: audioSize
                    }
                ]);

                // Bob audio (server sends)
                events.push([
                    audioTime + 2,
                    'transport',
                    this.eventTypes.OBJECT_DATAGRAM_CREATED,
                    {
                        vantage_point: 'server',
                        track_id: 'bob,audio',
                        group_id: groupId,
                        subgroup_id: 0,
                        object_id: audioFrame,
                        object_size: audioSize
                    }
                ]);
            }

            currentTime += 1000; // Next second
            groupId++;
        }

        return {
            qlog_format: "JSON-SEQ",
            qlog_version: "0.3",
            title: "MoQ Transport - Alice & Bob Meeting",
            traces: [{
                title: "Alice & Bob Video Call Session",
                description: "Realistic MoQ Transport session with cross-subscribed audio/video tracks",
                vantage_point: { type: "client" },
                events: events
            }]
        };
    }
}

// Global parser instance
window.MoQParser = MoQParser;
