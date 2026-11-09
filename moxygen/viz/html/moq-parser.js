/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

class MoQParser {
    constructor({ mlogTimeUnits = 'ns' } = {}) {
        this.mlogTimeUnits = mlogTimeUnits;
        this.eventTypes = {
            CONTROL_MESSAGE_CREATED: 'moqt:control_message_created',
            CONTROL_MESSAGE_PARSED: 'moqt:control_message_parsed',
            OBJECT_DATAGRAM_CREATED: 'moqt:object_datagram_created',
            OBJECT_DATAGRAM_PARSED: 'moqt:object_datagram_parsed',
            OBJECT_DATAGRAM_STATUS_CREATED: 'moqt:object_datagram_status_created',
            OBJECT_DATAGRAM_STATUS_PARSED: 'moqt:object_datagram_status_parsed',
            PADDING_DATAGRAM_CREATED: 'moqt:padding_datagram_created',
            PADDING_DATAGRAM_PARSED: 'moqt:padding_datagram_parsed',
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
        this.vantagePoints = { CLIENT: 'client', SERVER: 'server' };
    }

    /** Parse QLOG traces or the JSON records written by FileMLogger. */
    parseQLog(input) {
        const data = typeof input === 'string' ? this.parseJSONRecords(input) : input;
        let traces;
        if (Array.isArray(data)) {
            traces = [{ events: data }];
        } else if (data?.traces) {
            traces = data.traces;
        } else if (data?.name) {
            traces = [{ events: [data] }];
        } else if (data?.events) {
            traces = [data];
        }
        if (!Array.isArray(traces) || traces.length === 0) {
            throw new Error('Invalid MoQ log: No traces or event records found');
        }

        const events = [];
        traces.forEach((trace, traceIndex) => {
            if (!Array.isArray(trace.events)) {
                throw new Error('Invalid MoQ log: No events found in trace');
            }
            for (const event of this.processEvents(trace.events, trace, traceIndex)) events.push(event);
        });
        events.sort((a, b) => a.timestamp - b.timestamp ||
            a.trace_index - b.trace_index || a.originalIndex - b.originalIndex);
        const startTime = events[0]?.timestamp ?? 0;
        events.forEach(event => { event.relative_time = event.timestamp - startTime; });

        const trace = traces[0];
        return {
            metadata: {
                title: trace.title || data.title || 'MoQ Transport Session',
                description: trace.description || '',
                vantage_point: trace.vantage_point || {},
                configuration: trace.configuration || {},
                start_time: startTime
            },
            events,
            tracks: this.groupEventsByTracks(events),
            timelineBounds: this.calculateTimelineBounds(events),
            totalEvents: events.length,
            eventStats: this.calculateEventStats(events)
        };
    }

    parseJSONRecords(text) {
        const records = [];
        let start = -1;
        let depth = 0;
        let inString = false;
        let escaped = false;
        // FileMLogger pretty-prints each record, so newline splitting is insufficient.
        for (let i = 0; i < text.length; i++) {
            const c = text[i];
            if (start === -1) {
                if (/\s/.test(c) || c === '\u001e') continue;
                if (c !== '{' && c !== '[') {
                    throw new Error(`Invalid JSON record at character ${i}`);
                }
                start = i;
            }
            if (inString) {
                if (escaped) escaped = false;
                else if (c === '\\') escaped = true;
                else if (c === '"') inString = false;
            } else if (c === '"') {
                inString = true;
            } else if (c === '{' || c === '[') {
                depth++;
            } else if (c === '}' || c === ']') {
                depth--;
                if (depth === 0) {
                    records.push(JSON.parse(text.slice(start, i + 1)));
                    start = -1;
                }
            }
        }
        if (start !== -1) throw new Error('Incomplete JSON record at end of file');
        return records.length === 1 ? records[0] : records;
    }

    parseEventTime(event, timeUnits) {
        const value = Array.isArray(event) ? event[0] : event.time;
        const numeric = typeof value === 'number' ||
            (typeof value === 'string' && value.trim() !== '' && Number.isFinite(Number(value)));
        if (!numeric) {
            const timestamp = typeof value === 'string' ? Date.parse(value) : NaN;
            if (!Number.isFinite(timestamp)) throw new Error('Invalid MoQ event timestamp');
            return timestamp;
        }
        // MLogEventCreator uses system_clock ticks, which are nanoseconds on Linux.
        const units = timeUnits ?? (event.vantagePoint !== undefined ? this.mlogTimeUnits : 'ms');
        const scales = { ns: 1e-6, us: 1e-3, ms: 1, s: 1000 };
        if (scales[units] === undefined) throw new Error(`Unsupported time units: ${units}`);
        const timestamp = Number(value) * scales[units];
        if (!Number.isFinite(timestamp)) throw new Error('Invalid MoQ event timestamp');
        return timestamp;
    }

    processEvents(rawEvents, trace = {}, traceIndex = 0) {
        const events = [];
        rawEvents.forEach((raw, index) => {
            const rawName = Array.isArray(raw) ? raw[2] : raw?.name;
            const name = this.eventTypes[rawName] ?? rawName;
            if (!this.isMoQEvent(name)) return;
            const data = (Array.isArray(raw) ? raw[3] : raw.data) || {};
            const message = data.message || data;
            const vantage = this.determineVantagePoint(name, data, raw, trace);
            const direction = name.endsWith('_parsed') || data.owner === 'remote' ? 'received' : 'sent';
            events.push({
                id: `event_${traceIndex}_${index}`,
                originalIndex: index,
                trace_index: traceIndex,
                timestamp: this.parseEventTime(raw, trace.configuration?.time_units ?? trace.common_fields?.time_units),
                relative_time: 0,
                category: Array.isArray(raw) ? raw[1] : 'transport',
                name,
                data,
                vantage_point: vantage,
                direction,
                sender: direction === 'sent' ? vantage : this.otherEndpoint(vantage),
                event_category: this.categorizeEvent(name),
                track_id: this.extractTrackId(message),
                track_alias: message.track_alias ?? data.track_alias ?? null,
                request_id: message.request_id ?? message.subscribe_id ?? message.subscribeId ?? data.request_id ?? null,
                stream_id: data.stream_id ?? data.streamId ?? null,
                group_id: this.extractGroupId(data),
                subgroup_id: this.extractSubgroupId(data),
                object_id: this.extractObjectId(data),
                object_size: this.extractObjectSize(data),
                control_message_type: name.includes('control_message') ? this.extractControlMessageType(data) : null,
                raw_event: raw
            });
        });
        events.sort((a, b) => a.timestamp - b.timestamp || a.originalIndex - b.originalIndex);
        this.resolveEventContext(events);
        return events;
    }

    isMoQEvent(name) {
        return Object.values(this.eventTypes).includes(name);
    }

    otherEndpoint(vantage) {
        return vantage === 'client' ? 'server' : 'client';
    }

    determineVantagePoint(name, data, raw = {}, trace = {}) {
        const value = raw.vantagePoint ?? raw.vantage_point ?? data.vantage_point ?? trace.vantage_point;
        const vantage = typeof value === 'object' ? value?.type : value;
        if (typeof vantage === 'string' && ['client', 'server'].includes(vantage.toLowerCase())) {
            return vantage.toLowerCase();
        }
        return name.endsWith('_parsed') ? 'server' : 'client';
    }

    categorizeEvent(name) {
        if (name.includes('control_message')) return 'control';
        if (name.includes('padding_datagram')) return 'padding';
        if (name.includes('object_datagram')) return 'object';
        if (name.includes('subgroup')) return 'subgroup';
        if (name.includes('fetch')) return 'fetch';
        return 'stream';
    }

    byteString(value) {
        if (typeof value === 'string') return value;
        if (typeof value?.value === 'string') return value.value;
        if (typeof value?.value_bytes === 'string') return `0x${value.value_bytes}`;
        return null;
    }

    extractTrackId(data) {
        const explicit = data.track_id ?? data.trackId;
        if (explicit !== undefined && explicit !== null) return String(explicit);
        const name = this.byteString(data.track_name);
        if (name !== null) {
            const namespace = Array.isArray(data.track_namespace) ? data.track_namespace :
                data.track_namespace !== undefined ? [data.track_namespace] : [];
            return [...namespace.map(value => this.byteString(value) ?? '?'), name].join('/');
        }
        return data.track_alias !== undefined && data.track_alias !== null ? `alias:${data.track_alias}` : null;
    }

    extractGroupId(data) {
        return this.numberOrNull(data.group_id ?? data.groupId ?? data.group);
    }

    extractSubgroupId(data) {
        return this.numberOrNull(data.subgroup_id ?? data.subgroupId ?? data.subgroup);
    }

    extractObjectId(data) {
        return this.numberOrNull(data.object_id ?? data.objectId ?? data.object);
    }

    numberOrNull(value) {
        return value === undefined || value === null ? null : Number(value);
    }

    extractObjectSize(data) {
        return Number(data.object_payload_length ?? data.objectPayloadLength ??
            data.object_payload?.payload_length ?? data.object_payload?.length ??
            data.object_size ?? data.objectSize ?? data.payload_length ?? data.size ??
            data.padding_length ?? data.length ?? 0);
    }

    extractControlMessageType(data) {
        const type = data.message?.type ?? data.message_type ?? data.messageType ?? data.type ?? data.control_type;
        return typeof type === 'string' ? type.replace(/([a-z])([A-Z])/g, '$1_$2').toUpperCase() : null;
    }

    parameter(message, name) {
        return message.parameters?.find(parameter => parameter.name === name)?.value ?? null;
    }

    streamKey(event) {
        return `${event.vantage_point}:${event.direction}:${event.stream_id}`;
    }

    aliasEndpoint(event) {
        return event.control_message_type === 'SUBSCRIBE' ? this.otherEndpoint(event.sender) : event.sender;
    }

    resolveEventContext(events) {
        const requests = new Map();
        const aliases = new Map();
        const streams = new Map();
        // Request IDs identify tracks; aliases are scoped to the publishing endpoint.
        for (const event of events) {
            const message = event.data.message || event.data;
            const track = this.extractTrackId(message);
            if (event.event_category === 'control' && track !== null && !track.startsWith('alias:')) {
                if (event.request_id !== null) {
                    requests.set(String(event.request_id), {
                        track_id: track,
                        group_order: this.numberOrNull(this.parameter(message, 'group_order') ??
                            message.group_order ?? (event.control_message_type === 'FETCH' ? 1 : null))
                    });
                }
                if (event.track_alias !== null) {
                    aliases.set(`${this.aliasEndpoint(event)}:${event.track_alias}`, track);
                }
            }
        }
        for (const event of events) {
            const request = requests.get(String(event.request_id));
            if (event.event_category === 'control' && event.track_alias !== null && request) {
                aliases.set(`${this.aliasEndpoint(event)}:${event.track_alias}`, request.track_id);
            }
        }

        for (const event of events) {
            const data = event.data;
            const request = requests.get(String(event.request_id));
            if (event.track_alias !== null) {
                const publisher = this.aliasEndpoint(event);
                event.track_id = aliases.get(`${publisher}:${event.track_alias}`) ??
                    (event.track_id?.startsWith('alias:') ? `alias:${publisher}:${event.track_alias}` : event.track_id);
            }
            if (event.track_id === null && request && event.control_message_type !== 'MAX_REQUEST_ID') {
                event.track_id = request.track_id;
            }
            if (event.stream_id === null || event.event_category === 'control') continue;
            const key = this.streamKey(event);
            let state = streams.get(key);
            if (event.name.includes('subgroup_header')) {
                state = {
                    track_id: event.track_id,
                    group_id: event.group_id,
                    subgroup_id: event.subgroup_id ?? (data.subgroup_id_mode === 1 ? 0 : null),
                    subgroup_id_mode: data.subgroup_id_mode,
                    object_id: null,
                    type: 'subgroup'
                };
                streams.set(key, state);
                event.subgroup_id = state.subgroup_id;
            } else if (event.name.includes('fetch_header')) {
                state = {
                    track_id: request?.track_id ?? `request:${event.request_id}`,
                    request_id: event.request_id,
                    group_order: request?.group_order ?? null,
                    group_id: null,
                    subgroup_id: null,
                    object_id: null,
                    has_objects: false,
                    type: 'fetch'
                };
                streams.set(key, state);
            } else if (state?.type === 'subgroup' && event.name.includes('subgroup_object')) {
                if (event.object_id === null && data.object_id_delta !== undefined) {
                    const delta = Number(data.object_id_delta);
                    event.object_id = state.object_id === null ? delta : state.object_id + delta + 1;
                }
                state.object_id = event.object_id;
                if (state.subgroup_id === null && state.subgroup_id_mode === 2) {
                    state.subgroup_id = event.object_id;
                }
                event.group_id ??= state.group_id;
                event.subgroup_id ??= state.subgroup_id;
            } else if (state?.type === 'fetch' && event.name.includes('fetch_object')) {
                this.resolveFetchObject(event, state);
            }
            event.track_id = state?.track_id ?? event.track_id ?? `stream:${event.stream_id}`;
            event.request_id ??= state?.request_id ?? null;
        }
        // A stream_type_set record can precede the header that identifies its track.
        for (const event of events) {
            if (event.event_category === 'stream') {
                event.track_id = streams.get(this.streamKey(event))?.track_id ?? null;
            }
        }
    }

    resolveFetchObject(event, state) {
        const data = event.data;
        const groupDelta = this.numberOrNull(data.group_id_delta);
        const objectDelta = this.numberOrNull(data.object_id_delta);
        const rangeEnd = data.end_of_nonexistent_range || data.end_of_unknown_range || data.end_of_timed_out_range;
        // Range-end records store absolute endpoints in the fields named *_delta.
        if (rangeEnd) {
            event.group_id ??= groupDelta;
            event.object_id ??= objectDelta;
        } else {
            if (event.group_id === null) {
                if (groupDelta === null) event.group_id = state.group_id;
                else if (!state.has_objects) event.group_id = groupDelta;
                else if (state.group_id !== null && (state.group_order === 1 || state.group_order === 2)) {
                    event.group_id = state.group_id + (state.group_order === 2 ? -1 : 1) * (groupDelta + 1);
                }
            }
            if (event.object_id === null) {
                if (objectDelta === null) event.object_id = state.object_id === null ? null : state.object_id + 1;
                else if (groupDelta !== null || state.object_id === null) event.object_id = objectDelta;
                else event.object_id = state.object_id + objectDelta;
            }
            if (!data.datagram) {
                switch (data.subgroup_id_bits) {
                    case 0: event.subgroup_id = 0; break;
                    case 1: event.subgroup_id = state.subgroup_id; break;
                    case 2: event.subgroup_id = state.subgroup_id === null ? null : state.subgroup_id + 1; break;
                }
                state.subgroup_id = event.subgroup_id;
            } else {
                state.subgroup_id = 0;
            }
        }
        state.group_id = event.group_id;
        state.object_id = event.object_id;
        state.has_objects = true;
    }

    groupEventsByTracks(events) {
        const tracks = Object.create(null);
        for (const event of events) {
            if (event.event_category === 'control' || event.event_category === 'padding' ||
                (event.event_category === 'stream' && event.track_id === null)) continue;
            const id = event.track_id ?? 'unknown';
            if (!tracks[id]) {
                tracks[id] = {
                    id,
                    name: this.getTrackDisplayName(id),
                    className: this.getTrackClassName(id),
                    events: [],
                    client_events: [],
                    server_events: []
                };
            }
            tracks[id].events.push(event);
            tracks[id][`${event.vantage_point}_events`].push(event);
        }
        return tracks;
    }

    getTrackDisplayName(id) {
        return id === 'unknown' ? 'Unknown' : String(id);
    }

    getTrackClassName(id) {
        return 'track-' + Array.from(String(id), c => c.codePointAt(0).toString(16)).join('-');
    }

    calculateTimelineBounds(events) {
        if (events.length === 0) return { start: 0, end: 1000, duration: 1000 };
        const end = events[events.length - 1].relative_time;
        return { start: 0, end, duration: end };
    }

    calculateEventStats(events) {
        const stats = {
            total: events.length,
            by_category: Object.create(null),
            by_vantage_point: { client: 0, server: 0 },
            by_track: Object.create(null)
        };
        for (const event of events) {
            const category = event.event_category;
            stats.by_category[category] = (stats.by_category[category] || 0) + 1;
            stats.by_vantage_point[event.vantage_point]++;
            const track = event.track_id ?? 'default';
            stats.by_track[track] = (stats.by_track[track] || 0) + 1;
        }
        return stats;
    }

    generateExampleData() {
        const events = [];
        const exchange = (time, sender, type, data) => {
            for (const [suffix, vantage, delay] of [
                ['created', sender, 0], ['parsed', this.otherEndpoint(sender), 5]
            ]) {
                events.push({
                    time: String((time + delay) * 1e6),
                    vantagePoint: vantage,
                    name: `moqt:${type}_${suffix}`,
                    data
                });
            }
        };
        const control = (time, sender, message) =>
            exchange(time, sender, 'control_message', { stream_id: 0, message });
        control(0, 'client', { type: 'setup', setup_options: [{ name: 'max_request_id', value: 100 }] });
        control(20, 'server', { type: 'setup', setup_options: [{ name: 'max_request_id', value: 100 }] });
        control(50, 'server', {
            type: 'publish', request_id: 1, track_alias: 0,
            track_namespace: [{ value: 'meeting1' }, { value: 'bob' }],
            track_name: { value: 'video' }, number_of_parameters: 0
        });
        control(100, 'client', {
            type: 'publish', request_id: 0, track_alias: 0,
            track_namespace: [{ value: 'meeting1' }, { value: 'alice' }],
            track_name: { value: 'audio' }, number_of_parameters: 0
        });
        control(150, 'client', { type: 'request_ok', number_of_parameters: 0 });
        control(180, 'server', { type: 'request_ok', number_of_parameters: 0 });
        exchange(200, 'server', 'subgroup_header', {
            stream_id: 3, track_alias: 0, group_id: 0, subgroup_id_mode: 1,
            contains_end_of_group: true, properties_present: true, first_object: true
        });
        for (let i = 0; i < 3; i++) {
            exchange(220 + i * 100, 'server', 'subgroup_object', {
                stream_id: 3, object_id_delta: 0,
                object_payload_length: i === 0 ? 40000 : 2000,
                properties: [{ type: 2, value: i }]
            });
            exchange(240 + i * 100, 'client', 'object_datagram', {
                track_alias: 0, group_id: 0, object_id: i,
                object_payload: { length: 256 }, end_of_group: i === 2
            });
        }
        exchange(550, 'server', 'padding_datagram', { padding_length: 32 });
        control(600, 'client', {
            type: 'fetch', request_id: 2,
            track_namespace: [{ value: 'meeting1' }, { value: 'bob' }],
            track_name: { value: 'video' }, number_of_parameters: 1,
            parameters: [{ name: 'group_order', value: 2 }]
        });
        control(650, 'server', { type: 'fetch_ok', end_of_track: false,
            end_location: { group: 0, object: 2 }, number_of_parameters: 0 });
        exchange(700, 'server', 'fetch_header', { stream_id: 7, request_id: 2 });
        exchange(750, 'server', 'fetch_object', {
            stream_id: 7, datagram: false, subgroup_id_bits: 0,
            group_id_delta: 2, object_id_delta: 0, object_payload_length: 1200,
            end_of_nonexistent_range: false, end_of_unknown_range: false, end_of_timed_out_range: false
        });
        exchange(850, 'server', 'fetch_object', {
            stream_id: 7, datagram: false, subgroup_id_bits: 1,
            group_id_delta: 0, object_id_delta: 0, object_payload_length: 800,
            end_of_nonexistent_range: false, end_of_unknown_range: false, end_of_timed_out_range: false
        });
        exchange(950, 'server', 'fetch_object', {
            stream_id: 7, datagram: false, group_id_delta: 0, object_id_delta: 2,
            object_payload_length: 0, end_of_nonexistent_range: true,
            end_of_unknown_range: false, end_of_timed_out_range: false
        });
        return events.sort((a, b) => Number(a.time) - Number(b.time));
    }
}

if (typeof window !== 'undefined') window.MoQParser = MoQParser;
if (typeof module !== 'undefined') module.exports = MoQParser;
