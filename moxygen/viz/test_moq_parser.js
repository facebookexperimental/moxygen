/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');
let MoQParser = require('./html/moq-parser');

const cases = [];
const test = (name, body) => cases.push({ name, body });
const record = (name, data = {}, time = '0', vantagePoint = 'client') => ({
    name: `moqt:${name}`, data, time, vantagePoint
});
const control = message => record('control_message_created', { stream_id: 0, message });
const parse = events => new MoQParser().parseQLog(events);
const fetchObjects = (objects, order = 1) => parse([
    control({ type: 'fetch', request_id: 0, track_namespace: [{ value: 'room' }],
        track_name: { value: 'video' }, parameters: [{ name: 'group_order', value: order }] }),
    record('fetch_header_parsed', { stream_id: 3, request_id: 0 }),
    ...objects.map(data => record('fetch_object_parsed', { stream_id: 3, object_payload_length: 0, ...data }))
]).events.filter(event => event.name.includes('fetch_object'));

test('loads the native FileMLogger example with endpoint and nanosecond timing', () => {
    const text = fs.readFileSync(path.join(__dirname, 'examples/sample_moq.mlog'), 'utf8');
    const data = new MoQParser().parseQLog(text);
    assert.strictEqual(data.totalEvents, 20);
    assert.strictEqual(data.timelineBounds.duration, 955);
    assert.strictEqual(data.eventStats.by_vantage_point.client, 20);
    assert.strictEqual(data.eventStats.by_vantage_point.server, 0);
    assert.deepStrictEqual(Object.keys(data.tracks).sort(), ['meeting1/alice/audio', 'meeting1/bob/video']);
    const video = data.events.filter(event => event.name === 'moqt:subgroup_object_parsed');
    assert.deepStrictEqual(video.map(event => event.object_id), [0, 1, 2]);
    assert.deepStrictEqual(video.map(event => event.object_size), [40000, 2000, 2000]);
    assert.strictEqual(data.events.find(event => event.name.includes('padding')).object_size, 32);
});

test('reads pretty JSON sequences, JSONL, record separators, and escaped string delimiters', () => {
    const events = [control({ type: 'request_error', reason_phrase: 'Braces } {, quote " and slash \\' }),
        record('padding_datagram_parsed', { padding_length: 0 }, '1000000')];
    for (const separator of ['\n', '', '\n\u001e']) {
        const data = new MoQParser().parseQLog('\ufeff\u001e' +
            events.map(event => JSON.stringify(event, null, 2)).join(separator));
        assert.strictEqual(data.totalEvents, 2);
        assert.strictEqual(data.events[0].data.message.reason_phrase, events[0].data.message.reason_phrase);
        assert.strictEqual(data.events[1].relative_time, 1);
    }
    assert.strictEqual(new MoQParser().parseQLog(events.map(event => JSON.stringify(event)).join('\n')).totalEvents, 2);
    assert.strictEqual(new MoQParser().parseQLog(JSON.stringify(events)).totalEvents, 2);
    assert.strictEqual(new MoQParser().parseQLog(JSON.stringify(events[0])).totalEvents, 1);
    assert.strictEqual(new MoQParser().parseQLog('').totalEvents, 0);
});

test('rejects malformed and incomplete records and invalid timestamps', () => {
    assert.throws(() => new MoQParser().parseQLog('{"name":'), /Incomplete JSON record/);
    assert.throws(() => new MoQParser().parseQLog('{} invalid'), /Invalid JSON record/);
    assert.throws(() => new MoQParser().parseQLog('{"a":]}'), SyntaxError);
    assert.throws(() => parse([record('padding_datagram_created', {}, 'invalid')]), /timestamp/);
    assert.throws(() => parse(null), /No traces or event records/);
    assert.throws(() => parse({ traces: [{}] }), /No events/);
});

test('classifies every current control message and keeps extensible fields', () => {
    const types = ['setup', 'request_ok', 'request_error', 'publish', 'subscribe', 'fetch',
        'publish_state_notify', 'publish_skipped', 'subscribe_tracks', 'namespace', 'namespace_done',
        'publish_done', 'max_request_id', 'requests_blocked', 'track_status', 'publishNamespaceDone'];
    const messages = types.map(type => ({ type, setup_options: [{ name: 'unknown', value: { nested: 7 } }],
        parameters: [{ name: 'location_filter', value: { filter_type: 'absolute_start', start: { group: 0, object: 0 } } }],
        track_properties: [{ type: 2, value: 0 }] }));
    const data = parse(messages.map(control));
    assert.strictEqual(data.eventStats.by_category.control, types.length);
    assert.strictEqual(data.events[0].control_message_type, 'SETUP');
    assert.strictEqual(data.events[types.length - 1].control_message_type, 'PUBLISH_NAMESPACE_DONE');
    assert.deepStrictEqual(data.events.map(event => event.data.message), messages);
    assert.strictEqual(Object.keys(data.tracks).length, 0);
});

test('preserves numeric and string zero identifiers and payload sizes', () => {
    const data = parse([
        record('object_datagram_created', { track_alias: 0, group_id: 0, object_id: 0, object_payload: { length: 25 } }),
        record('object_datagram_parsed', { track_alias: 0, groupId: '0', subgroupId: '0', objectId: '0',
            object_payload_length: 0, length: 30 }),
        record('subgroup_object_created', { track_id: 0, groupId: '0', subgroupId: '0', objectId: '0',
            objectPayloadLength: '18' })
    ]);
    assert.deepStrictEqual(data.events.map(event => event.group_id), [0, 0, 0]);
    assert.deepStrictEqual(data.events.map(event => event.object_id), [0, 0, 0]);
    assert.deepStrictEqual(data.events.map(event => event.object_size), [25, 0, 18]);
    assert.strictEqual(data.events[0].track_id, 'alias:client:0');
    assert.strictEqual(data.events[2].track_id, '0');
    assert.ok(data.tracks['0']);
});

test('scopes track aliases to publishers and decodes text and binary names', () => {
    const data = parse([
        control({ type: 'publish', request_id: 0, track_alias: 0,
            track_namespace: [{ value: 'room' }, { value_bytes: 'ff00' }], track_name: { value: 'audio' } }),
        record('control_message_parsed', { stream_id: 0, message: { type: 'publish', request_id: 1,
            track_alias: 0, track_namespace: [{ value: 'room' }], track_name: { value_bytes: '766964656f' } } }),
        record('object_datagram_created', { track_alias: 0, group_id: 0, object_id: 0 }),
        record('object_datagram_parsed', { track_alias: 0, group_id: 0, object_id: 0 })
    ]);
    assert.strictEqual(data.events[2].track_id, 'room/0xff00/audio');
    assert.strictEqual(data.events[3].track_id, 'room/0x766964656f');
    assert.strictEqual(data.events[2].sender, 'client');
    assert.strictEqual(data.events[3].sender, 'server');
    const unnamed = parse([record('object_datagram_created', { track_alias: 0 }),
        record('object_datagram_parsed', { track_alias: 0 })]);
    assert.deepStrictEqual(Object.keys(unnamed.tracks).sort(), ['alias:client:0', 'alias:server:0']);
    const parser = new MoQParser();
    assert.notStrictEqual(parser.getTrackClassName('room/video'), parser.getTrackClassName('room-video'));
    assert.notStrictEqual(parser.getTrackClassName('Video'), parser.getTrackClassName('video'));
});

test('reconstructs subgroup IDs and skipped objects in independent sent and received streams', () => {
    const data = parse([
        record('stream_type_set', { stream_id: 0, stream_type: 'subgroup_header', owner: 'local' }),
        record('subgroup_header_created', { stream_id: 0, track_alias: 0, group_id: 0, subgroup_id_mode: 1 }),
        record('subgroup_header_parsed', { stream_id: 0, track_alias: 1, group_id: 1, subgroup_id_mode: 0, subgroup_id: 7 }),
        record('subgroup_object_created', { stream_id: 0, object_id_delta: 0, object_payload_length: 10 }),
        record('subgroup_object_parsed', { stream_id: 0, object_id_delta: 3, object_payload_length: 20 }),
        record('subgroup_object_created', { stream_id: 0, object_id_delta: 2, object_payload_length: 30 }),
        record('subgroup_object_parsed', { stream_id: 0, object_id_delta: 0, object_payload_length: 40 })
    ]);
    const objects = data.events.filter(event => event.name.includes('subgroup_object'));
    assert.deepStrictEqual(objects.map(event => [event.track_id, event.group_id, event.subgroup_id, event.object_id]),
        [['alias:client:0', 0, 0, 0], ['alias:server:1', 1, 7, 3], ['alias:client:0', 0, 0, 3], ['alias:server:1', 1, 7, 4]]);
    assert.strictEqual(data.events[0].track_id, 'alias:client:0');
});

test('infers a subgroup ID from its first object and leaves incomplete streams unresolved', () => {
    const data = parse([
        record('subgroup_header_created', { stream_id: 3, track_alias: 0, group_id: 0, subgroup_id_mode: 2 }),
        record('subgroup_object_created', { stream_id: 3, object_id_delta: 5 }),
        record('subgroup_object_created', { stream_id: 3, object_id_delta: 0 }),
        record('subgroup_object_parsed', { stream_id: 7, object_id_delta: 9 })
    ]);
    assert.strictEqual(data.events[1].subgroup_id, 5);
    assert.strictEqual(data.events[2].subgroup_id, 5);
    assert.strictEqual(data.events[2].object_id, 6);
    assert.strictEqual(data.events[3].track_id, 'stream:7');
    assert.strictEqual(data.events[3].group_id, null);
    assert.strictEqual(data.events[3].object_id, null);
});

test('reconstructs deltas when Scuba returns rows out of timestamp order', () => {
    const data = parse([
        record('subgroup_object_created', { stream_id: 3, object_id_delta: 0 }, '4000000'),
        record('subgroup_object_created', { stream_id: 3, object_id_delta: 0 }, '3000000'),
        record('subgroup_object_created', { stream_id: 3, object_id_delta: 0 }, '2000000'),
        record('subgroup_header_created', { stream_id: 3, track_alias: 0, group_id: 0, subgroup_id_mode: 1 }, '1000000')
    ]);
    assert.deepStrictEqual(data.events.map(event => event.object_id), [null, 0, 1, 2]);
    assert.deepStrictEqual(data.events.map(event => event.group_id), [0, 0, 0, 0]);
});

test('decodes fetch group and object deltas, omissions, and subgroup bits', () => {
    const events = fetchObjects([
        { group_id_delta: 0, object_id_delta: 0, subgroup_id_bits: 0 },
        { subgroup_id_bits: 2 },
        { object_id_delta: 3, subgroup_id_bits: 1 },
        { group_id_delta: 1, object_id_delta: 0, subgroup_id_bits: 3, subgroup_id: 8 },
        { datagram: true },
        { subgroup_id_bits: 2 }
    ]);
    assert.deepStrictEqual(events.map(event => [event.group_id, event.object_id, event.subgroup_id]),
        [[0, 0, 0], [0, 1, 1], [0, 4, 1], [2, 0, 8], [2, 1, null], [2, 2, 1]]);
    assert.ok(events.every(event => event.request_id === 0 && event.track_id === 'room/video'));
});

test('decodes descending fetches using the group_order parameter', () => {
    const events = fetchObjects([
        { group_id_delta: 10, object_id_delta: 5, subgroup_id_bits: 0 },
        { group_id_delta: 2, subgroup_id_bits: 0 },
        { group_id_delta: 0, object_id_delta: 0, subgroup_id_bits: 0 }
    ], 2);
    assert.deepStrictEqual(events.map(event => [event.group_id, event.object_id]), [[10, 5], [7, 6], [6, 0]]);
});

test('interprets all fetch range ends as absolute endpoints and advances subsequent objects', () => {
    for (const range of ['nonexistent', 'unknown', 'timed_out']) {
        const events = fetchObjects([
            { group_id_delta: 10, object_id_delta: 4, subgroup_id_bits: 0 },
            { group_id_delta: 6, object_id_delta: 20, [`end_of_${range}_range`]: true },
            { subgroup_id_bits: 1 },
            { group_id_delta: 0, object_id_delta: 0, subgroup_id_bits: 0 }
        ], 2);
        assert.deepStrictEqual(events.map(event => [event.group_id, event.object_id]), [[10, 4], [6, 20], [6, 21], [5, 0]]);
        assert.strictEqual(events[1].object_size, 0);
    }
});

test('keeps fetch deltas visible when the log omits the request and group order', () => {
    const data = parse([
        record('fetch_header_parsed', { stream_id: 3, request_id: 0 }),
        record('fetch_object_parsed', { stream_id: 3, group_id_delta: 10, object_id_delta: 0 }),
        record('fetch_object_parsed', { stream_id: 3, group_id_delta: 2, object_id_delta: 0 }),
        record('fetch_object_parsed', { stream_id: 3, group_id_delta: 0, object_id_delta: 0 })
    ]);
    assert.deepStrictEqual(data.events.slice(1).map(event => event.group_id), [10, null, null]);
    assert.strictEqual(data.events[2].data.group_id_delta, 2);
    assert.strictEqual(data.events[2].track_id, 'request:0');
});

test('handles legacy QLOG files and explicit time units without magnitude heuristics', () => {
    for (const file of ['sample_moq.qlog', 'sample_serialized.qlog']) {
        const raw = JSON.parse(fs.readFileSync(path.join(__dirname, 'examples', file), 'utf8'));
        const data = parse(raw);
        assert.strictEqual(data.totalEvents, raw.traces[0].events.length);
    }
    for (const [units, step] of [['ns', 1e6], ['us', 1000], ['ms', 1], ['s', 0.001]]) {
        const data = parse({ traces: [{ configuration: { time_units: units }, vantage_point: { type: 'server' },
            events: [[0, 'transport', 'moqt:padding_datagram_created', {}],
                [String(step), 'transport', 'moqt:padding_datagram_parsed', {}]] }] });
        assert.strictEqual(data.timelineBounds.duration, 1);
        assert.ok(data.events.every(event => event.vantage_point === 'server'));
    }
    const parser = new MoQParser();
    assert.strictEqual(parser.parseEventTime([1700000000000]), 1700000000000);
    assert.strictEqual(parser.parseEventTime(['2025-01-15T10:30:00.000Z']), Date.parse('2025-01-15T10:30:00.000Z'));
    assert.throws(() => parser.parseEventTime([1], 'invalid'), /Unsupported time units/);
    const microseconds = new MoQParser({ mlogTimeUnits: 'us' }).parseQLog([
        record('padding_datagram_created', {}, '1000'), record('padding_datagram_parsed', {}, '2000')
    ]);
    assert.strictEqual(microseconds.timelineBounds.duration, 1);
});

test('isolates traces and parser invocations and preserves file order at equal timestamps', () => {
    const parser = new MoQParser();
    const trace = alias => ({ events: [
        record('subgroup_header_created', { stream_id: 0, track_alias: alias, group_id: 0, subgroup_id_mode: 1 }),
        record('subgroup_object_created', { stream_id: 0, object_id_delta: 0 }),
        record('subgroup_object_created', { stream_id: 0, object_id_delta: 0 })
    ] });
    const data = parser.parseQLog({ traces: [trace(0), trace(1)] });
    assert.deepStrictEqual(data.events.map(event => event.track_id), ['alias:client:0', 'alias:client:0', 'alias:client:0',
        'alias:client:1', 'alias:client:1', 'alias:client:1']);
    assert.deepStrictEqual(data.events.map(event => event.object_id), [null, 0, 1, null, 0, 1]);
    assert.strictEqual(new Set(data.events.map(event => event.id)).size, 6);
    assert.strictEqual(parser.parseQLog([record('subgroup_object_created', { stream_id: 0, object_id_delta: 2 })])
        .events[0].object_id, null);
});

test('renders current event labels, directions, and escaped details using millisecond times', () => {
    const code = fs.readFileSync(path.join(__dirname, 'html/moq-timeline.js'), 'utf8');
    let details;
    const Timeline = vm.runInNewContext(code + '\nMoQTimeline;', {
        document: {}, window: {}, $: () => ({ ready() {}, html(value) { details = value; } })
    });
    const timeline = Object.create(Timeline.prototype);
    timeline.viewOptions = { showObjectIds: true };
    const data = parse([
        record('control_message_parsed', { stream_id: 0, message: { type: 'request_error', reason_phrase: '<script>x</script>' } }),
        record('padding_datagram_parsed', { padding_length: 0 }, '500000000'),
        record('object_datagram_created', { track_id: '<video>"', group_id: 0, object_id: 0 }, '1000000000')
    ]);
    timeline.currentData = data;
    assert.strictEqual(timeline.getEventColumnKey(data.events[0]), 'client-control');
    assert.strictEqual(timeline.getEventColumnKey(data.events[1]), 'client-control');
    assert.strictEqual(data.events[0].sender, 'server');
    assert.strictEqual(timeline.getEventLabel(data.events[2]), 'Group: 0, Object: 0');
    assert.ok(timeline.generateTooltipContent(data.events[1]).includes('500ms'));
    assert.ok(timeline.generateTooltipContent(data.events[2]).includes('&lt;video&gt;&quot;'));
    assert.strictEqual(timeline.getEventClass(data.events[1]), 'padding-datagram stream-setting');
    timeline.showEventDetails = () => {};
    timeline.updateEventDetails(data.events[0]);
    assert.ok(details.includes('&lt;script&gt;'));
    assert.ok(!details.includes('<script>'));
});

const embeddedPath = path.resolve(__dirname, '../../../../..',
    'www/html/intern/js/mlog_viewer/MlogViewerEmbeddedScripts.js');

async function verifyScubaLoader(scripts, Parser) {
    const fixture = fs.readFileSync(path.join(__dirname, 'examples/sample_moq.mlog'), 'utf8');
    const records = new Parser().parseJSONRecords(fixture);
    const ndjson = records.reverse().map(event => JSON.stringify(event)).join('\n');
    const makeTimeline = vm.runInThisContext(`(function(document, Node, fetch, alert) {
        const window = {};
        ${scripts.MOQ_TIMELINE_CODE}
        return MoQTimeline;
    })`);
    for (const [src, dest] of [[' aa12 ', ''], ['', ' bb34 '], ['aa12', 'bb34']]) {
        const inputs = { '#src-conn-id-input': {value: src}, '#dest-conn-id-input': {value: dest} };
        const document = {
            readyState: 'loading', addEventListener() {},
            querySelectorAll(selector) { return inputs[selector] ? [inputs[selector]] : []; }
        };
        let request;
        let loaded;
        const statuses = [];
        const alerts = [];
        const Timeline = makeTimeline(document, class {}, async (url, options) => {
            request = {url, options};
            return {ok: true, async json() { return {ndjson, row_count: 20}; }};
        }, message => alerts.push(message));
        const timeline = Object.create(Timeline.prototype);
        timeline.updateStatus = status => statuses.push(status);
        timeline.loadQLogData = text => { loaded = new Parser().parseQLog(text); };
        await timeline.loadFromScubaBySrcConnId();
        const url = new URL(request.url, 'https://www.internalfb.com');
        assert.strictEqual(url.pathname, '/intern/mlog-viewer/scuba_fetch/');
        assert.strictEqual(url.searchParams.get('src_conn_id'), src.trim() || null);
        assert.strictEqual(url.searchParams.get('dest_conn_id'), dest.trim() || null);
        assert.strictEqual(request.options.credentials, 'same-origin');
        assert.strictEqual(loaded.totalEvents, 20);
        assert.deepStrictEqual(loaded.events.filter(event => event.name === 'moqt:subgroup_object_parsed')
            .map(event => event.object_id), [0, 1, 2]);
        assert.strictEqual(statuses[statuses.length - 1], 'Loaded 20 rows from Scuba');
        assert.deepStrictEqual(alerts, []);
    }
    console.log('PASS: internal Scuba loader forwards source, destination, and combined IDs and decodes unordered current MLog rows');
}

async function run() {
    const parsers = [['standalone', MoQParser]];
    let scripts;
    let EmbeddedParser;
    if (fs.existsSync(embeddedPath)) {
        scripts = require(embeddedPath);
        const coreSource = fs.readFileSync(path.join(__dirname, 'html/moq-parser.js'), 'utf8');
        assert.ok(scripts.MOQ_PARSER_CODE.startsWith(coreSource), 'Internal viewer must embed the current parser');
        EmbeddedParser = vm.runInThisContext(`(function() {
            const window = {};
            const module = {exports: {}};
            ${scripts.MOQ_PARSER_CODE}
            return window.MoQParser;
        })()`);
        parsers.push(['InternalFB', EmbeddedParser]);
    }
    for (const [label, Parser] of parsers) {
        MoQParser = Parser;
        for (const {name, body} of cases) {
            await body();
            console.log(`PASS (${label}): ${name}`);
        }
    }
    if (scripts) await verifyScubaLoader(scripts, EmbeddedParser);
    console.log(`${cases.length * parsers.length + (scripts ? 1 : 0)} tests passed`);
}

run().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
