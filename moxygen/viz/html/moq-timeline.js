/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/**
 * MoQ Timeline Visualization
 * Interactive timeline for visualizing MoQ Transport data transfers
 */

class MoQTimeline {
    constructor() {
        this.parser = new MoQParser();
        this.currentData = null;
        this.zoomLevel = 1.0;
        this.panOffset = 0;
        this.isDragging = false;
        this.dragStart = { x: 0, y: 0 };
        this.trackColors = {};
        this.filters = {
            control: true,
            objects: true,
            subgroups: true,
            fetch: true
        };
        this.viewOptions = {
            showObjectIds: true,
            showSizes: true
        };

        this.initializeUI();
        this.setupEventListeners();
    }

    /**
     * Initialize UI components
     */
    initializeUI() {
        // Clear any existing content
        $('#timeline-header').empty();
        $('#event-columns').empty();
        $('#time-labels').empty();

        this.updateStatus('Ready to load MoQ QLOG data');
    }

    /**
     * Setup event listeners for UI interactions
     */
    setupEventListeners() {
        // File upload
        $('#qlog-file-input').on('change', (e) => {
            this.handleFileUpload(e.target.files[0]);
        });

        // Load example data
        $('#load-example-btn').on('click', () => {
            this.loadExampleData();
        });

        // Filter controls
        $('#filter-control').on('change', (e) => {
            this.filters.control = e.target.checked;
            this.applyFilters();
        });

        $('#filter-objects').on('change', (e) => {
            this.filters.objects = e.target.checked;
            this.applyFilters();
        });

        $('#filter-subgroups').on('change', (e) => {
            this.filters.subgroups = e.target.checked;
            this.applyFilters();
        });

        $('#filter-fetch').on('change', (e) => {
            this.filters.fetch = e.target.checked;
            this.applyFilters();
        });

        // View options
        $('#show-object-ids').on('change', (e) => {
            this.viewOptions.showObjectIds = e.target.checked;
            this.updateEventLabels();
        });

        $('#show-sizes').on('change', (e) => {
            this.viewOptions.showSizes = e.target.checked;
            this.renderEvents();
        });

        // Reset view
        $('#reset-view-btn').on('click', () => {
            this.resetView();
        });

        // Timeline interactions
        this.setupTimelineInteractions();
    }

    /**
     * Setup timeline pan/zoom interactions
     */
    setupTimelineInteractions() {
        const $timeline = $('#timeline-div');

        // Mouse wheel: Ctrl+scroll for zoom, plain scroll for page scrolling
        $timeline.on('wheel', (e) => {
            if (e.ctrlKey || e.metaKey) {
                // Ctrl+scroll = zoom timeline (gentler zoom)
                e.preventDefault();
                const delta = e.originalEvent.deltaY;
                const zoomFactor = delta > 0 ? 0.95 : 1.05; // More gradual zoom
                this.zoom(zoomFactor, e.offsetY);
            }
            // Plain scroll = let browser handle page scrolling (don't preventDefault)
         });

        // Mouse drag pan
        $timeline.on('mousedown', (e) => {
            this.isDragging = true;
            this.dragStart = { x: e.clientX, y: e.clientY };
            $timeline.addClass('dragging');
        });

        $(document).on('mousemove', (e) => {
            if (this.isDragging && this.currentData) {
                const deltaY = e.clientY - this.dragStart.y;
                this.pan(deltaY);
                this.dragStart.y = e.clientY;
            }
        });

        $(document).on('mouseup', () => {
            this.isDragging = false;
            $timeline.removeClass('dragging');
        });

        // Double-click to center/reset (vertical timeline)
        $timeline.on('dblclick', (e) => {
            if (e.target === $timeline[0]) {
                this.resetView();
            } else {
                this.centerOnTime(e.offsetY);
            }
        });
    }

    /**
     * Handle file upload
     * @param {File} file - Uploaded QLOG file
     */
    async handleFileUpload(file) {
        if (!file) return;

        this.updateStatus('Loading QLOG file...');

        try {
            const text = await this.readFileAsText(file);
            const qlogData = JSON.parse(text);

            this.loadQLogData(qlogData);
            this.updateStatus(`Loaded ${file.name} successfully`);
        } catch (error) {
            console.error('Error loading file:', error);
            this.updateStatus(`Error loading file: ${error.message}`);
            alert(`Error loading file: ${error.message}`);
        }
    }

    /**
     * Read file as text
     * @param {File} file - File to read
     * @returns {Promise<string>} File content
     */
    readFileAsText(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = (e) => resolve(e.target.result);
            reader.onerror = (e) => reject(new Error('Failed to read file'));
            reader.readAsText(file);
        });
    }

    /**
     * Load example data for demonstration
     */
    loadExampleData() {
        this.updateStatus('Loading example data...');

        const exampleData = this.parser.generateExampleData();
        this.loadQLogData(exampleData);
        this.updateStatus('Example data loaded');
    }

    /**
     * Load and process QLOG data
     * @param {Object} qlogData - Raw QLOG data
     */
    loadQLogData(qlogData) {
        try {
            this.currentData = this.parser.parseQLog(qlogData);
            this.generateTrackColors();
            this.renderTimeline();
            this.updateEventCount();
        } catch (error) {
            console.error('Error processing QLOG data:', error);
            this.updateStatus(`Error processing data: ${error.message}`);
            throw error;
        }
    }

    /**
     * Generate distinct colors for tracks
     */
    generateTrackColors() {
        const trackIds = Object.keys(this.currentData.tracks);

        // Generate more distinct colors using HSL color space
        const colors = [];
        const hueStep = 360 / Math.max(trackIds.length, 4); // Ensure at least 4 colors

        trackIds.forEach((trackId, index) => {
            const hue = (index * hueStep) % 360;
            const saturation = 70 + (index % 3) * 10; // 70%, 80%, 90%
            const lightness = 45 + (index % 2) * 10;  // 45%, 55%
            const color = `hsl(${hue}, ${saturation}%, ${lightness}%)`;

            this.trackColors[trackId] = color;
            colors.push(color);
        });

        // Apply colors to CSS custom properties for dynamic styling
        const root = document.documentElement;
        trackIds.forEach((trackId, index) => {
            const track = this.currentData.tracks[trackId];
            if (track && track.className) {
                root.style.setProperty(`--track-${track.className}-color`, this.trackColors[trackId]);
            }
        });
    }

    /**
     * Render the complete timeline
     */
    renderTimeline() {
        if (!this.currentData) return;

        this.renderColumnHeaders();
        this.renderTimeLabels();
        this.renderEvents();
    }

    /**
     * Render column headers with true left/right client/server division
     */
    renderColumnHeaders() {
        const $header = $('#timeline-header');
        const $columns = $('#event-columns');
        $header.empty();
        $columns.empty();

        const trackIds = Object.keys(this.currentData.tracks);

        // Add Time header to align with time labels column
        $header.append(`
            <div class="column-header time-header">
                <div class="track-name">Time</div>
            </div>
        `);

        // =================
        // LEFT HALF: ALL CLIENT COLUMNS
        // =================

        // Client control column
        $header.append(`
            <div class="column-header control-column client-half">
                <div class="endpoint-label">Client</div>
                <div class="track-name">Control</div>
            </div>
        `);
        $columns.append('<div class="event-column control-column client-control client-half" data-column="client-control"></div>');

        // All client track columns
        trackIds.forEach(trackId => {
            const track = this.currentData.tracks[trackId];
            if (track.client_events && track.client_events.length > 0) {
                $header.append(`
                    <div class="column-header track-column client-half">
                        <div class="endpoint-label">Client</div>
                        <div class="track-name">${track.name}</div>
                    </div>
                `);
                $columns.append(`<div class="event-column track-column client-${track.className} client-half" data-column="client-${track.className}" data-track="${trackId}"></div>`);
            }
        });

        // Add the middle divider column
        $header.append('<div class="column-header divider-column"><div class="divider-line"></div></div>');
        $columns.append('<div class="event-column divider-column"><div class="center-divider"></div></div>');

        // =================
        // RIGHT HALF: ALL SERVER COLUMNS
        // =================

        // Server control column
        $header.append(`
            <div class="column-header control-column server-half">
                <div class="endpoint-label">Server</div>
                <div class="track-name">Control</div>
            </div>
        `);
        $columns.append('<div class="event-column control-column server-control server-half" data-column="server-control"></div>');

        // All server track columns
        trackIds.forEach(trackId => {
            const track = this.currentData.tracks[trackId];
            if (track.server_events && track.server_events.length > 0) {
                $header.append(`
                    <div class="column-header track-column server-half">
                        <div class="endpoint-label">Server</div>
                        <div class="track-name">${track.name}</div>
                    </div>
                `);
                $columns.append(`<div class="event-column track-column server-${track.className} server-half" data-column="server-${track.className}" data-track="${trackId}"></div>`);
            }
        });
    }

    /**
     * Render time labels on the left side
     */
    renderTimeLabels() {
        const $timeLabels = $('#time-labels');
        $timeLabels.empty();

        if (!this.currentData || this.currentData.events.length === 0) return;

        const timelineDuration = this.currentData.timelineBounds.duration;

        // Simple linear time-based positioning
        const baseTimelineHeight = Math.max(2000, timelineDuration * 2);
        const timelineHeight = baseTimelineHeight * this.zoomLevel;

        // Set height of timeline containers
        $('.event-column').css('min-height', `${timelineHeight}px`);
        $timeLabels.css('min-height', `${timelineHeight}px`);

        // Simple time labels every 200ms (0.2 seconds)
        const interval = 0.2;

        // Generate time labels with zoom and pan awareness
        const visibleStart = timelineDuration * (-this.panOffset) / this.zoomLevel;
        const visibleEnd = visibleStart + (timelineDuration / this.zoomLevel);

        // Start slightly before visible area and end slightly after
        const startTime = Math.max(0, Math.floor(visibleStart / interval) * interval - interval);
        const endTime = Math.min(timelineDuration, Math.ceil(visibleEnd / interval) * interval + interval);

        for (let time = startTime; time <= endTime; time += interval) {
            // Simple linear time positioning to match object positioning
            const timePosition = time / timelineDuration;
            const topPosition = (timePosition * timelineHeight) + (this.panOffset * timelineHeight);

            // Only show labels that are actually visible
            if (topPosition >= -50 && topPosition <= timelineHeight + 50) {
                const $label = $(`
                    <div class="time-label" style="top: ${topPosition}px">
                        ${this.formatTime(time * 1000)}
                    </div>
                `);
                $timeLabels.append($label);
            }
        }
    }

    /**
     * Render event bars
     */
    renderEvents() {
        if (!this.currentData) return;

        // Clear all event columns
        $('.event-column').empty();

        const timelineDuration = this.currentData.timelineBounds.duration;
        const baseTimelineHeight = Math.max(2000, timelineDuration * 2);
        const timelineHeight = baseTimelineHeight * this.zoomLevel;

        // Update container height to accommodate zoom
        $('.event-column').css('height', `${timelineHeight}px`);

        // Group events by column to handle overlapping
        const eventsByColumn = {};
        this.currentData.events.forEach(event => {
            const columnKey = this.getEventColumnKey(event);
            if (!eventsByColumn[columnKey]) {
                eventsByColumn[columnKey] = [];
            }
            eventsByColumn[columnKey].push(event);
        });

        // Render events with overlap prevention
        Object.keys(eventsByColumn).forEach(columnKey => {
            this.renderEventsInColumn(eventsByColumn[columnKey], timelineHeight, timelineDuration);
        });

        this.applyFilters();
    }

    /**
     * Get column key for event (for overlap prevention)
     * @param {Object} event - Event data
     * @returns {string} Column key
     */
    getEventColumnKey(event) {
        if (event.event_category === 'control') {
            return event.vantage_point === 'client' ? 'client-control' : 'server-control';
        } else {
            const trackId = event.track_id || 'unknown';
            const track = this.currentData.tracks[trackId];
            const className = track ? track.className : 'unknown';
            return event.vantage_point === 'client' ? `client-${className}` : `server-${className}`;
        }
    }



    /**
     * Render events in a column with overlap prevention using time-scaled positions
     * @param {Array} events - Events for this column
     * @param {number} timelineHeight - Timeline height
     * @param {number} timelineDuration - Timeline duration
     */
    renderEventsInColumn(events, timelineHeight, timelineDuration) {
        if (timelineDuration === 0) return;

        // Sort events by time for consistent z-index ordering
        events.sort((a, b) => a.relative_time - b.relative_time);

        events.forEach((event, index) => {
            // Simple Y position based on relative time
            const timePosition = (event.relative_time / timelineDuration) * timelineHeight;
            const top = timePosition + (this.panOffset * timelineHeight);

            // Calculate proportional height based on byte count
            let height = 20; // Base height
            if (this.viewOptions.showSizes && event.object_size > 0) {
                height = Math.max(20, Math.min(200, event.object_size / 256));
            }
            // Keep objects same size - only positions spread apart with zoom

            // Z-index based on render order (later events on top)
            const zIndex = 100 + index;

            // Render the event - objects will overlap naturally
            this.renderEventBarWithOffset(event, top, height, 0, zIndex);
        });
    }

    /**
     * Render individual event bar with horizontal offset
     * @param {Object} event - Event data
     * @param {number} top - Top position in pixels
     * @param {number} height - Height in pixels
     * @param {number} horizontalOffset - Horizontal offset for overlap prevention
     * @param {number} zIndex - Z-index for stacking order
     */
    renderEventBarWithOffset(event, top, height, horizontalOffset, zIndex = 1) {
        // Determine column selector
        const columnKey = this.getEventColumnKey(event);
        const columnSelector = `.${columnKey}`;

        const $container = $(columnSelector);
        if ($container.length === 0) {
            console.warn(`No column found for selector: ${columnSelector}`, event);
            return;
        }

        // Determine event class for styling
        const eventClass = this.getEventClass(event);

        // Create label with Group and Object IDs (explicitly handle 0 values)
        let label = '';
        if (this.viewOptions.showObjectIds) {
            const hasGroupId = (event.group_id !== null && event.group_id !== undefined);
            const hasObjectId = (event.object_id !== null && event.object_id !== undefined);

            if (hasGroupId && hasObjectId) {
                label = `Group: ${event.group_id}, Object: ${event.object_id}`;
            } else if (hasGroupId) {
                label = `Group: ${event.group_id}`;
            } else if (hasObjectId) {
                label = `Object: ${event.object_id}`;
            } else if (event.control_message_type) {
                label = event.control_message_type;
            }
        }

        const $eventBar = $(`
            <div class="event-bar ${eventClass}"
                 data-event-id="${event.id}"
                 style="top: ${top}px; height: ${height}px; z-index: ${zIndex};">
                ${label}
            </div>
        `);

        // Store original z-index for hover restoration
        $eventBar.data('original-zindex', zIndex);

        // Add hover and click handlers
        $eventBar.on('mouseenter', (e) => {
            // Bring hovered object to top
            $eventBar.css('z-index', 9999);
            this.showEventTooltip(event, e.pageX, e.pageY);
        }).on('mouseleave', () => {
            // Restore original z-index
            $eventBar.css('z-index', $eventBar.data('original-zindex'));
            this.hideEventTooltip();
        }).on('click', () => {
            this.updateEventDetails(event);
        });

        $container.append($eventBar);
    }



    /**
     * Get CSS class for event based on type
     * @param {Object} event - Event data
     * @returns {string} CSS class name
     */
    getEventClass(event) {
        let baseClass = '';
        switch (event.event_category) {
            case 'control': baseClass = 'control-message'; break;
            case 'object': baseClass = 'object-datagram'; break;
            case 'subgroup': baseClass = 'subgroup-operation'; break;
            case 'fetch': baseClass = 'fetch-operation'; break;
            case 'stream': baseClass = 'stream-setting'; break;
            default: baseClass = 'unknown-event'; break;
        }

        // Add track-specific class for color coding
        if (event.track_id && this.currentData.tracks[event.track_id]) {
            const track = this.currentData.tracks[event.track_id];
            baseClass += ` track-${track.className}`;
        }

        return baseClass;
    }

    /**
     * Apply current filters to events
     */
    applyFilters() {
        $('.event-bar').removeClass('filtered');

        if (!this.filters.control) {
            $('.control-message').addClass('filtered');
        }
        if (!this.filters.objects) {
            $('.object-datagram').addClass('filtered');
        }
        if (!this.filters.subgroups) {
            $('.subgroup-operation').addClass('filtered');
        }
        if (!this.filters.fetch) {
            $('.fetch-operation').addClass('filtered');
        }
    }

    /**
     * Update event labels based on view options
     */
    updateEventLabels() {
        // Re-render events to update labels
        this.renderEvents();
    }

    /**
     * Zoom timeline vertically
     * @param {number} factor - Zoom factor
     * @param {number} centerY - Center point for zoom
     */
    zoom(factor, centerY = 0) {
        const oldZoom = this.zoomLevel;
        this.zoomLevel *= factor;
        this.zoomLevel = Math.max(0.1, Math.min(100, this.zoomLevel)); // Allow much more zoom in

        // Adjust pan to keep zoom centered vertically
        const containerHeight = $('#timeline-div').height();
        if (containerHeight > 0) {
            this.panOffet = (this.panOffset * factor) - (centerY / containerHeight) * (1 - factor);
            console.log(this.panOffset);
            // Apply boundary constraints after zoom adjustment (relaxed for better UX)
            const maxPan = 0.1; // Allow slight scroll past start
            const minPan = -1.1; // Allow slight scroll past end

            // Only apply constraints if significantly outside bounds
            if (this.panOffset > maxPan) {
                this.panOffset = maxPan;
            } else if (this.panOffset < minPan) {
                this.panOffset = minPan;
            }
        }

        this.applyTransform();
        this.renderTimeLabels();
    }

    /**
     * Pan timeline vertically
     * @param {number} deltaY - Pan distance in pixels
     */
    pan(deltaY) {
        const containerHeight = $('#timeline-div').height();
        if (containerHeight > 0) {
            this.panOffset += deltaY / (containerHeight * this.zoomLevel);
            console.log(this.panOffset);
            // Apply boundary constraints only if we're actually near the boundaries
            // This prevents interference with normal scrolling behavior
            const maxPan = 0.1; // Allow slight scroll past start for better UX
            const minPan = -1.1; // Allow slight scroll past end

            // Only apply constraints if we're significantly outside bounds
            if (this.panOffset > maxPan) {
                this.panOffset = maxPan;
            } else if (this.panOffset < minPan) {
                this.panOffset = minPan;
            }
        }

        this.applyTransform();
        this.renderTimeLabels();
    }

    /**
     * Apply zoom and pan transform to events (vertical timeline)
     * Now directly modifies positions and heights instead of CSS transform
     */
    applyTransform() {
        // Re-render all events with new zoom/pan settings
        this.renderEvents();
        this.updateTimeLabels();

        // Fix scroll interaction after zoom by forcing scroll area recalculation
        const $timeline = $('#timeline-div');
        if ($timeline.length) {
            // Temporarily disable and re-enable scrolling to reset scroll state
            const currentScrollTop = $timeline.scrollTop();
            $timeline.css('overflow', 'hidden').css('overflow', 'auto');
            $timeline.scrollTop(currentScrollTop);
        }
    }

    /**
     * Reset view to default zoom and pan
     */
    resetView() {
        this.zoomLevel = 1.0;
        this.panOffset = 0;
        this.applyTransform();
        this.updateTimeLabels();
    }

    /**
     * Center timeline on specific time position (vertical)
     * @param {number} y - Y position in pixels
     */
    centerOnTime(y) {
        const containerHeight = $('#timeline-div').height();
        if (containerHeight > 0) {
            const centerRatio = y / containerHeight;
            this.panOffset = 0.5 - centerRatio / this.zoomLevel;
            this.applyTransform();
            this.renderTimeLabels();
        }
    }

    /**
     * Update time labels on timeline
     */
    updateTimeLabels() {
        if (!this.currentData) return;

        const duration = this.currentData.timelineBounds.duration;
        const startTime = this.currentData.timelineBounds.start;

        // Calculate visible time range
        const visibleStart = startTime + (duration * (-this.panOffset) / this.zoomLevel);
        const visibleEnd = visibleStart + (duration / this.zoomLevel);
        const visibleCenter = (visibleStart + visibleEnd) / 2;

        // Update labels
        $('#left-time .tick-label').text(this.formatTime(visibleStart));
        $('#center-time .tick-label').text(this.formatTime(visibleCenter));
        $('#right-time .tick-label').text(this.formatTime(visibleEnd));
    }

    /**
     * Format time for display
     * @param {number} timeMs - Time in milliseconds
     * @returns {string} Formatted time string
     */
    formatTime(timeMs) {
        if (timeMs < 1000) {
            return `${timeMs.toFixed(0)}ms`;
        } else if (timeMs < 60000) {
            return `${(timeMs / 1000).toFixed(2)}s`;
        } else {
            const minutes = Math.floor(timeMs / 60000);
            const seconds = ((timeMs % 60000) / 1000).toFixed(1);
            return `${minutes}:${seconds.padStart(4, '0')}`;
        }
    }

    /**
     * Show event tooltip
     * @param {Event} e - Mouse event
     * @param {Object} eventData - Event data
     */
    showEventTooltip(e, eventData) {
        const $tooltip = $('#event-tooltip');
        const content = this.generateTooltipContent(eventData);

        $tooltip.find('.tooltip-content').html(content);
        $tooltip.css({
            left: e.pageX + 10,
            top: e.pageY - 10
        }).addClass('show');
    }

    /**
     * Hide event tooltip
     */
    hideEventTooltip() {
        $('#event-tooltip').removeClass('show');
    }

    /**
     * Generate tooltip content
     * @param {Object} event - Event data
     * @returns {string} HTML content
     */
    generateTooltipContent(event) {
        let content = `
            <strong>${event.name}</strong><br>
            <strong>Time:</strong> ${this.formatTime(event.relative_time * 1000)}<br>
            <strong>Vantage:</strong> ${event.vantage_point}<br>
        `;

        if (event.track_id) {
            content += `<strong>Track:</strong> ${event.track_id}<br>`;
        }
        if (event.group_id !== null) {
            content += `<strong>Group:</strong> ${event.group_id}<br>`;
        }
        if (event.subgroup_id !== null) {
            content += `<strong>Subgroup:</strong> ${event.subgroup_id}<br>`;
        }
        if (event.object_id !== null) {
            content += `<strong>Object:</strong> ${event.object_id}<br>`;
        }
        if (event.object_size > 0) {
            content += `<strong>Size:</strong> ${event.object_size} bytes<br>`;
        }
        if (event.control_message_type) {
            content += `<strong>Type:</strong> ${event.control_message_type}<br>`;
        }

        return content;
    }

    /**
     * Update event details panel
     * @param {Object} event - Event data
     */
    updateEventDetails(event) {
        const content = `
            <h4>${event.name}</h4>
            <p><strong>Timestamp:</strong> ${this.formatTime(event.relative_time * 1000)}</p>
            <p><strong>Vantage Point:</strong> ${event.vantage_point}</p>
            <p><strong>Category:</strong> ${event.event_category}</p>
            ${event.track_id ? `<p><strong>Track ID:</strong> ${event.track_id}</p>` : ''}
            ${event.group_id !== null ? `<p><strong>Group ID:</strong> ${event.group_id}</p>` : ''}
            ${event.subgroup_id !== null ? `<p><strong>Subgroup ID:</strong> ${event.subgroup_id}</p>` : ''}
            ${event.object_id !== null ? `<p><strong>Object ID:</strong> ${event.object_id}</p>` : ''}
            ${event.object_size > 0 ? `<p><strong>Object Size:</strong> ${event.object_size} bytes</p>` : ''}
            ${event.control_message_type ? `<p><strong>Message Type:</strong> ${event.control_message_type}</p>` : ''}
            <details>
                <summary>Raw Event Data</summary>
                <pre>${JSON.stringify(event.data, null, 2)}</pre>
            </details>
        `;

        $('#event-details .details-content').html(content);
    }

    /**
     * Update status message
     * @param {string} message - Status message
     */
    updateStatus(message) {
        $('#status-text').text(message);
    }

    /**
     * Update event count display
     */
    updateEventCount() {
        if (this.currentData) {
            const stats = this.currentData.eventStats;
            let countText = `${stats.total} events`;

            if (stats.by_vantage_point.client > 0 || stats.by_vantage_point.server > 0) {
                countText += ` (${stats.by_vantage_point.client} client, ${stats.by_vantage_point.server} server)`;
            }

            $('#event-count').text(countText);
        } else {
            $('#event-count').text('');
        }
    }
}

// Initialize when document is ready
$(document).ready(() => {
    window.moqTimeline = new MoQTimeline();
});
