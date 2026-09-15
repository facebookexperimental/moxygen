/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

/**
 * Minimal jQuery-compatible helper for vanilla JavaScript
 * Implements only the methods used in this file
 */
const $ = (function () {
  // Minimal jQuery-like data storage for elements (supports dashed keys)
  const elementDataStore = new WeakMap();
  const getElementDataMap = el => {
    let map = elementDataStore.get(el);
    if (map == null) {
      map = new Map();
      elementDataStore.set(el, map);
    }
    return map;
  };

  // Minimal event wrapper to emulate the parts of jQuery's event object that
  // this visualization relies on.
  //
  // Important: do NOT use Object.create(e) here. It produces an object that is
  // *not* a real DOM Event, which can make native methods like
  // stopPropagation() / preventDefault() throw "Illegal invocation".
  //
  // Instead, create a plain wrapper with bound methods and copied fields.
  const wrapEvent = e => {
    if (e == null || e.originalEvent != null) {
      return e;
    }

    return {
      originalEvent: e,
      target: e.target,
      currentTarget: e.currentTarget,
      ctrlKey: e.ctrlKey,
      metaKey: e.metaKey,
      offsetX: e.offsetX,
      offsetY: e.offsetY,
      clientX: e.clientX,
      clientY: e.clientY,
      pageX: e.pageX,
      pageY: e.pageY,
      preventDefault: () => e.preventDefault(),
      stopPropagation: () => e.stopPropagation(),
    };
  };

  class DOMWrapper {
    constructor(elements) {
      this.elements = elements;
      this.length = elements.length;
    }

    // Iterate over elements
    each(callback) {
      this.elements.forEach((el, i) => callback.call(el, i, el));
      return this;
    }

    // Get/set text content
    text(value) {
      if (value === undefined) {
        return this.elements[0]?.textContent || '';
      }
      this.elements.forEach(el => {
        el.textContent = value;
      });
      return this;
    }

    // Get/set HTML content
    html(value) {
      if (value === undefined) {
        return this.elements[0]?.innerHTML || '';
      }
      this.elements.forEach(el => {
        el.innerHTML = value;
      });
      return this;
    }

    // Clear content
    empty() {
      this.elements.forEach(el => {
        el.innerHTML = '';
      });
      return this;
    }

    // Append child element or HTML
    append(content) {
      this.elements.forEach(el => {
        if (typeof content === 'string') {
          el.insertAdjacentHTML('beforeend', content);
        } else if (content instanceof DOMWrapper) {
          content.elements.forEach(child => el.appendChild(child));
        } else if (content instanceof Node) {
          el.appendChild(content);
        }
      });
      return this;
    }

    // Add event listener
    on(event, selectorOrHandler, handler) {
      const wrapHandler = fn => {
        return function (e) {
          return fn.call(this, wrapEvent(e));
        };
      };

      if (typeof selectorOrHandler === 'function') {
        // Direct event binding
        this.elements.forEach(el => {
          el.addEventListener(event, wrapHandler(selectorOrHandler));
        });
      } else {
        // Delegated event binding (simplified)
        this.elements.forEach(el => {
          el.addEventListener(
            event,
            wrapHandler(e => {
              if (e.target.matches(selectorOrHandler)) {
                handler.call(e.target, e);
              }
            }),
          );
        });
      }
      return this;
    }

    // Set CSS property
    css(prop, value) {
      const setStyle = (el, key, val) => {
        if (key.includes('-')) {
          // e.g. min-height, z-index
          el.style.setProperty(key, String(val));
        } else {
          // e.g. top, height
          el.style[key] = val;
        }
      };

      if (typeof prop === 'object') {
        this.elements.forEach(el => {
          Object.keys(prop).forEach(key => {
            setStyle(el, key, prop[key]);
          });
        });
      } else {
        this.elements.forEach(el => {
          setStyle(el, prop, value);
        });
      }
      return this;
    }

    // Add class
    addClass(className) {
      this.elements.forEach(el => {
        el.classList.add(className);
      });
      return this;
    }

    // Remove class
    removeClass(className) {
      this.elements.forEach(el => {
        el.classList.remove(className);
      });
      return this;
    }

    // Toggle class
    toggleClass(className, force) {
      this.elements.forEach(el => {
        el.classList.toggle(className, force);
      });
      return this;
    }

    // Check if has class
    hasClass(className) {
      return this.elements[0]?.classList.contains(className) || false;
    }

    // Get/set attribute
    attr(name, value) {
      if (value === undefined) {
        return this.elements[0]?.getAttribute(name);
      }
      this.elements.forEach(el => {
        el.setAttribute(name, value);
      });
      return this;
    }

    // Get/set data attribute (jQuery-like)
    //
    // jQuery's .data() accepts keys like "track-id" or "original-zindex".
    // The DOM dataset API does not: it only supports camelCased keys.
    //
    // For compatibility with the upstream moxygen viz (which uses jQuery), we:
    // - read from data-* attributes when possible
    // - store arbitrary keys in a WeakMap-backed per-element Map
    data(name, value) {
      const el = this.elements[0];
      if (!el) {
        return value === undefined ? undefined : this;
      }

      const dataMap = getElementDataMap(el);

      if (value === undefined) {
        // Prefer explicitly stored values.
        if (dataMap.has(name)) {
          return dataMap.get(name);
        }

        // Fall back to data-* attribute.
        // For name "track-id" => attribute "data-track-id"
        const attrVal = el.getAttribute('data-' + name);
        if (attrVal != null) {
          return attrVal;
        }

        // Fall back to dataset for camelCase access.
        // For name "track-id" => dataset.trackId
        const camelName = name.replace(/-([a-z])/g, (_, c) => c.toUpperCase());
        return el.dataset?.[camelName];
      }

      this.elements.forEach(elem => {
        getElementDataMap(elem).set(name, value);
      });
      return this;
    }

    // Find descendant elements
    find(selector) {
      const results = [];
      this.elements.forEach(el => {
        results.push(...el.querySelectorAll(selector));
      });
      return new DOMWrapper(results);
    }

    // Get closest ancestor matching selector
    closest(selector) {
      const el = this.elements[0]?.closest(selector);
      return new DOMWrapper(el ? [el] : []);
    }

    // Get parent element
    parent() {
      const parents = this.elements
        .map(el => el.parentElement)
        .filter(el => el);
      return new DOMWrapper(parents);
    }

    // Show element
    show() {
      this.elements.forEach(el => {
        el.style.display = '';
      });
      return this;
    }

    // Hide element
    hide() {
      this.elements.forEach(el => {
        el.style.display = 'none';
      });
      return this;
    }

    // Get first element
    get(index) {
      return this.elements[index];
    }

    // Check if element exists
    is(selector) {
      return this.elements[0]?.matches(selector) || false;
    }

    // Scroll position
    scrollTop(value) {
      if (value === undefined) {
        return this.elements[0]?.scrollTop || 0;
      }
      this.elements.forEach(el => {
        el.scrollTop = value;
      });
      return this;
    }

    scrollLeft(value) {
      if (value === undefined) {
        return this.elements[0]?.scrollLeft || 0;
      }
      this.elements.forEach(el => {
        el.scrollLeft = value;
      });
      return this;
    }

    // Dimensions
    width() {
      return this.elements[0]?.clientWidth || 0;
    }

    height() {
      return this.elements[0]?.clientHeight || 0;
    }

    // Offset
    offset() {
      const el = this.elements[0];
      if (!el) {
        return {top: 0, left: 0};
      }
      const rect = el.getBoundingClientRect();
      return {
        top: rect.top + window.scrollY,
        left: rect.left + window.scrollX,
      };
    }

    // Position relative to offset parent
    position() {
      const el = this.elements[0];
      if (!el) {
        return {top: 0, left: 0};
      }
      return {
        top: el.offsetTop,
        left: el.offsetLeft,
      };
    }

    // Get value (for inputs)
    val(value) {
      if (value === undefined) {
        return this.elements[0]?.value;
      }
      this.elements.forEach(el => {
        el.value = value;
      });
      return this;
    }

    // Prop (for checkboxes, etc.)
    prop(name, value) {
      if (value === undefined) {
        return this.elements[0]?.[name];
      }
      this.elements.forEach(el => {
        el[name] = value;
      });
      return this;
    }

    // Trigger event
    trigger(eventName) {
      this.elements.forEach(el => {
        el.dispatchEvent(new Event(eventName, {bubbles: true}));
      });
      return this;
    }

    // Stop propagation helper for event object
    stopPropagation() {
      // This is called on jQuery event wrapper, handled in on()
      return this;
    }

    // Document ready handler (jQuery-compatible)
    // If the document is already loaded, the callback fires immediately.
    ready(callback) {
      if (
        document.readyState === 'complete' ||
        document.readyState === 'interactive'
      ) {
        setTimeout(callback, 0);
      } else {
        document.addEventListener('DOMContentLoaded', callback);
      }
      return this;
    }
  }

  // Main $ function
  function $(selector) {
    if (typeof selector === 'string') {
      if (selector.trim().startsWith('<')) {
        // Create element from HTML string
        const template = document.createElement('template');
        template.innerHTML = selector.trim();
        return new DOMWrapper([...template.content.children]);
      } else {
        // Query selector
        const elements = document.querySelectorAll(selector);
        return new DOMWrapper([...elements]);
      }
    } else if (selector instanceof Node) {
      return new DOMWrapper([selector]);
    } else if (selector instanceof DOMWrapper) {
      return selector;
    } else if (selector === document) {
      return new DOMWrapper([document]);
    }
    return new DOMWrapper([]);
  }

  // Static methods
  $.each = function (obj, callback) {
    if (Array.isArray(obj)) {
      obj.forEach((item, i) => callback(i, item));
    } else {
      Object.keys(obj).forEach(key => callback(key, obj[key]));
    }
  };

  return $;
})();

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
        this.trackVisibility = {
            client: {}, // trackId: boolean (checked/unchecked)
            server: {}  // trackId: boolean (checked/unchecked)
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

        // Click outside to hide event details
        $(document).on('click', (e) => {
            // Don't hide if clicking on event details panel itself or an event bar
            if (!$(e.target).closest('#event-details, .event-bar').length) {
                this.hideEventDetails();
            }
        });

        // Prevent clicks on event details panel from propagating
        $('#event-details').on('click', (e) => {
            e.stopPropagation();
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
     * Handle file upload.
     * Expects NDJSON format (newline-separated JSON objects, as written by FileMLogger).
     * @param {File} file - Uploaded MoQ log file
     */
    async handleFileUpload(file) {
        if (!file) return;

        this.updateStatus('Loading MoQ log file...');

        try {
            const text = await this.readFileAsText(file);

            // Pass raw text directly - parser handles NDJSON format
            this.loadQLogData(text);
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
        this.loadQLogData(exampleData.join('\n'));
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
            this.renderTrackSelectionLists();
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
     * Render track selection lists with checkboxes
     */
    renderTrackSelectionLists() {
        const $clientList = $('#client-tracks-list');
        const $serverList = $('#server-tracks-list');
        $clientList.empty();
        $serverList.empty();

        const trackIds = Object.keys(this.currentData.tracks);

        trackIds.forEach(trackId => {
            const track = this.currentData.tracks[trackId];

            // Add to client list if track has client events (server subscribed)
            if (track.client_events && track.client_events.length > 0) {
                // Initialize track visibility to true (checked) so events are visible by default
                if (this.trackVisibility.client[trackId] === undefined) {
                    this.trackVisibility.client[trackId] = true;
                }

                const clientFullName = track.fullName || track.name;
                const $checkbox = $(`
                    <label>
                        <input type="checkbox"
                               class="track-visibility-toggle"
                               data-vantage="client"
                               data-track-id="${trackId}"
                               ${this.trackVisibility.client[trackId] ? 'checked' : ''}>
                        <span class="track-name"${clientFullName !== track.name ? ' data-full-name="' + clientFullName + '"' : ''}>${track.name}</span>
                    </label>
                `);
                $clientList.append($checkbox);
            }

            // Add to server list if track has server events (client subscribed)
            if (track.server_events && track.server_events.length > 0) {
                // Initialize track visibility to true (checked) so events are visible by default
                if (this.trackVisibility.server[trackId] === undefined) {
                    this.trackVisibility.server[trackId] = true;
                }

                const serverFullName = track.fullName || track.name;
                const $checkbox = $(`
                    <label>
                        <input type="checkbox"
                               class="track-visibility-toggle"
                               data-vantage="server"
                               data-track-id="${trackId}"
                               ${this.trackVisibility.server[trackId] ? 'checked' : ''}>
                        <span class="track-name"${serverFullName !== track.name ? ' data-full-name="' + serverFullName + '"' : ''}>${track.name}</span>
                    </label>
                `);
                $serverList.append($checkbox);
            }
        });

        // Add event listeners for checkboxes
        $('.track-visibility-toggle').on('change', (e) => {
            const $checkbox = $(e.target);
            const vantage = $checkbox.data('vantage');
            const trackId = $checkbox.data('track-id');
            const isChecked = $checkbox.is(':checked');

            this.trackVisibility[vantage][trackId] = isChecked;

            // Re-render timeline to show/hide columns
            this.renderTimeline();
        });

        // Attach hover tooltips for truncated track names in checkboxes
        this.attachTrackNameTooltips();
    }

    /**
     * Attach mouseover/mouseout/mousemove handlers via event delegation
     * on the viewer root so the tooltip works for any element with a
     * data-full-name attribute, including elements added dynamically
     * after column header re-renders. Attaches only once (guarded).
     */
    attachTrackNameTooltips() {
        if (this._trackTipAttached) return;
        this._trackTipAttached = true;

        var tipEl = document.getElementById('track-name-tooltip');
        if (!tipEl) return;
        var root = document.querySelector('.mlog-viewer-root');
        if (!root) return;

        var currentTarget = null;

        // mouseover bubbles (unlike mouseenter), so delegation works.
        root.addEventListener('mouseover', function (e) {
            var el = e.target.closest ? e.target.closest('[data-full-name]') : null;
            if (el && el !== currentTarget) {
                currentTarget = el;
                tipEl.textContent = el.getAttribute('data-full-name');
                tipEl.classList.add('show');
                tipEl.style.left = e.clientX + 12 + 'px';
                tipEl.style.top = e.clientY - 8 + 'px';
            }
        });

        root.addEventListener('mousemove', function (e) {
            if (currentTarget) {
                tipEl.style.left = e.clientX + 12 + 'px';
                tipEl.style.top = e.clientY - 8 + 'px';
            }
        });

        // mouseout bubbles (unlike mouseleave), so delegation works.
        root.addEventListener('mouseout', function (e) {
            if (!currentTarget) return;
            var related = e.relatedTarget;
            // Hide only when the cursor moves outside the current target.
            if (!related || !currentTarget.contains(related)) {
                currentTarget = null;
                tipEl.classList.remove('show');
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
     * Render column headers with client control first, server control second
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
        // COLUMN 1: CLIENT CONTROL (always first)
        // =================
        $header.append(`
            <div class="column-header control-column client-control-col">
                <div class="endpoint-label">Client</div>
                <div class="track-name">Control</div>
            </div>
        `);
        $columns.append('<div class="event-column control-column client-control client-control-col" data-column="client-control"></div>');

        // =================
        // COLUMN 2: SERVER CONTROL (always second)
        // =================
        $header.append(`
            <div class="column-header control-column server-control-col">
                <div class="endpoint-label">Server</div>
                <div class="track-name">Control</div>
            </div>
        `);
        $columns.append('<div class="event-column control-column server-control server-control-col" data-column="server-control"></div>');

        // =================
        // REMAINING COLUMNS: CLIENT TRACKS
        // =================
        trackIds.forEach(trackId => {
            const track = this.currentData.tracks[trackId];
            // Only show track column if checkbox is checked
            if (track.client_events && track.client_events.length > 0 && this.trackVisibility.client[trackId]) {
                const cFullName = track.fullName || track.name;
                $header.append(`
                    <div class="column-header track-column client-half">
                        <div class="endpoint-label">Client</div>
                        <div class="track-name"${cFullName !== track.name ? ' data-full-name="' + cFullName + '"' : ''}>${track.name}</div>
                    </div>
                `);
                $columns.append(`<div class="event-column track-column client-${track.className} client-half" data-column="client-${track.className}" data-track="${trackId}"></div>`);
            }
        });

        // =================
        // OPTIONAL: Divider between client and server tracks
        // =================
        // Check if there are both visible client and server tracks to show
        const hasClientTracks = trackIds.some(trackId =>
            this.currentData.tracks[trackId].client_events &&
            this.currentData.tracks[trackId].client_events.length > 0 &&
            this.trackVisibility.client[trackId]
        );
        const hasServerTracks = trackIds.some(trackId =>
            this.currentData.tracks[trackId].server_events &&
            this.currentData.tracks[trackId].server_events.length > 0 &&
            this.trackVisibility.server[trackId]
        );

        if (hasClientTracks && hasServerTracks) {
            $header.append('<div class="column-header divider-column"><div class="divider-line"></div></div>');
            $columns.append('<div class="event-column divider-column"><div class="center-divider"></div></div>');
        }

        // =================
        // REMAINING COLUMNS: SERVER TRACKS
        // =================
        trackIds.forEach(trackId => {
            const track = this.currentData.tracks[trackId];
            // Only show track column if checkbox is checked
            if (track.server_events && track.server_events.length > 0 && this.trackVisibility.server[trackId]) {
                const sFullName = track.fullName || track.name;
                $header.append(`
                    <div class="column-header track-column server-half">
                        <div class="endpoint-label">Server</div>
                        <div class="track-name"${sFullName !== track.name ? ' data-full-name="' + sFullName + '"' : ''}>${track.name}</div>
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

        // Scale timeline height based on event count for appropriate spacing
        const numEvents = this.currentData.events.length;
        const baseTimelineHeight = Math.min(20000, Math.max(2000, numEvents * 150));
        const timelineHeight = baseTimelineHeight * this.zoomLevel;

        // Set height of timeline containers
        $('.event-column').css('min-height', `${timelineHeight}px`);
        $timeLabels.css('min-height', `${timelineHeight}px`);

        // Adaptive time label interval: aim for ~20-30 labels
        const rawInterval = timelineDuration / 25;
        const magnitude = Math.pow(10, Math.floor(Math.log10(Math.max(rawInterval, 0.001))));
        const normalized = rawInterval / magnitude;
        let niceInterval;
        if (normalized < 1.5) niceInterval = 1;
        else if (normalized < 3.5) niceInterval = 2;
        else if (normalized < 7.5) niceInterval = 5;
        else niceInterval = 10;
        const interval = niceInterval * magnitude;

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
                        ${this.formatTime(time)}
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
        // Scale timeline height based on event count for appropriate spacing
        const numEvents = this.currentData.events.length;
        const baseTimelineHeight = Math.min(20000, Math.max(2000, numEvents * 150));
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
            // Parsed messages originated from the opposite side, so flip the column
            const isParsed = event.name && event.name.includes('parsed');
            const side = isParsed
                ? (event.vantage_point === 'client' ? 'server' : 'client')
                : event.vantage_point;
            return side === 'client' ? 'client-control' : 'server-control';
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
            // Calculate proportional height based on byte count
            let height = 20; // Base height
            if (this.viewOptions.showSizes && event.object_size > 0) {
                height = Math.max(20, Math.min(200, event.object_size / 256));
            }

            // Simple Y position based on relative time.
            // Use a usable height that accounts for the bar height so the last
            // event doesn't render past the bottom of the timeline.
            const usableHeight = Math.max(0, timelineHeight - height);
            const timePosition = (event.relative_time / timelineDuration) * usableHeight;
            const top = timePosition + (this.panOffset * timelineHeight);

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

        // Determine if this is a control message and add appropriate arrow class.
        // Use the *rendered column* (which may differ from vantage_point for parsed
        // messages) so client-side arrows always point right.
        let arrowClass = '';
        if (event.event_category === 'control') {
            if (columnKey === 'client-control') {
                arrowClass = 'arrow-right';
            } else if (columnKey === 'server-control') {
                arrowClass = 'arrow-left';
            }
        }

        const $eventBar = $(`
            <div class="event-bar ${eventClass} ${arrowClass}"
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
        }).on('click', (e) => {
            e.stopPropagation();
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
     * Format time for display (always in milliseconds)
     * @param {number} timeMs - Time in milliseconds
     * @returns {string} Formatted time string
     */
    formatTime(timeMs) {
        if (timeMs < 0.1) {
            return `${(timeMs * 1000).toFixed(1)} µs`;
        } else if (timeMs < 10) {
            return `${timeMs.toFixed(2)} ms`;
        } else if (timeMs < 1000) {
            return `${timeMs.toFixed(1)} ms`;
        } else {
            return `${timeMs.toFixed(0)} ms`;
        }
    }

    /**
     * Show event tooltip
     * @param {Event} e - Mouse event
     * @param {Object} eventData - Event data
     */
    showEventTooltip(eventData, pageX, pageY) {
        const $tooltip = $('#event-tooltip');
        const content = this.generateTooltipContent(eventData);

        $tooltip.find('.tooltip-content').html(content);
        $tooltip.css({
            left: pageX + 10,
            top: pageY - 10
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
            <strong>Time:</strong> ${this.formatTime(event.relative_time)}<br>
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
            <p><strong>Timestamp:</strong> ${this.formatTime(event.relative_time)}</p>
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
        this.showEventDetails();
    }

    /**
     * Show the event details panel
     */
    showEventDetails() {
        $('#event-details').removeClass('hidden');
    }

    /**
     * Hide the event details panel
     */
    hideEventDetails() {
        $('#event-details').addClass('hidden');
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
