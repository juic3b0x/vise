import {parseUrls, readUrlFile, requestFields, clearRequestFields, requestSummary} from '/assets/dashboard-input.mjs';

'use strict';

// Dependency-free controller. Histories, event entries, and DOM nodes are bounded
// by the current queue; only changed attributes/text are patched on updates.
(() => {
  const $ = id => document.getElementById(id);
  const requestInputs = {headers: $('request-headers'), referer: $('request-referer'), userAgent: $('request-user-agent'), cookies: $('request-cookies')};
  const active = state => ['queued', 'probing', 'downloading', 'verifying'].includes(state);
  const states = new Set(['queued', 'probing', 'downloading', 'verifying', 'paused', 'completed', 'failed', 'corrupt']);
  const inCategory = (d, category) => category === 'all' || (category === 'active' ? active(d.status) : category === 'failed' ? ['failed', 'corrupt'].includes(d.status) : d.status === category);
  const chunkStates = new Set(['queued', 'downloading', 'paused', 'completed', 'failed']);
  const cards = new Map();
  const histories = new Map();
  const pending = new Map();
  const aggregate = [];
  const WINDOW = 60000;
  let downloads = [], configuration = null, initialized = false, filter = 'all';
  let lastSnapshot = -Infinity, lastTransport = '', frame = 0;
  let socket, reconnectTimer, reconnectDelay = 500, stopped = false;
  let polling = false, generation = 0;
  let rateDirty = false, rateRevision = 0, queuedRate = null, rateRunning = false;

  function text(node, value) {
    value = String(value);
    if (node.textContent !== value) node.textContent = value;
  }
  function attr(node, name, value) {
    value = String(value);
    if (node.getAttribute(name) !== value) node.setAttribute(name, value);
  }
  function bytes(value) {
    if (!Number.isFinite(value)) return '—';
    const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
    let unit = 0;
    while (value >= 1024 && unit < units.length - 1) { value /= 1024; unit++; }
    return `${value.toFixed(unit ? 1 : 0)} ${units[unit]}`;
  }
  const speed = value => `${bytes(value)}/s`;
  function eta(value) {
    if (value == null || !Number.isFinite(value)) return 'ETA —';
    value = Math.max(0, Math.ceil(value));
    return 'ETA ' + (value < 60 ? `${value}s` : value < 3600
      ? `${Math.floor(value / 60)}m ${value % 60}s`
      : `${Math.floor(value / 3600)}h ${Math.floor(value % 3600 / 60)}m`);
  }
  function percentage(written, total, completed) {
    if (completed) return 100;
    if (total == null) return null;
    return total === 0 ? (completed ? 100 : 0) : Math.min(100, Math.max(0, written / total * 100));
  }
  function showError(message) {
    text($('notice-text'), message);
    $('notice').hidden = false;
  }
  $('dismiss-notice').addEventListener('click', () => { $('notice').hidden = true; });

  async function api(path, body, method) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), path === '/api/downloads/batch' ? 120000 : 10000);
    try {
      const response = await fetch(path, {
        method: method || (body === undefined ? 'GET' : 'POST'),
        headers: body === undefined ? {} : { 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: controller.signal,
        cache: 'no-store',
      });
      const raw = await response.text();
      let data;
      try { data = raw ? JSON.parse(raw) : null; }
      catch { throw Error(response.ok ? 'The engine returned an invalid response.' : `Request failed (${response.status}).`); }
      if (!response.ok) throw Error(data?.error || `Request failed (${response.status}).`);
      return data;
    } catch (error) {
      if (error.name === 'AbortError') throw Error('The engine took too long to respond. Check the connection before retrying.');
      throw error;
    } finally { clearTimeout(timeout); }
  }

  function activity(message, detail = '', tone = '') {
    const list = $('activity');
    list.querySelector('.activity-empty')?.remove();
    const row = document.createElement('li'), time = document.createElement('time');
    const now = new Date();
    time.dateTime = now.toISOString();
    time.textContent = now.toLocaleTimeString([], { hour12: false, hour: '2-digit', minute: '2-digit' });
    time.title = now.toLocaleString();
    const body = document.createElement('div');
    body.className = 'activity-message';
    body.textContent = message;
    if (detail) { const secondary = document.createElement('small'); secondary.textContent = detail; body.append(secondary); }
    row.dataset.tone = tone;
    row.append(time, body);
    list.prepend(row);
    while (list.children.length > 40) list.lastChild.remove();
  }

  function validateSnapshot(data) {
    if (!data || !Array.isArray(data.downloads) || !data.configuration
      || typeof data.configuration.limit_rate !== 'string'
      || !Number.isFinite(data.timestamp)) throw Error('Invalid engine telemetry.');
    for (const d of data.downloads) {
      if (typeof d.id !== 'string' || typeof d.filename !== 'string' || typeof d.url !== 'string'
        || !states.has(d.status) || !Number.isFinite(d.bytes_per_second)
        || !Number.isFinite(d.downloaded_bytes) || !Array.isArray(d.chunks)) throw Error('Invalid download telemetry.');
    }
  }
  function accept(data, transport) {
    validateSnapshot(data);
    const previous = new Map(downloads.map(d => [d.id, d]));
    if (initialized) {
      for (const d of data.downloads) {
        const old = previous.get(d.id);
        if (!old) activity('Transfer queued', d.filename);
        else if (old.status !== d.status) {
          const labels = { downloading: 'Transfer running', verifying: 'Verifying checksums', corrupt: 'Checksum mismatch', paused: 'Transfer paused', completed: 'File completed', failed: 'Transfer failed', queued: 'Transfer queued', probing: 'Checking source' };
          activity(labels[d.status], d.filename, ['failed', 'corrupt'].includes(d.status) ? 'error' : d.status === 'completed' ? 'success' : '');
        }
      }
      const ids = new Set(data.downloads.map(d => d.id));
      for (const old of downloads) if (!ids.has(old.id)) activity('Removed from queue', old.filename);
      if (configuration && !rateRunning && configuration.limit_rate !== data.configuration.limit_rate) activity('Bandwidth updated', data.configuration.limit_rate);
    } else activity('Engine connected', `${data.downloads.length} saved transfer${data.downloads.length === 1 ? '' : 's'} available`, 'success');
    initialized = true;
    downloads = data.downloads;
    const currentIds = new Set(downloads.map(d => d.id));
    for (const id of histories.keys()) if (!currentIds.has(id)) histories.delete(id);
    configuration = data.configuration;
    lastSnapshot = performance.now();
    lastTransport = transport;
    text($('update-time'), 'Updated ' + new Date(data.timestamp).toLocaleTimeString([], { hour12: false }));
    renderConfiguration();
    scheduleRender();
  }

  function connected() { return performance.now() - lastSnapshot < 3500; }
  function scheduleRender() {
    if (frame) return;
    frame = requestAnimationFrame(() => { frame = 0; render(); });
  }
  function matches(d) {
    const query = $('search').value.trim().toLocaleLowerCase();
    return inCategory(d, filter)
      && (!query || d.filename.toLocaleLowerCase().includes(query) || d.url.toLocaleLowerCase().includes(query));
  }
  function render() {
    const fresh = connected();
    const running = downloads.filter(d => active(d.status));
    const totalSpeed = downloads.reduce((sum, d) => sum + Math.max(0, d.bytes_per_second), 0);
    text($('total-speed'), fresh ? speed(totalSpeed) : '—');
    text($('speed-note'), fresh ? (running.length ? 'Live across the worker pool' : 'Ready for new transfers') : initialized ? 'Connection interrupted · data stale' : 'Awaiting engine telemetry');
    text($('active-count'), initialized ? String(running.length) : '—');
    text($('connection-count'), downloads.reduce((sum, d) => sum + d.chunks.filter(c => c.status === 'downloading' && active(d.status)).length, 0) + ' active chunks');
    text($('received-total'), initialized ? bytes(downloads.reduce((sum, d) => sum + d.downloaded_bytes, 0)) : '—');
    text($('completed-count'), initialized ? String(downloads.filter(d => d.status === 'completed').length) : '—');
    text($('queue-count'), `${downloads.length} transfer${downloads.length === 1 ? '' : 's'} in queue`);
    $('connection').className = 'connection' + (fresh ? lastTransport === 'websocket' ? ' live' : ' polling' : '');
    text($('connection-text'), fresh ? lastTransport === 'websocket' ? 'Connected' : 'Connected · polling' : initialized ? 'Reconnecting' : 'Connecting');
    for (const count of document.querySelectorAll('[data-count]')) {
      const category = count.dataset.count;
      text(count, downloads.filter(d => inCategory(d, category)).length);
    }
    const ids = new Set(downloads.map(d => d.id));
    for (const [id, card] of cards) if (!ids.has(id)) {
      if (card.root.contains(document.activeElement)) $('url').focus();
      card.root.remove(); cards.delete(id); histories.delete(id);
    }
    let cursor = $('downloads').firstChild, visible = 0;
    // Stable ordering avoids moving cards when worker speeds change.
    for (const d of [...downloads].sort((a, b) => b.created_at - a.created_at || a.id.localeCompare(b.id))) {
      let card = cards.get(d.id);
      if (!card) { card = makeCard(d.id); cards.set(d.id, card); }
      if (card.root !== cursor) $('downloads').insertBefore(card.root, cursor);
      cursor = card.root.nextSibling;
      card.root.hidden = !matches(d);
      card.download = d;
      updateCard(card, d, fresh);
      if (!card.root.hidden) visible++;
    }
    text($('visible-count'), String(visible));
    $('empty-state').hidden = visible > 0;
    if (initialized) {
      text($('empty-title'), downloads.length ? 'No matching transfers' : 'Ready for your next transfer.');
      text($('empty-description'), downloads.length ? 'Choose another filter or change your search.' : 'Add a URL above. Vise takes it from here.');
    }
    drawGraphs();
  }

  function makeCard(id) {
    const root = $('download-template').content.firstElementChild.cloneNode(true);
    const select = selector => root.querySelector(selector);
    const card = { root, download: null, filename: select('.filename'), source: select('.source'), badge: select('.badge'),
      size: select('.download-size'), rate: select('.download-speed'), percent: select('.download-percent'),
      track: select('.progress-track'), fill: select('.progress-fill'), summary: select('.chunk-summary'),
      grid: select('.chunk-grid'), eta: select('.download-eta'),
      toggle: select('.toggle'), cancel: select('.cancel'), error: select('.download-error'), checksum: select('.checksum-status'), request: select('.request-summary'), spark: select('.sparkline path'), tiles: [] };
    card.toggle.addEventListener('click', () => action(id, active(card.download.status) ? 'pause' : ['corrupt','failed'].includes(card.download.status) ? 'retry' : 'resume'));
    card.cancel.addEventListener('click', () => action(id, 'delete'));
    return card;
  }
  function updateCard(c, d, fresh) {
    const inFlight = pending.get(d.id);
    attr(c.root, 'data-state', d.status);
    c.root.classList.toggle('pending', !!inFlight);
    attr(c.root, 'aria-busy', !!inFlight);
    text(c.filename, d.filename); attr(c.filename, 'title', d.filename);
    text(c.source, d.url); attr(c.source, 'title', d.url);
    text(c.badge, inFlight ? { pause: 'Pausing…', resume: 'Resuming…', retry: 'Restarting…', delete: 'Removing…' }[inFlight] : d.status[0].toUpperCase() + d.status.slice(1));
    text(c.size, bytes(d.downloaded_bytes) + ' / ' + (d.total_bytes == null ? 'unknown' : bytes(d.total_bytes)));
    text(c.rate, fresh ? speed(d.bytes_per_second) : '— /s');
    const percent = percentage(d.downloaded_bytes, d.total_bytes, d.status === 'completed');
    text(c.percent, percent == null ? '—%' : `${percent.toFixed(1)}%`);
    c.track.classList.toggle('indeterminate', percent == null);
    if (percent == null) c.track.removeAttribute('aria-valuenow');
    else attr(c.track, 'aria-valuenow', percent.toFixed(1));
    attr(c.track, 'aria-valuetext', `${bytes(d.downloaded_bytes)} of ${d.total_bytes == null ? 'unknown size' : bytes(d.total_bytes)}`);
    c.fill.style.width = percent == null ? '' : `${percent}%`;
    text(c.eta, d.status === 'completed' ? 'Complete' : d.status === 'paused' ? 'Paused' : ['failed', 'corrupt'].includes(d.status) ? 'Needs attention' : fresh ? eta(d.eta_seconds) : 'Telemetry unavailable');
    c.error.hidden = !d.error; text(c.error, d.error || '');
    const algorithms = [d.expected_sha256 && 'SHA-256', d.expected_md5 && 'MD5'].filter(Boolean).join(' + ');
    const summary = requestSummary(d.request);
    c.request.hidden = !summary; text(c.request, summary);
    c.checksum.hidden = !algorithms;
    text(c.checksum, d.status === 'corrupt' ? `${algorithms} mismatch. File retained; re-download retries every chunk.` : d.status === 'verifying' ? `Verifying ${algorithms} before publication…` : d.verified_at != null ? `${algorithms} verified · ${new Date(d.verified_at).toLocaleString()}` : `${algorithms} · Verification required before publication`);
    attr(c.checksum, 'title', [d.actual_sha256 && `SHA-256: ${d.actual_sha256}`, d.actual_md5 && `MD5: ${d.actual_md5}`].filter(Boolean).join(' / '));
    c.toggle.hidden = d.status === 'completed';
    text(c.toggle, inFlight && inFlight !== 'delete' ? 'Working…' : active(d.status) ? 'Pause' : d.status === 'corrupt' ? 'Re-download' : d.status === 'failed' ? 'Retry' : 'Resume');
    text(c.cancel, inFlight === 'delete' ? 'Removing…' : d.status === 'completed' ? 'Remove' : 'Cancel');
    attr(c.cancel, 'title', d.status === 'completed' ? 'Remove from queue. The completed file is kept.' : 'Cancel this transfer and discard its partial data.');
    attr(c.toggle, 'aria-label', `${c.toggle.textContent}: ${d.filename}`);
    attr(c.cancel, 'aria-label', `${c.cancel.textContent}: ${d.filename}`);
    c.toggle.disabled = c.cancel.disabled = !!inFlight;
    updateChunks(c, d);
  }
  function updateChunks(c, d) {
    const complete = d.chunks.filter(chunk => chunk.status === 'completed').length;
    text(c.summary, d.chunks.length ? `Chunks · ${complete} of ${d.chunks.length} complete` : 'Chunks · Checking source');
    if (!d.chunks.length) {
      if (!c.grid.querySelector('.chunk-waiting')) {
        const waiting = document.createElement('span'); waiting.className = 'chunk-waiting';
        waiting.textContent = 'Layout appears after the source is checked.'; c.grid.replaceChildren(waiting); c.tiles = [];
      }
      return;
    }
    c.grid.querySelector('.chunk-waiting')?.remove();
    while (c.tiles.length > d.chunks.length) c.tiles.pop().remove();
    for (let index = 0; index < d.chunks.length; index++) {
      const chunk = d.chunks[index];
      let tile = c.tiles[index];
      if (!tile) { tile = document.createElement('span'); tile.className = 'chunk'; tile.setAttribute('role', 'img'); c.grid.append(tile); c.tiles.push(tile); }
      const state = chunkStates.has(chunk.status) ? chunk.status : 'queued';
      attr(tile, 'data-state', state);
      const total = chunk.end_byte == null ? null : chunk.end_byte - chunk.start_byte;
      const percent = percentage(chunk.downloaded_bytes, total, state === 'completed');
      tile.classList.toggle('unknown', total == null);
      tile.style.setProperty('--progress', `${percent ?? 0}%`);
      const label = `Chunk ${chunk.index + 1}: ${state}; ${bytes(chunk.downloaded_bytes)} / ${total == null ? 'unknown length' : bytes(total)}${percent == null ? '' : ` (${percent.toFixed(1)}%)`}; bytes ${chunk.start_byte}–${chunk.end_byte == null ? '?' : chunk.end_byte - 1}`;
      attr(tile, 'title', label); attr(tile, 'aria-label', label);
    }
  }
  async function action(id, operation) {
    if (pending.has(id)) return;
    pending.set(id, operation); scheduleRender();
    try {
      await api(`/api/downloads/${encodeURIComponent(id)}${operation === 'delete' ? '' : '/' + operation}`, {}, operation === 'delete' ? 'DELETE' : 'POST');
      if (operation === 'delete') {
        const removed = downloads.find(item => item.id === id);
        downloads = downloads.filter(item => item.id !== id);
        if (removed) activity(removed.status === 'completed' ? 'Removed from queue' : 'Transfer cancelled', removed.filename);
      }
      // GET reconciliation also handles a file completing during a command.
      await poll(true);
    } catch (error) { showError(error.message); }
    finally { pending.delete(id); scheduleRender(); }
  }

  let readingFile = false;
  function inputFeedback(message, error = false) {
    text($('input-feedback'), message);
    $('input-feedback').classList.toggle('error', error);
  }
  async function loadFile(file) {
    if (readingFile || $('add-button').disabled) return;
    readingFile = true; $('add-button').disabled = true;
    try {
      const {content, urls} = await readUrlFile(file);
      $('url').value = content;
      inputFeedback(`${file.name}: ${urls.length} URL${urls.length === 1 ? '' : 's'} ready. Select Add transfers to queue.`);
    } catch (error) { inputFeedback(error.message, true); }
    finally { readingFile = false; $('add-button').disabled = false; $('input-file').value = ''; }
  }
  $('input-file').addEventListener('change', () => loadFile($('input-file').files[0]));
  const drop = $('file-drop');
  for (const type of ['dragenter', 'dragover']) drop.addEventListener(type, event => {
    event.preventDefault(); drop.classList.add('dragging');
  });
  drop.addEventListener('dragleave', event => { if (!drop.contains(event.relatedTarget)) drop.classList.remove('dragging'); });
  drop.addEventListener('drop', event => {
    event.preventDefault(); drop.classList.remove('dragging');
    if (event.dataTransfer.files.length !== 1) { inputFeedback('Drop exactly one .txt file.', true); return; }
    loadFile(event.dataTransfer.files[0]);
  });
  $('url').addEventListener('input', () => inputFeedback('Ready to submit. Filename and checksum fields apply to one URL only.'));
  function added(items) {
    for (const item of items) if (!downloads.some(d => d.id === item.id)) {
      downloads.push(item); activity('Transfer added', item.filename);
    }
    filter = 'all'; updateFilters(); $('search').value = ''; scheduleRender();
  }
  async function submitUrls(urls, credentials) {
    const filename = $('filename').value.trim() || null;
    const expected_sha256 = $('expected-sha256').value.trim() || null;
    const expected_md5 = $('expected-md5').value.trim() || null;
    if (urls.length > 1 && (filename || expected_sha256 || expected_md5)) {
      throw Error('Filename and checksum fields require exactly one URL. Clear these fields to submit a batch.');
    }
    if (urls.length === 1) {
      const item = await api('/api/downloads', {url: urls[0], filename, expected_sha256, expected_md5, ...credentials});
      added([item]); $('add-form').reset(); inputFeedback('Transfer queued.');
      return;
    }
    const result = await api('/api/downloads/batch', {urls, ...credentials});
    added(result.accepted.map(entry => entry.download));
    if (result.rejected.length) {
      // Keep only rejected entries for inspection; never automatically retry a
      // batch. A journal/start failure may have left a paused task in history.
      $('url').value = result.rejected.map(entry => urls[entry.index - 1]).join('\n');
      inputFeedback(`${result.accepted.length} queued; ${result.rejected.length} rejected. Check queue history before retrying.`, true);
      showError(result.rejected.map(entry => `Entry ${entry.index}: ${entry.error}`).join('\n'));
    } else {
      $('add-form').reset(); inputFeedback(`${result.accepted.length} transfers queued.`);
    }
  }
  $('add-form').addEventListener('submit', async event => {
    event.preventDefault();
    const button = $('add-button'); button.disabled = true; text(button, 'Adding…');
    try {
      const urls = parseUrls($('url').value);
      const credentials = requestFields(requestInputs, urls);
      clearRequestFields(requestInputs);
      await submitUrls(urls, credentials); $('url').focus();
      await poll(true);
    } catch (error) { inputFeedback(error.message, true); showError(error.message); }
    finally { clearRequestFields(requestInputs); button.disabled = false; text(button, 'Add transfers'); }
  });
  function updateFilters() {
    for (const button of document.querySelectorAll('[data-filter]')) {
      const selected = button.dataset.filter === filter;
      button.classList.toggle('selected', selected); attr(button, 'aria-pressed', selected);
    }
  }
  for (const button of document.querySelectorAll('[data-filter]')) button.addEventListener('click', () => {
    filter = button.dataset.filter; updateFilters(); scheduleRender();
  });
  $('search').addEventListener('input', scheduleRender);

  const sliderRate = position => position === 0 ? null : Math.round(2 ** (14 + 16 * (position - 1) / 99) / 1024) * 1024;
  const sliderPosition = rate => rate == null ? 0 : Math.min(100, Math.max(1, Math.round(1 + (Math.log2(Math.max(1, rate)) - 14) / 16 * 99)));
  const rateInput = rate => rate == null ? 'off' : rate % 1048576 === 0 ? `${rate / 1048576}M` : rate % 1024 === 0 ? `${rate / 1024}K` : `${rate} B/s`;
  function sliderDisplay(position, rate) {
    $('rate-slider').value = position;
    $('rate-slider').style.setProperty('--range', `${position}%`);
    attr($('rate-slider'), 'aria-valuetext', rate == null ? 'Unlimited' : speed(rate));
  }
  function renderConfiguration() {
    if (!configuration) return;
    const rate = configuration.limit_bytes_per_second;
    text($('rate-current'), rate == null ? 'Unlimited' : speed(rate));
    for (const preset of document.querySelectorAll('[data-rate]')) {
      const selected = preset.dataset.rate === 'off' ? rate == null : Number.parseFloat(preset.dataset.rate) * 1048576 === rate;
      preset.classList.toggle('selected', selected); attr(preset, 'aria-pressed', selected);
    }
    if (!rateDirty && !rateRunning && document.activeElement !== $('limit-rate') && document.activeElement !== $('rate-slider')) {
      $('limit-rate').value = rateInput(rate); sliderDisplay(sliderPosition(rate), rate);
    }
    if (!rateDirty && !rateRunning) text($('rate-feedback'), 'Saved. Applies to all active transfers.');
  }
  $('limit-rate').addEventListener('input', () => {
    rateDirty = true; rateRevision++;
    $('limit-rate').removeAttribute('aria-invalid'); $('rate-feedback').classList.remove('error');
    text($('rate-feedback'), 'Press Enter or Apply to save.');
  });
  $('rate-slider').addEventListener('input', () => {
    const position = Number($('rate-slider').value), rate = sliderRate(position);
    rateDirty = true; rateRevision++;
    $('limit-rate').value = rateInput(rate); sliderDisplay(position, rate);
    text($('rate-feedback'), `Release to apply · ${rate == null ? 'Unlimited' : speed(rate)}`);
  });
  $('rate-slider').addEventListener('change', () => queueRate($('limit-rate').value));
  $('rate-form').addEventListener('submit', event => { event.preventDefault(); queueRate($('limit-rate').value.trim() || 'off'); });
  for (const preset of document.querySelectorAll('[data-rate]')) preset.addEventListener('click', () => {
    rateRevision++; rateDirty = true; $('limit-rate').value = preset.dataset.rate; queueRate(preset.dataset.rate);
  });
  async function queueRate(value) {
    queuedRate = { value, revision: rateRevision };
    if (rateRunning) return;
    rateRunning = true;
    $('rate-form').setAttribute('aria-busy', 'true');
    text($('rate-button'), 'Applying…');
    while (queuedRate) {
      const request = queuedRate; queuedRate = null;
      text($('rate-feedback'), 'Applying to active connections…');
      try {
        configuration = await api('/api/config', { limit_rate: request.value });
        if (request.revision === rateRevision) rateDirty = false;
        $('limit-rate').removeAttribute('aria-invalid'); $('rate-feedback').classList.remove('error');
        text($('rate-feedback'), rateDirty ? 'Press Enter or Apply to save.' : 'Saved. Applies to all active transfers.');
        activity('Bandwidth applied', configuration.limit_rate);
      } catch (error) {
        rateDirty = true;
        $('limit-rate').setAttribute('aria-invalid', 'true'); $('rate-feedback').classList.add('error');
        text($('rate-feedback'), error.message);
      }
      renderConfiguration();
    }
    rateRunning = false; $('rate-form').removeAttribute('aria-busy'); text($('rate-button'), 'Apply'); renderConfiguration();
  }

  function trim(history, now) {
    while (history.length && (history[0].time < now - WINDOW || history.length > 65)) history.shift();
  }
  function paths(history, width, height, max, now) {
    let line = '', area = '', start = null, last = null;
    const close = () => { if (last) area += `L${last.x},${height}L${start.x},${height}Z`; start = last = null; };
    for (const point of history) {
      if (point.value == null || point.time < now - WINDOW) { close(); continue; }
      const x = Math.max(0, Math.min(width, (point.time - now + WINDOW) / WINDOW * width)).toFixed(1);
      const y = (height - Math.min(1, point.value / max) * (height - 5) - 2).toFixed(1);
      if (last && point.time - last.time > 2500) close();
      const command = start ? 'L' : 'M';
      line += `${command}${x},${y}`;
      area += `${command}${x},${y}`;
      if (!start) start = { x };
      last = { x, time: point.time };
    }
    close(); return { line, area };
  }
  function drawGraphs() {
    if (document.hidden) return;
    const now = performance.now();
    trim(aggregate, now);
    const peak = aggregate.reduce((max, point) => Math.max(max, point.value || 0), 0);
    const maximum = Math.max(1024, peak * 1.15);
    const graph = paths(aggregate, 800, 150, maximum, now);
    attr($('graph-line'), 'd', graph.line); attr($('graph-area'), 'd', graph.area);
    text($('graph-scale'), speed(maximum)); text($('graph-peak'), speed(peak));
    text($('graph-current'), connected() ? speed(downloads.reduce((sum, d) => sum + d.bytes_per_second, 0)) : '—');
    $('graph-empty').hidden = peak > 0;
    for (const [id, card] of cards) {
      if (card.root.hidden) continue;
      const history = histories.get(id) || []; trim(history, now);
      const max = Math.max(1, ...history.map(point => point.value || 0));
      attr(card.spark, 'd', paths(history, 160, 28, max * 1.1, now).line);
    }
  }
  function sample() {
    const now = performance.now(), fresh = connected();
    aggregate.push({ time: now, value: fresh ? downloads.reduce((sum, d) => sum + d.bytes_per_second, 0) : null });
    trim(aggregate, now);
    for (const d of downloads) {
      let history = histories.get(d.id);
      if (!history) { history = []; histories.set(d.id, history); }
      history.push({ time: now, value: fresh ? d.bytes_per_second : null }); trim(history, now);
    }
    scheduleRender();
    // A live socket with no new frames is stale too. Fall back while reconnecting.
    if (!fresh && socket?.readyState === WebSocket.OPEN) socket.close();
  }

  async function poll(force = false) {
    if (polling || (!force && connected() && lastTransport === 'websocket')) return;
    polling = true;
    const observedGeneration = generation;
    try {
      const data = await api('/api/telemetry');
      // An HTTP response started before a newer WS frame must not rewind state.
      if (observedGeneration === generation) {
        accept(data, socket?.readyState === WebSocket.OPEN && connected() ? 'websocket' : 'http');
        generation++;
      }
    } catch (error) { if (force) showError(error.message); }
    finally { polling = false; }
  }
  function connect() {
    if (stopped) return;
    socket = new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/ws/telemetry`);
    const current = socket;
    let deadline = setTimeout(() => current.close(), 6000);
    current.onmessage = event => {
      clearTimeout(deadline);
      try { accept(JSON.parse(event.data), 'websocket'); generation++; reconnectDelay = 500; }
      catch { showError('Telemetry could not be read. Reconnecting to the engine.'); current.close(); }
    };
    current.onclose = () => {
      clearTimeout(deadline);
      if (stopped) return;
      poll(); scheduleRender();
      reconnectTimer = setTimeout(connect, reconnectDelay + Math.random() * 200);
      reconnectDelay = Math.min(10000, reconnectDelay * 2);
    };
    current.onerror = () => current.close();
  }
  document.addEventListener('visibilitychange', () => { if (!document.hidden) { scheduleRender(); poll(); } });
  window.addEventListener('pagehide', () => { clearRequestFields(requestInputs); stopped = true; clearTimeout(reconnectTimer); socket?.close(); });
  // Restore connections when returning from the browser's back/forward cache.
  window.addEventListener('pageshow', () => { clearRequestFields(requestInputs); if (stopped) { stopped = false; connect(); poll(); } });
  poll(); connect();
  setInterval(sample, 1000);
  setInterval(poll, 2000);
})();
