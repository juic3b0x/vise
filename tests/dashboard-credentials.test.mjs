import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {parseUrls, readUrlFile, requestFields, clearRequestFields, requestSummary} from '../src/dashboard-input.mjs';

// Execute the shipped controller, not a parallel submission implementation.
function dashboard(failure = false) {
  const nodes = new Map(), logs = [], requests = [], storage = [], listeners = {};
  let animation;
  class Element {
    value = ''; textContent = ''; children = []; dataset = {}; style = {setProperty() {}};
    classList = {add() {}, remove() {}, toggle() {}}; events = {}; hidden = false;
    constructor(selectors = false) { this.selectors = selectors ? new Map() : null; }
    get firstChild() { return this.children[0]; }
    insertBefore(item) { if (!this.children.includes(item)) this.children.push(item); }
    replaceChildren(...items) { this.children = items; }
    addEventListener(type, callback) { this.events[type] = callback; }
    getAttribute() { return ''; } setAttribute() {} removeAttribute() {} focus() {}
    querySelector(selector) {
      if (!this.selectors) return null;
      if (!this.selectors.has(selector)) this.selectors.set(selector, new Element());
      return this.selectors.get(selector);
    }
    contains() { return false; }
    append(...items) { this.children.push(...items); }
    prepend(item) { this.children.unshift(item); }
    reset() { for (const id of ['url','filename','expected-sha256','expected-md5','request-headers','request-referer','request-user-agent','request-cookies']) get(id).value = ''; }
  }
  const get = id => { if (!nodes.has(id)) nodes.set(id, new Element()); return nodes.get(id); };
  get('download-template').content = {firstElementChild: {cloneNode: () => new Element(true)}};
  const snapshot = {timestamp: Date.now(), configuration: {limit_rate: 'off', limit_bytes_per_second: null}, downloads: []};
  class Socket { static OPEN = 1; constructor() { Socket.last = this; } close() {} }
  const context = vm.createContext({URL, AbortController, Error, Date, Set, Map, Number, JSON, Math,
    parseUrls, readUrlFile, requestFields, clearRequestFields, requestSummary,
    performance: {now: () => 10}, requestAnimationFrame: callback => { animation = callback; return 1; },
    setTimeout: () => 1, clearTimeout() {}, setInterval() {},
    document: {getElementById: get, querySelectorAll: () => [], createElement: () => new Element(), addEventListener() {}},
    window: {addEventListener: (name, fn) => { listeners[name] = fn; }},
    location: {protocol: 'http:', host: '127.0.0.1:8080'}, WebSocket: Socket,
    console: Object.fromEntries(['log','warn','error','debug'].map(level => [level, (...args) => logs.push(args)])),
    localStorage: {setItem: (...args) => storage.push(args)}, sessionStorage: {setItem: (...args) => storage.push(args)},
    fetch: async (path, options) => {
      requests.push({path, options});
      const error = path.startsWith('/api/downloads') && failure;
      return {ok: !error, status: error ? 400 : 200, text: async () => JSON.stringify(error ? {error: 'Credentialed batches must share one origin; split the batch.'} : path === '/api/telemetry' ? snapshot : path === '/api/downloads/batch' ? {accepted: [{index: 1, download: {id: '1', filename: 'archive.zip'}}], rejected: []} : {id: '1', filename: 'archive.zip'})};
    },
  });
  vm.runInContext(fs.readFileSync(new URL('../src/dashboard.js', import.meta.url), 'utf8').replace(/^import .*;\n/, ''), context);
  return {get, logs, requests, storage, listeners, snapshot, Socket, flush: () => { const callback = animation; animation = null; callback?.(); }};
}
for (const failure of [false, true]) test(`dashboard sentinel audit: ${failure ? 'rejected' : 'accepted'} admission clears write-only fields`, async () => {
  const app = dashboard(failure);
  await new Promise(setImmediate);
  const sentinels = ['HEADER_SENTINEL_51972','REFERER_SENTINEL_92371','COOKIE_SENTINEL_27361','UA_SENTINEL_77136'];
  app.get('url').value = 'https://example.com/archive.zip';
  app.get('request-headers').value = `X-Session: ${sentinels[0]}`;
  app.get('request-referer').value = 'https://example.com/' + sentinels[1];
  app.get('request-user-agent').value = sentinels[3];
  app.get('request-cookies').value = JSON.stringify([{name:'session', value:sentinels[2], domain:'example.com', path:'/', secure:true, httpOnly:true, hostOnly:true, expires:null}]);
  await app.get('add-form').events.submit({preventDefault() {}});
  const admission = app.requests.find(request => request.path === '/api/downloads');
  assert.ok(admission);
  for (const sentinel of sentinels) assert.ok(admission.options.body.includes(sentinel));
  for (const id of ['request-headers','request-referer','request-user-agent','request-cookies']) assert.equal(app.get(id).value, '');
  // Snapshots cannot refill any write-only input, even if a future server adds fields.
  app.Socket.last.onmessage({data: JSON.stringify({...app.snapshot, request_credentials: {referer: sentinels[1]}})});
  const sinks = JSON.stringify([app.logs, app.storage, app.get('activity'), app.get('notice-text'), app.get('input-feedback')]);
  for (const sentinel of sentinels) assert.ok(!sinks.includes(sentinel));
  assert.equal(app.storage.length, 0);
  if (failure) assert.match(app.get('notice-text').textContent, /split/);
  app.get('request-referer').value = sentinels[1];
  app.listeners.pagehide();
  assert.equal(app.get('request-referer').value, '');
});
test('dashboard multi-origin rejection performs no admission and clears credentials', async () => {
  const app = dashboard();
  await new Promise(setImmediate);
  app.get('url').value = 'https://a.example/a\nhttps://b.example/b';
  app.get('request-headers').value = 'X-Session: HEADER_SENTINEL_97125';
  await app.get('add-form').events.submit({preventDefault() {}});
  assert.ok(!app.requests.some(request => request.path.startsWith('/api/downloads')));
  assert.match(app.get('input-feedback').textContent, /Split/);
  assert.equal(app.get('request-headers').value, '');
});

test('dashboard submits same-origin credential batches once using the existing API shape', async () => {
  const app = dashboard(); await new Promise(setImmediate);
  app.get('url').value = 'https://example.com/a\nhttps://example.com/b';
  app.get('request-headers').value = 'X-Session: HEADER_SENTINEL_92681';
  await app.get('add-form').events.submit({preventDefault() {}});
  const admissions = app.requests.filter(request => request.path.startsWith('/api/downloads'));
  assert.equal(admissions.length, 1);
  assert.equal(admissions[0].path, '/api/downloads/batch');
  assert.deepEqual(JSON.parse(admissions[0].options.body), {urls: ['https://example.com/a','https://example.com/b'], headers: [{name: 'X-Session', value: 'HEADER_SENTINEL_92681'}]});
  assert.equal(app.get('request-headers').value, '');
  assert.ok(!JSON.stringify([app.logs, app.storage, app.get('activity')]).includes('HEADER_SENTINEL_92681'));
});

test('dashboard renders flattened API request summaries without exposing values or refilling inputs', async () => {
  const app = dashboard(); await new Promise(setImmediate);
  // This fixture is generated and shape-checked by the Rust model test.
  const task = JSON.parse(fs.readFileSync(new URL('./fixtures/public-download.json', import.meta.url), 'utf8'));
  const sentinels = ['GOLDEN_HEADER_SENTINEL_83172', 'GOLDEN_AUTH_SENTINEL_59384',
    'GOLDEN_REFERER_SENTINEL_24961', 'GOLDEN_AGENT_SENTINEL_48273', 'GOLDEN_COOKIE_SENTINEL_73018'];
  for (const sentinel of sentinels) assert.ok(!JSON.stringify(task).includes(sentinel));
  for (const privateKey of ['headers','referer','user_agent','cookies','request_credentials','request']) {
    assert.equal(Object.hasOwn(task, privateKey), false);
  }
  app.Socket.last.onmessage({data: JSON.stringify({...app.snapshot, downloads: [task]})});
  app.flush();
  const summary = app.get('downloads').children[0].querySelector('.request-summary');
  assert.equal(summary.hidden, false);
  assert.match(summary.textContent, /2 headers: authorization, x-session/);
  assert.match(summary.textContent, /Referer supplied/);
  assert.match(summary.textContent, /Custom user agent/);
  assert.match(summary.textContent, /Cookies supplied/);
  assert.equal(summary.textContent, '2 headers: authorization, x-session · Referer supplied · Custom user agent · Cookies supplied');
  for (const id of ['request-headers','request-referer','request-user-agent','request-cookies']) assert.equal(app.get(id).value, '');
  const rendered = app.get('downloads').children[0];
  const sinks = JSON.stringify([summary.textContent, [...rendered.selectors.values()].map(node => node.textContent), app.logs, app.storage, app.get('activity')]);
  for (const sentinel of sentinels) assert.ok(!sinks.includes(sentinel));
});
