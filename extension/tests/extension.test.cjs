'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const root = path.resolve(__dirname, '..');
const SECRET = 'synthetic-extension-token-0123456789abcdef';

function load({ worker = true, saved = { authToken: SECRET }, fetcher = async () => ({ ok: true, status: 201, json: async () => ({ limit_rate: 'unlimited' }) }), document, permissions = false, cookies = [], stores = [{id: 'container', tabIds: [7]}], partition = {topLevelSite: 'https://example.com', hasCrossSiteAncestor: false}, timer = setTimeout } = {}) {
  const events = {}, notifications = [], requests = [], menus = [], logs = [], cookieReads = [], grants = [], checks = [];
  let permitted = permissions;
  const state = { ...saved };
  const event = key => ({ addListener(fn) { events[key] = fn; } });
  const chrome = {
    runtime: { onInstalled: event('installed'), onStartup: event('startup'), lastError: undefined,
      getURL: name => `chrome-extension://test/${name}`, openOptionsPage: cb => cb() },
    storage: { local: {
      get: (defaults, cb) => cb({ ...defaults, ...state }),
      set: (values, cb) => { Object.assign(state, values); cb(); },
      setAccessLevel: (_level, cb) => cb(),
    } },
    contextMenus: { onClicked: event('link'), removeAll: cb => { menus.length = 0; cb(); }, create: (value, cb) => { menus.push(value); cb(); } },
    notifications: { create: (_id, value, cb) => { notifications.push(value); cb('test'); } },
    action: { onClicked: event('action') },
    permissions: { contains: (query, cb) => { checks.push(query); cb(permitted); }, request: (query, cb) => { grants.push(query); permitted = true; cb(true); }, getAll: cb => cb({origins: permitted ? ['https://example.com/*'] : []}), remove: (_query, cb) => { permitted = false; cb(true); } },
    cookies: { getAllCookieStores: cb => cb(stores), getPartitionKey: (_details, cb) => cb({partitionKey: partition}), getAll: (query, cb) => { cookieReads.push(query); cb(cookies.filter(c => Boolean(c.partitionKey) === Boolean(query.partitionKey))); } },
  };
  const context = vm.createContext({ chrome, URL, AbortController, Error, TypeError, document,
    setTimeout: timer, clearTimeout, console: { warn: (...args) => logs.push(args), log: (...args) => logs.push(args), error: (...args) => logs.push(args) },
    fetch: async (url, options) => { requests.push({ url, options }); return fetcher(url, options); },
  });
  const run = file => vm.runInContext(fs.readFileSync(path.join(root, file), 'utf8'), context, { filename: file });
  if (worker) context.importScripts = run;
  else run('common.js');
  run('background.js');
  return { events, notifications, requests, menus, state, run, context, logs, cookieReads, grants, checks };
}

test('manifest supports both MV3 backgrounds and bundles every referenced icon', () => {
  const manifest = JSON.parse(fs.readFileSync(path.join(root, 'manifest.json')));
  assert.equal(manifest.manifest_version, 3);
  assert.equal(manifest.background.service_worker, 'background.js');
  assert.deepEqual(manifest.background.scripts, ['common.js', 'background.js']);
  assert.deepEqual(manifest.host_permissions, ['http://127.0.0.1/*', 'http://localhost/*']);
  assert.deepEqual(manifest.permissions.sort(), ['contextMenus', 'notifications', 'storage']);
  assert.deepEqual(manifest.optional_permissions, ['cookies']);
  assert.deepEqual(manifest.optional_host_permissions, ['http://*/*', 'https://*/*']);
  for (const [size, icon] of Object.entries(manifest.icons)) {
    const png = fs.readFileSync(path.join(root, icon));
    assert.equal(png.subarray(1, 4).toString(), 'PNG');
    assert.equal(png.readUInt32BE(16), Number(size));
    assert.equal(png.readUInt32BE(20), Number(size));
  }
});

for (const worker of [true, false]) {
  test(`${worker ? 'service worker' : 'event page'} registers one menu and queues only the clicked link`, async () => {
    const app = load({ worker });
    assert.equal(typeof app.events.link, 'function');
    await app.events.installed();
    await app.events.startup();
    assert.equal(app.menus.length, 2);
    assert.equal(app.menus[0].title, 'Send to Vise');
    await app.events.link({ menuItemId: 'vise-send-link', linkUrl: 'https://example.com/file.zip?signature=private', pageUrl: 'https://private.example/page' });
    assert.equal(app.requests.length, 1);
    const { url, options } = app.requests[0];
    assert.equal(url, 'http://127.0.0.1:8080/api/downloads');
    assert.equal(options.method, 'POST');
    assert.equal(options.headers.Authorization, `Bearer ${SECRET}`);
    assert.deepEqual(JSON.parse(options.body), { url: 'https://example.com/file.zip?signature=private' });
    assert.equal(options.headers['Content-Type'], 'application/json');
    assert.equal(options.redirect, 'error');
    assert.equal(options.credentials, 'omit');
    assert.equal(options.referrerPolicy, 'no-referrer');
    assert.equal(app.notifications.at(-1).title, 'Added to Vise');
    assert.ok(!JSON.stringify(app.notifications).includes('private'));
    assert.ok(!JSON.stringify(app.notifications).includes(SECRET));
  });
}

test('settings are read on every click and an API endpoint suffix is normalized', async () => {
  const app = load();
  await app.events.link({ menuItemId: 'vise-send-link', linkUrl: 'https://example.com/a' });
  app.state.daemonUrl = 'http://localhost:9090/api/downloads';
  app.state.authToken = 'replacement-test-token-0123456789abcdef';
  await app.events.link({ menuItemId: 'vise-send-link', linkUrl: 'https://example.com/b' });
  assert.equal(app.requests[1].url, 'http://localhost:9090/api/downloads');
  assert.equal(app.requests[1].options.headers.Authorization, `Bearer ${app.state.authToken}`);
});

test('unsupported links and missing credentials never cause a network request', async () => {
  for (const linkUrl of ['javascript:alert(1)', 'file:///private', 'https://name:password@example.com/', 'x'.repeat(8193)]) {
    const app = load();
    await app.events.link({ menuItemId: 'vise-send-link', linkUrl });
    assert.equal(app.requests.length, 0);
    assert.equal(app.notifications[0].title, 'Could not send to Vise');
  }
  const app = load({ saved: {} });
  await app.events.link({ menuItemId: 'other', linkUrl: 'https://example.com/' });
  assert.equal(app.notifications.length, 0);
  await app.events.link({ menuItemId: 'vise-send-link', linkUrl: 'https://example.com/' });
  assert.equal(app.requests.length, 0);
  assert.match(app.notifications[0].message, /VISE_API_TOKEN/);
});

test('stored endpoint tampering cannot send a token to a non-loopback host or a redirect', async () => {
  for (const daemonUrl of ['https://127.0.0.1:8080', 'http://evil.example', 'http://localhost.evil.example', 'http://name:secret@localhost:8080', 'http://localhost:8080/other', 'http://localhost:8080?token=secret', 'http://localhost:0']) {
    const app = load({ saved: { daemonUrl, authToken: SECRET } });
    await app.events.link({ menuItemId: 'vise-send-link', linkUrl: 'https://example.com/' });
    assert.equal(app.requests.length, 0, daemonUrl);
    assert.ok(!app.notifications[0].message.includes(SECRET));
  }
});

test('HTTP and connection failures notify without retrying POST or exposing response secrets', async () => {
  for (const status of [400, 401, 403, 500]) {
    const app = load({ fetcher: async () => ({ ok: false, status, text: async () => SECRET }) });
    await app.events.link({ menuItemId: 'vise-send-link', linkUrl: 'https://example.com/' });
    assert.equal(app.requests.length, 1);
    assert.equal(app.notifications[0].title, 'Could not send to Vise');
    assert.ok(!app.notifications[0].message.includes(SECRET));
  }
  const app = load({ fetcher: async () => { throw new TypeError(SECRET); } });
  await app.events.link({ menuItemId: 'vise-send-link', linkUrl: 'https://example.com/' });
  assert.match(app.notifications[0].message, /Cannot reach Vise/);
  assert.ok(!app.notifications[0].message.includes(SECRET));
});

test('ambiguous request timeouts instruct the user to check the queue rather than retry', async () => {
  const app = load({ timer: fn => { queueMicrotask(fn); return undefined; }, fetcher: (_url, options) => new Promise((_resolve, reject) => {
    options.signal.addEventListener('abort', () => reject(new Error('aborted')), { once: true });
  }) });
  await app.events.link({ menuItemId: 'vise-send-link', linkUrl: 'https://example.com/' });
  assert.equal(app.requests.length, 1);
  assert.match(app.notifications[0].message, /queue/);
});

test('options save locally, normalize the endpoint, and never fetch on load or save', async () => {
  const nodes = new Map();
  for (const id of ['settings-form', 'status', 'save', 'test', 'daemon-url', 'auth-token', 'show-token', 'forward-cookies', 'forward-referer', 'grant-site', 'site-url', 'site-grants', 'revoke-sites']) {
    nodes.set(id, { value: '', type: id === 'auth-token' ? 'password' : '', disabled: true, dataset: {}, listeners: {},
      addEventListener(event, fn) { this.listeners[event] = fn; }, setAttribute() {}, reportValidity() { return true; } });
  }
  const document = { getElementById: id => nodes.get(id) };
  const app = load({ document });
  app.run('options.js');
  await new Promise(setImmediate);
  assert.equal(nodes.get('auth-token').value, SECRET);
  assert.equal(app.requests.length, 0);
  nodes.get('daemon-url').value = 'http://localhost:9091/api/downloads';
  await nodes.get('settings-form').listeners.submit({ preventDefault() {} });
  assert.equal(app.state.daemonUrl, 'http://localhost:9091');
  assert.equal(app.state.authToken, SECRET);
  assert.equal(app.requests.length, 0);
  assert.equal(nodes.get('save').disabled, false);
  assert.ok(!nodes.get('status').textContent.includes(SECRET));
  nodes.get('show-token').listeners.click();
  assert.equal(nodes.get('auth-token').type, 'text');
  await nodes.get('test').listeners.click();
  assert.equal(app.requests.length, 1);
  assert.equal(app.requests[0].url, 'http://localhost:9091/api/config');
  assert.equal(app.requests[0].options.method, 'GET');
  assert.match(nodes.get('status').textContent, /Connected and authenticated/);
  nodes.get('auth-token').value = 'not-a-valid-token';
  await nodes.get('settings-form').listeners.submit({ preventDefault() {} });
  assert.equal(app.state.authToken, SECRET);
  assert.equal(app.requests.length, 1);
  assert.equal(nodes.get('status').dataset.error, 'true');
});

const click = {menuItemId: 'vise-send-link', linkUrl: 'https://example.com/files/a', pageUrl: 'https://example.com/page', frameId: 0};
const tab = {id: 7, cookieStoreId: 'container'};
const cookie = {name: 'session', value: 'COOKIE_SENTINEL_49682', domain: 'example.com', path: '/files', secure: true, httpOnly: true, hostOnly: true, session: false, expirationDate: 1999999999, storeId: 'container'};

test('forwarding is off by default and does not inspect cookies or page URL', async () => {
  const app = load({cookies: [cookie]});
  await app.events.link(click, tab);
  assert.deepEqual(JSON.parse(app.requests[0].options.body), {url: click.linkUrl});
  assert.equal(app.cookieReads.length, 0);
});
test('opt-in forwards scoped cookies and click referer in JSON only with no sentinel leaks', async () => {
  const referer = 'https://example.com/REFERER_SENTINEL_84732';
  const app = load({permissions: true, cookies: [cookie, {...cookie, name: 'partitioned', partitionKey: {topLevelSite: 'https://example.com', hasCrossSiteAncestor: false}}], saved: {authToken: SECRET, forwardCookies: true, forwardingSites: ['https://example.com/*'], forwardReferer: true}});
  await app.events.link({...click, pageUrl: referer}, tab);
  assert.equal(app.requests.length, 1);
  const body = JSON.parse(app.requests[0].options.body);
  assert.equal(body.referer, referer);
  assert.equal(body.cookies.length, 2);
  assert.deepEqual(body.cookies[0], {name: cookie.name, value: cookie.value, domain: cookie.domain, path: cookie.path, secure: true, httpOnly: true, hostOnly: true, expires: cookie.expirationDate});
  assert.equal(app.requests[0].options.headers.Authorization, `Bearer ${SECRET}`);
  assert.ok(!JSON.stringify(body).includes(SECRET));
  for (const query of app.cookieReads) { assert.equal(query.url, click.linkUrl); assert.equal(query.storeId, tab.cookieStoreId); }
  for (const secret of [referer, cookie.value, SECRET]) {
    assert.ok(!JSON.stringify([app.notifications, app.logs, app.requests.map(r => r.url)]).includes(secret));
    if (secret !== SECRET) assert.ok(!JSON.stringify(app.state).includes(secret));
  }
});
test('missing permission or ambiguous store/partition blocks forwarding and offers explicit URL-only send', async () => {
  for (const options of [{permissions: false}, {permissions: true, stores: []}, {permissions: true, stores: [{id: 'wrong', tabIds: [7]}]}, {permissions: true, partition: undefined}]) {
    const app = load({...options, saved: {authToken: SECRET, forwardCookies: true, forwardingSites: ['https://example.com/*'], forwardReferer: true}});
    if (options.partition === undefined && 'partition' in options) delete app.context.chrome.cookies.getPartitionKey;
    await app.events.link(click, tab);
    assert.equal(app.requests.length, 0);
    assert.match(app.notifications.at(-1).message, /URL only/);
    await app.events.link({...click, menuItemId: 'vise-send-url-only'}, tab);
    assert.deepEqual(JSON.parse(app.requests[0].options.body), {url: click.linkUrl});
  }
});
test('referer opt-in is independent and uses the click page URL, never the tab URL', async () => {
  const app = load({permissions: true, saved: {authToken: SECRET, forwardReferer: true, forwardingSites: ['https://example.com/*']}});
  await app.events.link(click, {...tab, url: 'https://elsewhere.example/'});
  assert.deepEqual(JSON.parse(app.requests[0].options.body), {url: click.linkUrl, referer: click.pageUrl});
  assert.equal(app.cookieReads.length, 0);
});
test('raw browser errors never leak through notifications', async () => {
  const app = load({fetcher: async () => { throw new Error(cookie.value); }});
  await app.events.link(click, tab);
  assert.ok(!JSON.stringify([app.notifications, app.logs]).includes(cookie.value));
});

test('site permissions are requested only by the explicit options button, with no wildcard host or captured data', async () => {
  const nodes = new Map();
  for (const id of ['settings-form','status','save','test','daemon-url','auth-token','show-token','forward-cookies','forward-referer','grant-site','site-url','site-grants','revoke-sites']) {
    nodes.set(id, {value: '', checked: false, dataset: {}, listeners: {}, addEventListener(type, fn) { this.listeners[type] = fn; }, setAttribute() {}});
  }
  const app = load({document: {getElementById: id => nodes.get(id)}});
  app.run('options.js'); await new Promise(setImmediate);
  assert.equal(app.grants.length, 0);
  nodes.get('site-url').value = 'https://example.com:8443/a?private=HEADER_SENTINEL_71492';
  nodes.get('forward-cookies').checked = true;
  await nodes.get('grant-site').listeners.click();
  assert.deepEqual(JSON.parse(JSON.stringify(app.grants)), [{origins: ['https://example.com/*'], permissions: ['cookies']}]);
  assert.equal(nodes.get('site-url').value, '');
  nodes.get('forward-referer').checked = true;
  await nodes.get('settings-form').listeners.submit({preventDefault() {}});
  assert.equal(app.state.forwardCookies, true); assert.equal(app.state.forwardReferer, true);
  assert.deepEqual(JSON.parse(JSON.stringify(app.state.forwardingSites)), ['https://example.com/*']);
  assert.ok(!JSON.stringify(app.state).includes('SENTINEL'));
  await nodes.get('revoke-sites').listeners.click();
  await app.events.link(click, tab);
  assert.equal(app.requests.length, 0);
});
test('unknown partitions, foreign stores and out-of-scope cookie results fail closed with no leak', async () => {
  for (const bad of [
    {...cookie, storeId: 'other'}, {...cookie, path: '/not-files'}, {...cookie, domain: 'elsewhere.example'},
    {...cookie, firstPartyDomain: 'other.example'}, {...cookie, expirationDate: 1},
    {...cookie, partitionKey: {topLevelSite: 'https://other.example', hasCrossSiteAncestor: false}},
    {...cookie, partitionKey: {topLevelSite: 'https://example.com', hasCrossSiteAncestor: true}},
  ]) {
    const app = load({permissions: true, cookies: [bad], saved: {authToken: SECRET, forwardCookies: true, forwardingSites: ['https://example.com/*']}});
    await app.events.link(click, tab);
    assert.equal(app.requests.length, 0);
    assert.ok(!JSON.stringify([app.state, app.logs, app.notifications]).includes(cookie.value));
    assert.match(app.notifications.at(-1).message, /URL only/);
  }
});
test('session and domain cookies preserve scope metadata without inventing expiry', async () => {
  const app = load({permissions: true, cookies: [{...cookie, domain: '.example.com', hostOnly: false, path: '/', secure: false, httpOnly: false, session: true, expirationDate: undefined}], saved: {authToken: SECRET, forwardCookies: true, forwardingSites: ['https://example.com/*']}});
  await app.events.link(click, tab);
  const sent = JSON.parse(app.requests[0].options.body).cookies[0];
  assert.deepEqual(sent, {name:cookie.name, value:cookie.value, domain:'.example.com', path:'/', secure:false, httpOnly:false, hostOnly:false, expires:null});
});

test('required loopback host access never substitutes for per-site forwarding consent', async () => {
  const app = load({permissions: true, saved: {authToken: SECRET, forwardReferer: true}});
  await app.events.link({...click, linkUrl: 'http://localhost/download'}, tab);
  assert.equal(app.requests.length, 0);
  assert.equal(app.cookieReads.length, 0);
});

test('wildcard sites cannot turn the per-site permission request into broad host access', () => {
  const app = load();
  for (const site of ['https://*.example.com/', 'https://*/']) {
    assert.throws(() => app.context.ViseExtension.sitePattern(site), /wildcards/);
  }
  assert.equal(app.grants.length, 0);
});
