import assert from 'node:assert/strict';
import {test} from 'node:test';
import {parseUrls, readUrlFile, MAX_INPUT_BYTES, MAX_URLS} from '../src/dashboard-input.mjs';

test('pasted lists preserve order, repeats and fragments while skipping comments and blank lines', () => {
  assert.deepEqual(parseUrls('\uFEFF# urls\r\n\n https://example.com/a#x \r\n\t# skip\nhttps://example.com/a#x\nhttp://example.com/b'), ['https://example.com/a#x', 'https://example.com/a#x', 'http://example.com/b']);
});
test('empty, malformed, credential-bearing and oversized lists fail with safe line diagnostics', () => {
  for (const value of ['', '# no entries', 'ftp://example.com/a', 'not-a-url', 'https://user:secret@example.com/a']) {
    assert.throws(() => parseUrls(value), error => !error.message.includes('secret'));
  }
  assert.throws(() => parseUrls('https://example.com/a\nbad'), /Line 2/);
  assert.throws(() => parseUrls('#'.repeat(MAX_INPUT_BYTES + 1)), /1 MiB/);
  assert.throws(() => parseUrls('https://example.com/a\n'.repeat(MAX_URLS + 1)), /1000/);
  assert.throws(() => parseUrls('https://example.com/' + 'é'.repeat(4096)), /8192 bytes/);
});
test('file loading validates extension, size, UTF-8, and entries before accepting content', async () => {
  const file = (bytes, name='urls.txt', size=bytes.length) => ({name,size,arrayBuffer:async () => Uint8Array.from(bytes).buffer});
  const content = '# file\nhttps://example.com/a';
  const result = await readUrlFile(file(new TextEncoder().encode(content)));
  assert.equal(result.content, content);
  assert.deepEqual(result.urls, ['https://example.com/a']);
  await assert.rejects(readUrlFile(file([0xff])), /UTF-8/);
  await assert.rejects(readUrlFile(file([], 'urls.csv')), /.txt/);
  await assert.rejects(readUrlFile(file([], 'urls.txt', MAX_INPUT_BYTES+1)), /1 MiB/);
  await assert.rejects(readUrlFile(file(new Uint8Array(MAX_INPUT_BYTES+1), 'urls.txt', 1)), /1 MiB/);
  await assert.rejects(readUrlFile(file([])), /at least one URL/);
});

import {requestFields, clearRequestFields, requestSummary} from '../src/dashboard-input.mjs';
const credentialFields = () => ({
  headers: {value: 'X-Session: HEADER_SENTINEL_96215\nAuthorization: Bearer HEADER_SENTINEL_51286'},
  referer: {value: 'https://example.com/REFERER_SENTINEL_91263'},
  userAgent: {value: 'UA_SENTINEL_12643'},
  cookies: {value: JSON.stringify([{name: 'session', value: 'COOKIE_SENTINEL_61529', domain: 'example.com', path: '/', secure: true, httpOnly: true, hostOnly: true, expires: null}])},
});
test('advanced credentials are outbound only, use API fields and clear every input', () => {
  const fields = credentialFields();
  const payload = requestFields(fields, ['https://example.com/a']);
  assert.equal(payload.headers[0].value, 'HEADER_SENTINEL_96215');
  assert.equal(payload.cookies[0].value, 'COOKIE_SENTINEL_61529');
  clearRequestFields(fields);
  for (const field of Object.values(fields)) assert.equal(field.value, '');
  assert.deepEqual(requestFields(fields, ['https://example.com/a']), {});
});
test('mixed-origin credential batches and raw Cookie headers reject without values', () => {
  const fields = credentialFields();
  assert.throws(() => requestFields(fields, ['https://example.com/a', 'https://other.example/a']), /Split/);
  for (const name of ['Cookie', 'Set-Cookie']) {
    fields.headers.value = name + ': COOKIE_SENTINEL_61529';
    assert.throws(() => requestFields(fields, ['https://example.com/a']), e => /structured cookies/.test(e.message) && !e.message.includes('SENTINEL'));
  }
});
test('summary renders only names, counts and presence flags even with unexpected snapshot fields', () => {
  const summary = requestSummary({header_names: ['x-session'], header_count: 1, has_custom_headers: true, has_referer: true, has_user_agent: true, has_cookies: true, referer: 'REFERER_SENTINEL', cookies: 'COOKIE_SENTINEL', headers: 'HEADER_SENTINEL'});
  assert.match(summary, /x-session/);
  assert.match(summary, /Referer/);
  assert.ok(!summary.includes('SENTINEL'));
});

// Keep controller regressions in the existing dependency-free CI entry point.
import './dashboard-credentials.test.mjs';

test('credential batches normalize origins; user agent alone does not restrict batches', () => {
  let fields = credentialFields();
  assert.ok(requestFields(fields, ['https://EXAMPLE.com/a', 'https://example.com:443/b']).headers);
  assert.throws(() => requestFields(fields, ['https://example.com/a', 'https://example.com./b']), /Split/);
  assert.throws(() => requestFields(fields, ['https://example.com/a', 'https://example.com:444/b']), /Split/);
  clearRequestFields(fields); fields.userAgent.value = 'Agent/1.0';
  assert.deepEqual(requestFields(fields, ['https://a.example/a', 'https://b.example/a']), {user_agent: 'Agent/1.0'});
});
test('credential input parse errors and injection are value-free', () => {
  const fields = credentialFields();
  fields.cookies.value = '{ COOKIE_SENTINEL_61529';
  assert.throws(() => requestFields(fields, ['https://example.com/a']), e => !e.message.includes('SENTINEL'));
  fields.cookies.value = '';
  fields.headers.value = 'X-Session: HEADER_SENTINEL_96215\rINJECT';
  assert.throws(() => requestFields(fields, ['https://example.com/a']), e => !e.message.includes('SENTINEL'));
});
