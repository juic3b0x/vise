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
