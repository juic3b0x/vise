// Shared, dependency-free parsing for pasted lists and local .txt files.
export const MAX_INPUT_BYTES = 1024 * 1024;
export const MAX_URLS = 1000;
const encoder = new TextEncoder();

export function parseUrls(content) {
  if (encoder.encode(content).length > MAX_INPUT_BYTES) throw Error('URL lists must be 1 MiB or smaller.');
  const urls = [];
  for (const [index, raw] of content.replace(/^\uFEFF/, '').split('\n').entries()) {
    const value = raw.trim();
    if (!value || value.startsWith('#')) continue;
    const fail = () => Error(`Line ${index + 1}: use an HTTP(S) URL without embedded credentials.`);
    if (encoder.encode(value).length > 8192) throw Error(`Line ${index + 1}: URL exceeds 8192 bytes.`);
    let url;
    try { url = new URL(value); } catch { throw fail(); }
    if (!['http:', 'https:'].includes(url.protocol) || !url.hostname || url.username || url.password) throw fail();
    urls.push(value);
    if (urls.length > MAX_URLS) throw Error(`Submit at most ${MAX_URLS} URLs at a time.`);
  }
  if (!urls.length) throw Error('Add at least one URL. Blank lines and # comments are ignored.');
  return urls;
}

export async function readUrlFile(file) {
  if (!file || !file.name.toLowerCase().endsWith('.txt')) throw Error('Choose one .txt URL-list file.');
  if (file.size > MAX_INPUT_BYTES) throw Error('URL lists must be 1 MiB or smaller.');
  const bytes = await file.arrayBuffer();
  if (bytes.byteLength > MAX_INPUT_BYTES) throw Error('URL lists must be 1 MiB or smaller.');
  let content;
  try { content = new TextDecoder('utf-8', {fatal: true}).decode(bytes); }
  catch { throw Error('The URL-list file must use UTF-8 text.'); }
  return {content, urls: parseUrls(content)};
}

// Only these fields cross the outbound admission boundary. They are never part
// of telemetry, activity entries, or browser storage.
export function clearRequestFields(fields) {
  for (const field of Object.values(fields)) field.value = '';
}
export function requestFields(fields, urls) {
  const payload = {};
  const headers = fields.headers.value;
  if (headers.trim()) {
    payload.headers = headers.split('\n').filter(line => line.trim()).map(line => {
      const colon = line.indexOf(':');
      const name = line.slice(0, colon).trim();
      if (colon < 1 || !/^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/.test(name) || name.length > 128) {
        throw Error('Each header must use Name: value syntax with a valid HTTP header name.');
      }
      if (/^(cookie|set-cookie)$/i.test(name)) throw Error('Use structured cookies below or --cookies FILE, not raw Cookie or Set-Cookie headers.');
      return {name, value: line.slice(colon + 1).replace(/^ +| +$/g, '')};
    });
    if (payload.headers.length > 32) throw Error('Use at most 32 custom headers.');
  }
  if (fields.referer.value.trim()) payload.referer = fields.referer.value.trim();
  if (fields.userAgent.value.trim()) payload.user_agent = fields.userAgent.value.trim();
  if (fields.cookies.value.trim()) {
    try { payload.cookies = JSON.parse(fields.cookies.value); }
    catch { throw Error('Structured cookies must be a JSON array of cookie records.'); }
    if (!Array.isArray(payload.cookies) || payload.cookies.length > 128) throw Error('Use a JSON array of at most 128 structured cookies.');
  }
  for (const header of payload.headers || []) checkText(header.value, 8192);
  if (payload.referer) { checkText(payload.referer, 8192); parseUrls(payload.referer); }
  if (payload.user_agent) checkText(payload.user_agent, 512);
  if (encoder.encode(JSON.stringify(payload)).length > 65536) throw Error('Request settings exceed their size limit.');
  const credentialed = payload.headers?.length || payload.referer || payload.cookies?.length;
  if (credentialed && new Set(urls.map(value => new URL(value).origin)).size > 1) {
    throw Error('Credentialed batches must share one origin (scheme, host and port). Split this batch by origin before submitting.');
  }
  return payload;
}
function checkText(value, max) {
  if (encoder.encode(value).length > max || /[\x00-\x1f\x7f]/.test(value)) throw Error('Request settings exceed a field limit or contain control characters.');
}
export function requestSummary(summary) {
  if (!summary) return '';
  const parts = [];
  // Do not enumerate the object: future private fields must never be rendered.
  const names = Array.isArray(summary.header_names) ? summary.header_names.filter(name => typeof name === 'string' && /^[!#$%&'*+.^_`|~0-9A-Za-z-]{1,128}$/.test(name)).slice(0, 32) : [];
  if (summary.has_custom_headers === true) parts.push(`${Number.isSafeInteger(summary.header_count) ? summary.header_count : names.length} headers${names.length ? ': ' + names.join(', ') : ''}`);
  if (summary.has_referer === true) parts.push('Referer supplied');
  if (summary.has_user_agent === true) parts.push('Custom user agent');
  if (summary.has_cookies === true) parts.push('Cookies supplied');
  return parts.join(' · ');
}
