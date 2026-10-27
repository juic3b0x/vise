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
