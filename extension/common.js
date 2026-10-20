'use strict';

// Classic scripts work in both Chromium's worker and Firefox's event page.
globalThis.ViseExtension = (() => {
  const api = globalThis.chrome;
  const defaults = Object.freeze({ daemonUrl: 'http://127.0.0.1:8080', authToken: '' });

  function call(target, method, ...args) {
    return new Promise((resolve, reject) => {
      target[method](...args, result => {
        const failure = api.runtime.lastError;
        if (failure) reject(new Error('The browser could not complete this operation.'));
        else resolve(result);
      });
    });
  }

  function settings(values) {
    let address;
    try { address = new URL(values.daemonUrl); }
    catch { throw new Error('Enter a valid daemon URL, such as http://127.0.0.1:8080.'); }
    if (address.protocol !== 'http:' || !['127.0.0.1', 'localhost'].includes(address.hostname)
      || address.username || address.password || address.search || address.hash
      || !['/', '/api/downloads', '/api/downloads/'].includes(address.pathname)
      || address.port === '0') {
      throw new Error('Use an HTTP daemon URL on 127.0.0.1 or localhost, with no credentials or query string.');
    }
    const token = typeof values.authToken === 'string' ? values.authToken.trim() : '';
    if (!/^[A-Za-z0-9_-]{32,256}$/.test(token)) {
      throw new Error('Paste the 32–256 character VISE_API_TOKEN from ~/.vise/config.toml.');
    }
    return { daemonUrl: address.origin, authToken: token };
  }

  function downloadUrl(value) {
    if (typeof value !== 'string' || value.length > 8192) throw new Error('This link is not a supported download URL.');
    let address;
    try { address = new URL(value); }
    catch { throw new Error('This link is not a supported download URL.'); }
    if (!['http:', 'https:'].includes(address.protocol) || address.username || address.password) {
      throw new Error('Choose an HTTP(S) link without embedded credentials.');
    }
    return address.href;
  }

  async function request(configuration, path, body) {
    const selected = settings(configuration);
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    try {
      const response = await fetch(selected.daemonUrl + path, {
        method: body === undefined ? 'GET' : 'POST',
        headers: {
          Authorization: `Bearer ${selected.authToken}`,
          ...(body === undefined ? {} : { 'Content-Type': 'application/json' }),
        },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: controller.signal,
        redirect: 'error',
        credentials: 'omit',
        referrerPolicy: 'no-referrer',
        cache: 'no-store',
      });
      if (response.status === 401 || response.status === 403) {
        throw new Error('Vise denied access. Check the daemon URL and bearer token in extension settings.');
      }
      if (!response.ok) {
        throw new Error(`Vise rejected the request (HTTP ${response.status}). Check the daemon before trying again.`);
      }
      return response;
    } catch (error) {
      if (controller.signal.aborted) {
        throw new Error('Vise timed out. Check its queue before sending this link again.');
      }
      if (error instanceof TypeError) {
        throw new Error('Cannot reach Vise. Start vise --daemon and check its address and browser site permissions.');
      }
      throw error;
    } finally { clearTimeout(timeout); }
  }

  async function readSettings() {
    return settings(await call(api.storage.local, 'get', defaults));
  }

  return Object.freeze({ api, call, defaults, settings, downloadUrl, request, readSettings });
})();
