'use strict';

// Classic scripts work in both Chromium's worker and Firefox's event page.
globalThis.ViseExtension = (() => {
  const api = globalThis.chrome;
  class SafeError extends Error {}
  const errorMessage = error => error instanceof SafeError ? error.message : 'Could not send to Vise. Check the daemon and its queue before retrying.';
  const defaults = Object.freeze({ daemonUrl: 'http://127.0.0.1:8080', authToken: '', forwardCookies: false, forwardReferer: false, forwardingSites: [] });

  function call(target, method, ...args) {
    return new Promise((resolve, reject) => {
      target[method](...args, result => {
        const failure = api.runtime.lastError;
        if (failure) reject(new SafeError('The browser could not complete this operation.'));
        else resolve(result);
      });
    });
  }

  function settings(values) {
    let address;
    try { address = new URL(values.daemonUrl); }
    catch { throw new SafeError('Enter a valid daemon URL, such as http://127.0.0.1:8080.'); }
    if (address.protocol !== 'http:' || !['127.0.0.1', 'localhost'].includes(address.hostname)
      || address.username || address.password || address.search || address.hash
      || !['/', '/api/downloads', '/api/downloads/'].includes(address.pathname)
      || address.port === '0') {
      throw new SafeError('Use an HTTP daemon URL on 127.0.0.1 or localhost, with no credentials or query string.');
    }
    const token = typeof values.authToken === 'string' ? values.authToken.trim() : '';
    if (!/^[A-Za-z0-9_-]{32,256}$/.test(token)) {
      throw new SafeError('Paste the 32–256 character VISE_API_TOKEN from ~/.vise/config.toml.');
    }
    return { daemonUrl: address.origin, authToken: token, forwardCookies: values.forwardCookies === true, forwardReferer: values.forwardReferer === true };
  }

  function downloadUrl(value) {
    if (typeof value !== 'string' || value.length > 8192) throw new SafeError('This link is not a supported download URL.');
    let address;
    try { address = new URL(value); }
    catch { throw new SafeError('This link is not a supported download URL.'); }
    if (!['http:', 'https:'].includes(address.protocol) || address.username || address.password) {
      throw new SafeError('Choose an HTTP(S) link without embedded credentials.');
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
        throw new SafeError('Vise denied access. Check the daemon URL and bearer token in extension settings.');
      }
      if (!response.ok) {
        throw new SafeError(`Vise rejected the request (HTTP ${response.status}). Check the daemon before trying again.`);
      }
      return response;
    } catch (error) {
      if (controller.signal.aborted) {
        throw new SafeError('Vise timed out. Check its queue before sending this link again.');
      }
      if (error instanceof TypeError) {
        throw new SafeError('Cannot reach Vise. Start vise --daemon and check its address and browser site permissions.');
      }
      if (error instanceof SafeError) throw error;
      throw new SafeError('Could not send to Vise. Check the daemon and its queue before retrying.');
    } finally { clearTimeout(timeout); }
  }

  function sitePattern(value) {
    const url = new URL(downloadUrl(value));
    if (url.hostname.includes('*')) throw new SafeError('Enter one site hostname without wildcards.');
    // Host permissions cannot be restricted by port; never request wildcards.
    return `${url.protocol}//${url.hostname}/*`;
  }

  async function readSettings() {
    const saved = await call(api.storage.local, 'get', defaults);
    const forwardingSites = Array.isArray(saved.forwardingSites) ? saved.forwardingSites.filter(site => {
      try { return typeof site === 'string' && sitePattern(site) === site; } catch { return false; }
    }) : [];
    return {...settings(saved), forwardingSites};
  }

  return Object.freeze({ api, call, defaults, settings, downloadUrl, request, readSettings, sitePattern, errorMessage });
})();

// Transient capture boundary. Cookie values must never enter storage, URLs,
// notifications, logs or daemon Authorization; they exist only in admission JSON.
globalThis.ViseForwarding = (() => {
  const {api, call, sitePattern, downloadUrl} = globalThis.ViseExtension;
  async function capture(configuration, info, tab, url) {
    const permission = {origins: [sitePattern(url)], permissions: configuration.forwardCookies ? ['cookies'] : []};
    if (!configuration.forwardingSites.includes(permission.origins[0]) || !await call(api.permissions, 'contains', permission)) throw Error('Forwarding unavailable.');
    const body = {};
    if (configuration.forwardReferer) {
      body.referer = downloadUrl(info.pageUrl);
      const referer = new URL(body.referer); referer.hash = ''; body.referer = referer.href;
    }
    if (configuration.forwardCookies) body.cookies = await cookiesForClick(info, tab, url);
    return body;
  }
  async function cookiesForClick(info, tab, url) {
    if (!Number.isInteger(tab?.id) || tab.id < 0 || !Number.isInteger(info.frameId) || info.frameId < 0
      || typeof api.cookies?.getPartitionKey !== 'function') throw Error('Cookie context unavailable.');
    const stores = await call(api.cookies, 'getAllCookieStores');
    const matches = stores.filter(store => Array.isArray(store.tabIds) && store.tabIds.includes(tab.id));
    if (matches.length !== 1 || typeof matches[0].id !== 'string'
      || (tab.cookieStoreId !== undefined && tab.cookieStoreId !== matches[0].id)) throw Error('Cookie context unavailable.');
    const storeId = matches[0].id;
    const result = await call(api.cookies, 'getPartitionKey', {tabId: tab.id, frameId: info.frameId});
    const partitionKey = result?.partitionKey;
    if (!partitionKey || typeof partitionKey.topLevelSite !== 'string'
      || typeof partitionKey.hasCrossSiteAncestor !== 'boolean') throw Error('Cookie context unavailable.');
    // Never query all partitions or use the extension's default cookie store.
    const ordinary = await call(api.cookies, 'getAll', {url, storeId});
    const partitioned = await call(api.cookies, 'getAll', {url, storeId, partitionKey});
    if (ordinary.some(cookie => cookie.partitionKey) || partitioned.some(cookie => !samePartition(cookie.partitionKey, partitionKey))) throw Error('Cookie context unavailable.');
    const records = [...ordinary, ...partitioned];
    if (records.length > 128) throw Error('Cookie limit exceeded.');
    const selected = new URL(url);
    return records.map(cookie => cookieRecord(cookie, selected, storeId));
  }
  function samePartition(left, right) {
    return left?.topLevelSite === right.topLevelSite && left.hasCrossSiteAncestor === right.hasCrossSiteAncestor;
  }
  function cookieRecord(cookie, url, storeId) {
    const domain = typeof cookie.domain === 'string' ? cookie.domain.replace(/^\./, '').toLowerCase() : '';
    const host = url.hostname;
    const path = cookie.path;
    if (cookie.storeId !== storeId || (cookie.firstPartyDomain !== undefined && cookie.firstPartyDomain !== '')
      || !domain || !(host === domain || (cookie.hostOnly === false && host.endsWith('.' + domain)))
      || typeof path !== 'string' || !path.startsWith('/')
      || !(url.pathname === path || (url.pathname.startsWith(path) && (path.endsWith('/') || url.pathname[path.length] === '/')))
      || (cookie.secure && url.protocol !== 'https:')
      || ['secure', 'hostOnly', 'httpOnly', 'session'].some(key => typeof cookie[key] !== 'boolean')
      || typeof cookie.name !== 'string' || typeof cookie.value !== 'string'
      || (!cookie.session && (!Number.isFinite(cookie.expirationDate) || cookie.expirationDate <= Date.now() / 1000))) throw Error('Cookie context unavailable.');
    return {name: cookie.name, value: cookie.value, domain: cookie.domain, path,
      secure: cookie.secure, httpOnly: cookie.httpOnly, hostOnly: cookie.hostOnly,
      expires: cookie.session ? null : Math.floor(cookie.expirationDate)};
  }
  return Object.freeze({capture});
})();
