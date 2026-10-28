'use strict';

(() => {
  const { api, call, defaults, settings, request, sitePattern, errorMessage } = globalThis.ViseExtension;
  const $ = id => document.getElementById(id);
  const form = $('settings-form');
  const status = $('status');
  const buttons = [$('save'), $('test')];

  function message(text, failed = false) {
    status.textContent = text;
    status.dataset.error = String(failed);
  }
  function busy(value) {
    for (const button of buttons) button.disabled = value;
    form.setAttribute('aria-busy', String(value));
  }
  function values() {
    return settings({ daemonUrl: $('daemon-url').value.trim(), authToken: $('auth-token').value, forwardCookies: $('forward-cookies').checked, forwardReferer: $('forward-referer').checked });
  }
  form.addEventListener('submit', async event => {
    event.preventDefault();
    busy(true);
    try {
      const selected = values();
      await call(api.storage.local, 'set', selected);
      $('daemon-url').value = selected.daemonUrl;
      $('auth-token').value = selected.authToken;
      message('Settings saved. Right-click an HTTP(S) link and choose “Send to Vise”.');
    } catch (error) { message(errorMessage(error), true); }
    finally { busy(false); }
  });
  $('test').addEventListener('click', async () => {
    if (!form.reportValidity()) return;
    busy(true);
    message('Connecting to Vise…');
    try {
      const response = await request(values(), '/api/config');
      const configuration = await response.json().catch(() => null);
      if (!configuration || typeof configuration.limit_rate !== 'string') {
        throw new Error('The endpoint did not return Vise configuration. Check the daemon URL.');
      }
      message('Connected and authenticated. Save settings to use these values.');
    } catch (error) { message(errorMessage(error), true); }
    finally { busy(false); }
  });
  $('show-token').addEventListener('click', () => {
    const visible = $('auth-token').type === 'password';
    $('auth-token').type = visible ? 'text' : 'password';
    $('show-token').textContent = visible ? 'Hide' : 'Show';
    $('show-token').setAttribute('aria-pressed', String(visible));
  });
  async function showGrants() {
    const granted = await call(api.permissions, 'getAll');
    // Browser owns the grant list; captured request data is never persisted.
    $('site-grants').textContent = `Granted download sites: ${(granted.origins || []).filter(origin => !['http://127.0.0.1/*', 'http://localhost/*'].includes(origin)).join(', ') || 'none'}`;
  }
  $('grant-site').addEventListener('click', async () => {
    try {
      const origins = [sitePattern($('site-url').value.trim())];
      // Invoke request before the first await, while the button gesture is live.
      const granted = await call(api.permissions, 'request', {origins, permissions: $('forward-cookies').checked ? ['cookies'] : []});
      if (granted) {
        const saved = await call(api.storage.local, 'get', {forwardingSites: []});
        const sites = Array.isArray(saved.forwardingSites) ? saved.forwardingSites : [];
        await call(api.storage.local, 'set', {forwardingSites: [...new Set([...sites, ...origins])]});
      }
      $('site-url').value = '';
      message(granted ? 'Site access granted. Save your forwarding preferences separately.' : 'Site access was declined. URL-only sends remain available.', !granted);
      await showGrants();
    } catch { message('Site access could not be granted. Enter an HTTP(S) site and try again.', true); }
  });
  $('revoke-sites').addEventListener('click', async () => {
    try {
      const granted = await call(api.permissions, 'getAll');
      const origins = (granted.origins || []).filter(origin => !['http://127.0.0.1/*', 'http://localhost/*'].includes(origin));
      // Clear consent first, even if the browser refuses to remove a grant.
      await call(api.storage.local, 'set', {forwardingSites: []});
      await call(api.permissions, 'remove', {origins, permissions: ['cookies']});
      await showGrants(); message('Forwarded-site access revoked. URL-only sends remain available.');
    } catch { message('Could not revoke access. Use the browser extension permissions page.', true); }
  });
  (async () => {
    try {
      const saved = await call(api.storage.local, 'get', defaults);
      $('daemon-url').value = saved.daemonUrl;
      $('auth-token').value = saved.authToken;
      $('forward-cookies').checked = saved.forwardCookies === true;
      $('forward-referer').checked = saved.forwardReferer === true;
      await showGrants();
      message('Set your daemon address and token, then save.');
      busy(false);
    } catch { message('Settings could not be loaded. Reload this page to try again.', true); }
  })();
})();
