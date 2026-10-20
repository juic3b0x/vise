'use strict';

(() => {
  const { api, call, defaults, settings, request } = globalThis.ViseExtension;
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
    return settings({ daemonUrl: $('daemon-url').value.trim(), authToken: $('auth-token').value });
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
    } catch (error) { message(error.message, true); }
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
    } catch (error) { message(error.message, true); }
    finally { busy(false); }
  });
  $('show-token').addEventListener('click', () => {
    const visible = $('auth-token').type === 'password';
    $('auth-token').type = visible ? 'text' : 'password';
    $('show-token').textContent = visible ? 'Hide' : 'Show';
    $('show-token').setAttribute('aria-pressed', String(visible));
  });
  (async () => {
    try {
      const saved = await call(api.storage.local, 'get', defaults);
      $('daemon-url').value = saved.daemonUrl;
      $('auth-token').value = saved.authToken;
      message('Set your daemon address and token, then save.');
      busy(false);
    } catch { message('Settings could not be loaded. Reload this page to try again.', true); }
  })();
})();
