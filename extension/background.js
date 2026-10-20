'use strict';

if (typeof importScripts === 'function') importScripts('common.js');

(() => {
  const { api, call, downloadUrl, request, readSettings } = globalThis.ViseExtension;
  const MENU = 'vise-send-link';

  async function notify(title, message) {
    try {
      await call(api.notifications, 'create', '', {
        type: 'basic',
        iconUrl: api.runtime.getURL('icons/icon-128.png'),
        title,
        message,
      });
    } catch {
      // A disabled OS notification service must not turn a queued task into an error.
      console.warn('Vise could not display a browser notification.');
    }
  }

  async function registerMenu() {
    try {
      await call(api.contextMenus, 'removeAll');
      await call(api.contextMenus, 'create', {
        id: MENU,
        title: 'Send to Vise',
        contexts: ['link'],
        targetUrlPatterns: ['http://*/*', 'https://*/*'],
      });
    } catch { await notify('Vise setup failed', 'Reload the extension to restore its link menu.'); }
  }

  async function initialize() {
    try {
      if (typeof api.storage.local.setAccessLevel === 'function') {
        await call(api.storage.local, 'setAccessLevel', { accessLevel: 'TRUSTED_CONTEXTS' });
      }
    } catch { await notify('Vise settings unavailable', 'Open extension settings and save the daemon address and token.'); }
    await registerMenu();
  }

  // Register listeners synchronously so suspended MV3 workers wake correctly.
  api.runtime.onInstalled.addListener(initialize);
  api.runtime.onStartup.addListener(registerMenu);
  api.contextMenus.onClicked.addListener(async info => {
    if (info.menuItemId !== MENU) return;
    try {
      const url = downloadUrl(info.linkUrl);
      const configuration = await readSettings();
      // Never retry POST automatically: a lost response can still mean it was queued.
      await request(configuration, '/api/downloads', { url });
      await notify('Added to Vise', 'The link was accepted by your download manager.');
    } catch (error) { await notify('Could not send to Vise', error.message); }
  });
  api.action.onClicked.addListener(async () => {
    try { await call(api.runtime, 'openOptionsPage'); }
    catch { await notify('Vise settings unavailable', 'Open the extension’s settings from your browser’s extension manager.'); }
  });
})();
