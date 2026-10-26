const test = require('node:test');
const assert = require('node:assert/strict');
const { startFrontendUpdates } = require('../src/common/frontendUpdates');
const original = 'a'.repeat(32);
const updated = 'b'.repeat(32);
const flush = async () => { for (let n = 0; n < 6; n++) await Promise.resolve(); };

function fixture(t, options = {}) {
  let now = 100000;
  t.mock.method(Date, 'now', () => now);
  const events = new Map();
  const timeouts = new Map();
  let nextId = 0;
  const values = new Map();
  const state = { remote: original, requests: [], reloads: 0, notices: 0, busy: false };
  const listen = (name, callback) => events.set(name, callback);
  const remove = (name, callback) => { if (events.get(name) === callback) events.delete(name); };
  const doc = { visibilityState: 'visible', activeElement: null,
    addEventListener: listen, removeEventListener: remove, querySelector: () => state.busy };
  let interval;
  const win = {
    fetch: async (url, config) => { state.requests.push({ url, config }); return { ok: true, json: async () => ({ version: state.remote }) }; },
    setTimeout: callback => { const id = ++nextId; timeouts.set(id, callback); return id; },
    clearTimeout: id => timeouts.delete(id), setInterval: callback => { interval = callback; return 1; },
    clearInterval: () => { interval = null; }, addEventListener: listen, removeEventListener: remove,
    location: { reload: () => state.reloads++ },
    sessionStorage: { getItem: key => values.get(key), setItem: (key, value) => values.set(key, value) },
  };
  const router = { currentRoute: { path: '/home', name: 'Home' },
    afterEach: hook => { router.after = hook; return () => { router.after = null; }; },
    onError: hook => { router.error = hook; return () => { router.error = null; }; } };
  Object.assign(state, options);
  const stop = startFrontendUpdates({ version: original, router, win, doc, onDeferred: () => state.notices++ });
  t.after(stop);
  return { state, win, doc, router, events, values, stop,
    async tick() { now += 60000; if (interval) await interval(); await flush(); },
    async navigate(path) {
      const from = router.currentRoute; router.currentRoute = { path };
      router.after(router.currentRoute, from);
      const callbacks = [...timeouts.values()]; timeouts.clear(); callbacks.forEach(callback => callback());
      await flush();
    } };
}

test('an idle open tab notices a deployment and reloads once using an uncached manifest', async t => {
  const f = fixture(t); await flush();
  assert.equal(f.state.reloads, 0);
  f.state.remote = updated; await f.tick();
  assert.equal(f.state.reloads, 1);
  assert.match(f.state.requests[0].url, /^\/version.json\?_=/);
  assert.equal(f.state.requests[0].config.cache, 'no-store');
  await f.tick(); assert.equal(f.state.reloads, 1);
});

test('a stale page detects an already completed deployment at startup', async t => {
  const f = fixture(t, { remote: updated }); await flush();
  assert.equal(f.state.reloads, 1);
});

test('writing code or editing a form defers refresh until leaving the editor', async t => {
  const f = fixture(t); await flush();
  f.router.currentRoute = { path: '/contest/7/problem/A', name: 'ContestProblemDetails' };
  f.state.remote = updated; await f.tick();
  assert.equal(f.state.reloads, 0); assert.equal(f.state.notices, 1);
  await f.tick(); assert.equal(f.state.notices, 1);
  await f.navigate('/admin/problem/edit/123');
  assert.equal(f.state.reloads, 0);
  await f.navigate('/admin/problems');
  assert.equal(f.state.reloads, 1);
});

test('unsaved text and active uploads are preserved without clearing browser storage', async t => {
  const f = fixture(t); await flush();
  f.values.set('token', 'keep-session');
  f.events.get('input')({ target: { closest: () => true } });
  f.state.remote = updated; await f.tick();
  assert.equal(f.state.reloads, 0);
  f.state.busy = true; await f.navigate('/status');
  assert.equal(f.state.reloads, 0);
  f.state.busy = false; await f.tick();
  assert.equal(f.state.reloads, 1); assert.equal(f.values.get('token'), 'keep-session');
});

test('offline, invalid and missing manifests cannot cause a reload loop', async t => {
  const f = fixture(t); await flush();
  f.state.remote = '<html>proxy error</html>'; await f.tick();
  f.win.fetch = async () => ({ ok: false }); await f.tick();
  f.win.fetch = async () => { throw new Error('offline'); }; await f.tick();
  assert.equal(f.state.reloads, 0);
});

test('background tabs wait until visible and stopped watchers release their listeners', async t => {
  const f = fixture(t); await flush();
  f.doc.visibilityState = 'hidden'; f.state.remote = updated; await f.tick();
  assert.equal(f.state.requests.length, 1); assert.equal(f.state.reloads, 0);
  f.doc.visibilityState = 'visible'; await f.events.get('visibilitychange')();
  assert.equal(f.state.reloads, 1);
  f.stop(); assert.equal(f.events.size, 0); assert.equal(f.router.after, null);
});

test('a missing old lazy chunk immediately checks for the new deployment', async t => {
  const f = fixture(t); await flush();
  f.state.remote = updated;
  await f.router.error(new Error('Loading chunk 12 failed.')); await flush();
  assert.equal(f.state.reloads, 1);
});

test('a stale proxy cannot force repeated reloads immediately after an update', async t => {
  const f = fixture(t); await flush();
  f.values.set('sxuoj.frontend-update', JSON.stringify({ at: Date.now() }));
  f.state.remote = updated;
  await f.router.error(new Error('ChunkLoadError')); await flush();
  assert.equal(f.state.reloads, 0);
  await f.tick(); assert.equal(f.state.reloads, 1);
});
