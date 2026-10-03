const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const babel = require('@babel/core');
const { JSDOM } = require('jsdom');
const dom = new JSDOM('<html><body></body></html>', { url: 'https://oj.example/' });
global.window = dom.window;
global.document = dom.window.document;
global.localStorage = window.localStorage;
Object.defineProperty(global, 'navigator', { configurable: true, value: window.navigator });
const axios = require('axios');
const Vue = require('vue');
Vue.config.productionTip = false;
Vue.config.devtools = false;
Vue.prototype.$notify = { error() {} };
const { isLocalApiUrl } = require('../src/common/security');

function load(file, mocks = {}) {
  const source = fs.readFileSync(path.join(__dirname, '../src', file), 'utf8');
  const code = babel.transformSync(source, {
    configFile: false, babelrc: false, plugins: ['@babel/plugin-transform-modules-commonjs'],
  }).code;
  const module = { exports: {} };
  const resolve = id => Object.hasOwn(mocks, id) ? mocks[id] : require(id);
  new Function('require', 'module', 'exports', code)(resolve, module, module.exports);
  return module.exports.default || module.exports;
}

function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

function setupApi(dispatch = () => Promise.resolve(), onCommit = () => {}) {
  const commits = [], routes = [];
  const getters = { sessionVersion: 0, get token() { return localStorage.getItem('token'); } };
  load('common/api.js', {
    '@/common/security': { isLocalApiUrl }, '@/common/message': { error() {} },
    '@/router': { push: route => routes.push(route) }, '@/common/utils': {},
    '@/i18n': { t: key => key },
    '@/store': {
      dispatch,
      getters,
      commit(name, value) {
        commits.push([name, value]);
        if (name === 'refreshUserToken') localStorage.setItem('token', value);
        if (name === 'clearUserInfoAndToken') localStorage.removeItem('token');
        onCommit(name, value);
      },
    },
  });
  return { commits, routes, getters };
}

function response(config, headers = {}, data = { status: 200 }) {
  return { config, headers, data, status: 200, statusText: 'OK' };
}

function failure(config, status, data = { msg: 'denied' }) {
  const res = { ...response(config, {}, data), status };
  return new axios.AxiosError('denied', 'ERR_BAD_RESPONSE', config, null, res);
}

test.beforeEach(() => {
  axios.interceptors.request.clear(); axios.interceptors.response.clear(); localStorage.clear();
});
test.after(() => dom.window.close());

test('external responses and login URLs cannot overwrite the current session', async () => {
  localStorage.setItem('token', 'current-session');
  const { commits } = setupApi();
  for (const url of ['https://external.example/api/data', 'https://oj.example/api/login?from=home']) {
    await axios.get(url, { adapter: async config => {
      assert.equal(config.headers.get('Authorization'), undefined);
      return response(config, { 'refresh-token': 'true', authorization: 'untrusted-token' });
    } });
  }
  assert.equal(localStorage.getItem('token'), 'current-session');
  assert.deepEqual(commits, []);
});

test('late refreshes cannot restore a logged-out session or replace another login', async () => {
  setupApi();
  for (const nextToken of [null, 'new-account']) {
    localStorage.setItem('token', 'old-session');
    const started = deferred(), pending = deferred();
    const request = axios.get('/api/private', { adapter: config => { started.resolve(config); return pending.promise; } });
    const config = await started.promise;
    if (nextToken) localStorage.setItem('token', nextToken); else localStorage.removeItem('token');
    pending.resolve(response(config, { 'refresh-token': 'true', authorization: 'old-refreshed' }));
    await request;
    assert.equal(localStorage.getItem('token'), nextToken);
  }
});

test('current refresh is accepted but a missing Authorization response header is ignored', async () => {
  localStorage.setItem('token', 'current'); setupApi();
  await axios.get('/api/private', { adapter: async config => response(config, { 'refresh-token': 'true' }) });
  assert.equal(localStorage.getItem('token'), 'current');
  await axios.get('/api/private', { adapter: async config => response(config, { 'refresh-token': 'true', authorization: 'refreshed' }) });
  assert.equal(localStorage.getItem('token'), 'refreshed');
});

test('a stale 401 cannot log out a newer session; a current empty 401 still logs out', async () => {
  localStorage.setItem('token', 'old');
  const { commits } = setupApi();
  const started = deferred(), pending = deferred();
  const request = axios.get('/api/private', { adapter: config => { started.resolve(config); return pending.promise; } });
  const rejected = assert.rejects(request);
  const config = await started.promise;
  localStorage.setItem('token', 'new');
  pending.reject(failure(config, 401));
  await rejected;
  assert.equal(localStorage.getItem('token'), 'new');
  assert.deepEqual(commits, []);
  await assert.rejects(axios.get('/api/private', { adapter: async config => { throw failure(config, 401, null); } }));
  assert.equal(localStorage.getItem('token'), null);
});

test('parallel forbidden responses share one auth refresh and auth failures do not recurse', async () => {
  localStorage.setItem('token', 'current');
  let dispatches = 0;
  const pending = deferred();
  setupApi(() => { dispatches++; return pending.promise; });
  const reject403 = config => Promise.reject(failure(config, 403));
  await Promise.all([
    assert.rejects(axios.get('/api/admin/problem', { adapter: reject403 })),
    assert.rejects(axios.get('/api/admin/contest', { adapter: reject403 })),
    assert.rejects(axios.get('/api/get-user-auth-info', { adapter: reject403 })),
  ]);
  assert.equal(dispatches, 1);
  pending.reject(new Error('auth lookup failed'));
  await new Promise(resolve => setImmediate(resolve));
  await assert.rejects(axios.get('/api/get-user-auth-info', { adapter: reject403 }));
  assert.equal(dispatches, 1);
});

test('empty successful responses are valid', async () => {
  setupApi();
  const result = await axios.get('/api/empty', { adapter: async config => response(config, {}, null) });
  assert.equal(result.data, null);
});

test('an old account auth refresh cannot block or clear the new account refresh', async () => {
  localStorage.setItem('token', 'old');
  const pending = [deferred(), deferred()];
  let dispatches = 0;
  const { getters } = setupApi(() => pending[dispatches++].promise);
  const denied = () => axios.get('/api/admin/problem', { adapter: async config => { throw failure(config, 403); } });
  await assert.rejects(denied());
  localStorage.setItem('token', 'new'); getters.sessionVersion++;
  await assert.rejects(denied());
  assert.equal(dispatches, 2);
  pending[0].resolve();
  await new Promise(resolve => setImmediate(resolve));
  await assert.rejects(denied());
  assert.equal(dispatches, 2);
  pending[1].resolve();
  await new Promise(resolve => setImmediate(resolve));
});

function userModule(api) {
  return load('store/user.js', {
    '@/common/constants': { USER_TYPE: {} }, '@/common/storage': load('common/storage.js'),
    '@/i18n/language': { getSavedLanguage: () => 'zh-CN', LANGUAGE_STORAGE_KEY: 'Web_Language' },
    '@/common/api': api,
  });
}

test('auth refresh rejects failed requests instead of leaving callers pending', { timeout: 1000 }, async () => {
  const problem = new Error('network unavailable');
  const user = userModule({ getUserAuthInfo: () => Promise.reject(problem) });
  await assert.rejects(user.actions.refreshUserAuthInfo({ state: { token: 'current' }, commit() {} }), problem);
});

test('late role responses do not modify a different account', async () => {
  localStorage.setItem('token', 'old');
  const pending = deferred();
  const user = userModule({ getUserAuthInfo: () => pending.promise });
  const state = { token: 'old', sessionVersion: 0, userInfo: { roleList: ['user'] } }, commits = [];
  const task = user.actions.refreshUserAuthInfo({ state, commit: (...args) => commits.push(args) });
  user.mutations.changeUserToken(state, 'new');
  pending.resolve({ data: { data: { roles: ['admin'] } } });
  await task;
  assert.deepEqual(commits, []);
});

test('auth roles still update when the permissions response renews the same session', async () => {
  localStorage.setItem('token', 'current');
  const api = { getUserAuthInfo: () => axios.get('/api/get-user-auth-info', {
    adapter: async config => response(config,
      { 'refresh-token': 'true', authorization: 'renewed' }, { status: 200, data: { roles: ['user'] } }),
  }) };
  const user = userModule(api), state = user.state;
  state.userInfo = { roleList: ['admin'] };
  const commit = (name, value) => user.mutations[name](state, value);
  setupApi(undefined, commit);
  await user.actions.refreshUserAuthInfo({ state, commit });
  assert.equal(state.token, 'renewed');
  assert.equal(state.sessionVersion, 0);
  assert.deepEqual(state.userInfo.roleList, ['user']);
});

test('role refresh accepts a concurrent renewal but rejects logout followed by a new login', async () => {
  for (const changeSession of [false, true]) {
    localStorage.setItem('token', 'current');
    const pending = deferred(), user = userModule({ getUserAuthInfo: () => pending.promise });
    const state = user.state; state.userInfo = { roleList: ['user'] };
    const commits = [];
    const task = user.actions.refreshUserAuthInfo({ state, commit: (...args) => commits.push(args) });
    if (changeSession) {
      user.mutations.clearUserInfoAndToken(state);
      user.mutations.changeUserToken(state, 'new-login');
    } else {
      user.mutations.refreshUserToken(state, 'renewed');
    }
    pending.resolve({ data: { data: { roles: ['admin'] } } });
    await task;
    assert.equal(commits.length, changeSession ? 0 : 1);
  }
});

test('damaged cache does not break startup and false or zero values are preserved', () => {
  localStorage.setItem('userInfo', 'not JSON');
  const storage = load('common/storage.js');
  assert.equal(storage.get('userInfo'), null);
  storage.set('flag', false); storage.set('count', 0);
  assert.equal(storage.get('flag'), false); assert.equal(storage.get('count'), 0);
  assert.equal(userModule({}).state.userInfo, null);
});
