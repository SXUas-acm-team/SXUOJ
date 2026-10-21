const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const babel = require('@babel/core');
const compiler = require('vue-template-compiler');
const { JSDOM } = require('jsdom');
const dom = new JSDOM('<!doctype html><html><body></body></html>', { url: 'https://oj.example/' });
global.window = dom.window; global.document = dom.window.document;
global.localStorage = dom.window.localStorage;
Object.defineProperty(global, 'navigator', { configurable: true, value: dom.window.navigator });

function load(file, mocks = {}) {
  const source = fs.readFileSync(path.join(__dirname, '../src', file), 'utf8');
  const script = file.endsWith('.vue') ? compiler.parseComponent(source).script.content : source;
  const output = babel.transformSync(script, { configFile: false, babelrc: false,
    plugins: ['@babel/plugin-transform-modules-commonjs'] }).code;
  const module = { exports: {} };
  const resolve = id => {
    if (Object.hasOwn(mocks, id)) return mocks[id];
    if (['vue', 'vuex', 'moment'].includes(id)) return require(id);
    if (id === '@/common/constants') return load('common/constants.js');
    if (id === '@/common/message') return { success() {} };
    if (id === '@/common/storage') return { get() {} };
    if (id === '@/i18n/language') return { getSavedLanguage() {} };
    return {};
  };
  new Function('require', 'module', 'exports', output)(resolve, module, module.exports);
  return module.exports.default || module.exports;
}
function stateFor(component, extra = {}) {
  const state = { ...extra };
  Object.assign(state, component.data ? component.data.call(state) : {}, extra);
  for (const [name, method] of Object.entries(component.methods || {})) state[name] = method.bind(state);
  return state;
}
function deferred() { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; }

test('a public contest with identity enabled stays closed until the server grants access', () => {
  const store = load('store/contest.js');
  const state = { contest: { id: 7, auth: 0, status: 0, identityVerificationEnabled: true }, access: null };
  const getters = { isContestAdmin: false, isContestReleased: false, isContestSealed: false, contestStatus: 0 };
  assert.equal(store.getters.canViewContest(state, getters), false);
  assert.equal(store.getters.canSubmit(state, getters, {}, { isAuthenticated: true }), false);
  state.access = { canViewProblems: true, canSubmit: true };
  assert.equal(store.getters.canViewContest(state, getters), true);
  assert.equal(store.getters.canSubmit(state, getters, {}, { isAuthenticated: true }), true);
  getters.isContestReleased = true; state.access = null;
  assert.equal(store.getters.canViewContest(state, getters), true);
});

test('content administrator permissions do not grant system administrator permissions', () => {
  const user = load('store/user.js');
  const getters = { userInfo: { roleList: ['admin'] } };
  assert.equal(user.getters.isContentAdmin({}, getters), true);
  assert.equal(user.getters.isSuperAdmin({}, getters), false);
  getters.userInfo.roleList = ['problem_admin'];
  assert.equal(user.getters.isContentAdmin({}, getters), false);
});

test('configuration reads show only the configured marker and leave the credential field empty', async () => {
  const component = load('components/admin/ContestIdentitySettings.vue', { '@/common/api': {
    getContestIdentityConfig: async () => ({ data: { data: { enabled: true, hasApiKey: true, apiUrl: 'https://service.example/v1/identities/verify-name' } } }),
  } });
  const state = stateFor(component, { contest: { id: 7 } });
  await state.load();
  assert.equal(state.config.apiKey, ''); assert.equal(state.config.hasApiKey, true); assert.equal(state.loaded, true);
});

test('configuration load cannot leak another contest settings through an out of order response', async () => {
  const pending = deferred();
  const component = load('components/admin/ContestIdentitySettings.vue', { '@/common/api': { getContestIdentityConfig: () => pending.promise } });
  const state = stateFor(component, { contest: { id: 7 } });
  const request = state.load(); state.contest = { id: 8 }; state.requestVersion++;
  pending.resolve({ data: { data: { enabled: true, starPrefixes: '2024' } } }); await request;
  assert.deepEqual(state.config, {}); assert.equal(state.loaded, false);
});

test('verification suppresses repeat clicks and refreshes server permissions after binding', async () => {
  const pending = deferred(), requests = [], refresh = [];
  const component = load('views/oj/contest/ContestDetails.vue', { '@/common/api': {
    verifyContestIdentity(data) { requests.push(data); return pending.promise; },
  } });
  const state = stateFor(component, { contestID: 7, identityStudentNumber: '00201234', identityName: '姓名',
    userInfo: { uid: 'user' }, $route: { params: { contestID: '7' } },
    $store: { getters: { sessionVersion: 1 }, dispatch: async name => { refresh.push(name); } },
  });
  const task = state.verifyIdentity(); state.verifyIdentity(); assert.equal(requests.length, 1);
  pending.resolve(); await task;
  assert.deepEqual(requests[0], { cid: 7, studentNumber: '00201234', name: '姓名' });
  assert.deepEqual(refresh, ['refreshContestAccess']); assert.equal(state.identityStudentNumber, ''); assert.equal(state.identityName, '');
});

test('a verification response from the previous login cannot grant the new session access', async () => {
  const pending = deferred(), refresh = [];
  const component = load('views/oj/contest/ContestDetails.vue', { '@/common/api': { verifyContestIdentity: () => pending.promise } });
  const state = stateFor(component, { contestID: 7, userInfo: { uid: 'user' }, $route: { params: { contestID: 7 } },
    $store: { getters: { sessionVersion: 1 }, dispatch: async name => refresh.push(name) },
  });
  const task = state.verifyIdentity(); state.$store.getters.sessionVersion++;
  component.watch.contestSessionVersion.call(state); pending.resolve(); await task;
  assert.deepEqual(refresh, []); assert.equal(state.identityBusy, false);
});

test.after(() => dom.window.close());
