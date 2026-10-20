const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const babel = require('@babel/core');
const compiler = require('vue-template-compiler');
const moment = require('moment');
const { JSDOM } = require('jsdom');
const dom = new JSDOM('<!doctype html><html><body></body></html>', { url: 'https://oj.example/' });
global.window = dom.window;
global.document = dom.window.document;

function load(file, mocks = {}) {
  const source = fs.readFileSync(path.join(__dirname, '../src', file), 'utf8');
  const script = file.endsWith('.vue') ? compiler.parseComponent(source).script.content : source;
  const output = babel.transformSync(script, { configFile: false, babelrc: false,
    plugins: ['@babel/plugin-transform-modules-commonjs'] }).code;
  const module = { exports: {} };
  const resolve = id => {
    if (Object.hasOwn(mocks, id)) return mocks[id];
    if (['moment', 'vuex', 'vue'].includes(id)) return require(id);
    if (id === '@/common/message') return { error() {}, warning() {}, success() {} };
    if (id.startsWith('@/common/') && !['api', 'utils', 'codeblock'].includes(id.slice(9))) {
      return load('common/' + id.slice(9) + '.js', mocks);
    }
    return {};
  };
  new Function('require', 'module', 'exports', output)(resolve, module, module.exports);
  return module.exports.default || module.exports;
}
const response = data => ({ data: { data } });
const deferred = () => { let resolve; const promise = new Promise(yes => { resolve = yes; }); return { promise, resolve }; };
const flush = async () => { await Promise.resolve(); await Promise.resolve(); };

function fixture(contest = {}, authenticated = true, api = {}) {
  const module = load('store/contest.js', { '@/common/api': api });
  const state = { ...module.state, requestVersions: { ...module.state.requestVersions },
    now: moment('2026-10-19T06:59:59Z'), access: null,
    contest: { id: 1, auth: 1, confidential: true, released: false, status: 0,
      startTime: '2026-10-19T06:00:00Z', endTime: '2026-10-19T07:00:00Z', ...contest } };
  const rootGetters = { isAuthenticated: authenticated, userInfo: { username: 'student' }, isSuperAdmin: false };
  const getters = {};
  for (const [key, getter] of Object.entries(module.getters)) {
    Object.defineProperty(getters, key, { get: () => getter(state, getters, {}, rootGetters) });
  }
  const context = { state, getters, rootGetters,
    rootState: { route: { params: { contestID: '1' } }, user: { sessionVersion: 1 } },
    commit(name, value) { module.mutations[name](state, value); },
    dispatch(name, value) { return module.actions[name](context, value); } };
  return { module, state, getters, rootGetters, context };
}
function createState(component, extra = {}) {
  const state = { $i18n: { t: key => key }, ...extra };
  const parts = [...(component.mixins || []), component];
  for (const part of parts) Object.assign(state, part.data ? part.data.call(state) : {});
  Object.assign(state, extra);
  for (const part of parts) for (const [name, method] of Object.entries(part.methods || {})) state[name] = method.bind(state);
  return state;
}

test('a confidential contest requires server password permission and ignores stale entry grants at the end', () => {
  const f = fixture();
  f.state.intoAccess = true;
  assert.equal(f.getters.canViewContest, false);
  f.context.commit('changeContestAccess', { access: true, canViewProblems: true, canSubmit: true, accessReason: 'READY' });
  assert.equal(f.getters.canViewContest, true);
  assert.equal(f.getters.canSubmit, true);
  f.context.commit('nowAdd1s');
  assert.equal(f.getters.isContestSealed, true);
  assert.equal(f.getters.contestMenuDisabled, true);
  assert.equal(f.getters.canSubmit, false);
  assert.equal(f.getters.passwordFormVisible, false);
});

test('scheduled time alone never grants access; server release opens content anonymously and submissions after login', () => {
  const f = fixture({ status: 1, releaseAt: '2026-10-19T06:00:00Z', allowEndSubmit: false }, false);
  assert.equal(f.getters.canViewContest, false);
  f.context.commit('changeContestAccess', { access: false, released: true, canViewProblems: true, canSubmit: false, accessReason: 'RELEASED' });
  assert.equal(f.getters.canViewContest, true);
  assert.equal(f.getters.passwordFormVisible, false);
  assert.equal(f.getters.problemSubmitDisabled, true);
  f.rootGetters.isAuthenticated = true;
  assert.equal(f.getters.canSubmit, true);
  assert.equal(f.getters.problemSubmitDisabled, false);
  f.context.commit('denyContestAccess');
  assert.equal(f.getters.canViewContest, false);
});

test('anonymous release of a team contest does not call the authenticated group-role endpoint', async () => {
  const api = {
    getContest: async () => response({ id: 1, auth: 1, confidential: true, released: true, gid: 5, status: 1, now: '2026-10-19T08:00:00Z' }),
    getContestAccess: async () => response({ access: false, released: true, canViewProblems: true, canSubmit: false, accessReason: 'RELEASED' }),
    getGroupAuth: async () => { assert.fail('anonymous group-role lookup would trigger the login modal'); },
  };
  const f = fixture({}, false, api);
  await f.context.dispatch('getContest');
  assert.equal(f.getters.canViewContest, true);
  assert.equal(f.state.groupContestAuth, 0);
});

test('contest administrators retain access while legacy private and protected contests retain their original rules', () => {
  const admin = fixture({ status: 1 });
  admin.rootGetters.isSuperAdmin = true;
  assert.equal(admin.getters.canViewContest, true);
  assert.equal(admin.getters.canSubmit, true);
  const privateContest = fixture({ confidential: false, status: 1 });
  assert.equal(privateContest.getters.canViewContest, false);
  privateContest.state.intoAccess = true;
  assert.equal(privateContest.getters.canViewContest, true);
  const protectedContest = fixture({ auth: 2, confidential: false });
  assert.equal(protectedContest.getters.canViewContest, true);
  assert.equal(protectedContest.getters.canSubmit, false);
});

test('legacy private external scoreboards stay public while confidential boards follow the unified gate', () => {
  const mixin = load('views/oj/contest/outside/scoreBoardMixin.js');
  const gate = mixin.computed.scoreboardMenuDisabled;
  assert.equal(gate.call({ contest: {}, contestMenuDisabled: true }), true);
  const oldPrivateBoard = { contest: { id: 1, auth: 1, openRank: true, confidential: false }, contestMenuDisabled: true };
  assert.equal(gate.call(oldPrivateBoard), false);
  const confidentialBoard = { ...oldPrivateBoard, contest: { ...oldPrivateBoard.contest, confidential: true } };
  assert.equal(gate.call(confidentialBoard), true);
  confidentialBoard.contestMenuDisabled = false;
  assert.equal(gate.call(confidentialBoard), false);
  let requests = 0;
  const state = { contest: confidentialBoard.contest, loading: { info: true }, getContestOutsideScoreboard: () => { requests++; } };
  mixin.watch.scoreboardMenuDisabled.call(state, false);
  assert.equal(requests, 0, 'initial metadata completion must not race its own initial board request');
  state.loading.info = false;
  mixin.watch.scoreboardMenuDisabled.call(state, false);
  assert.equal(requests, 1, 'a later release must refresh the board');
});

test('legacy private external boards use their authorized columns without requesting the private problem list', async () => {
  const problems = [{ pid: 2, displayId: 'A', displayTitle: 'Board title', color: 'red' }];
  const f = fixture({ confidential: false }, false, {
    getScoreBoardContestInfo: async () => response({ contest: { id: 1, auth: 1, openRank: true, confidential: false, now: '2026-10-19T08:00:00Z' }, problemList: problems }),
  });
  await f.context.dispatch('getScoreBoardContestInfo');
  assert.deepEqual(f.state.contestProblems, problems);
  assert.equal(f.state.intoAccess, false);
  const mixin = load('views/oj/contest/outside/scoreBoardMixin.js', {
    '@/common/api': { getContestOutsideScoreboard: async () => response({ records: [{ uid: 'ranked' }], total: 1 }) },
  });
  let applied;
  const state = createState(mixin, { contest: f.state.contest, isContestAdmin: false,
    $route: { params: { contestID: '1' } }, loading: { rank: false }, concernedList: [], page: 1, limit: 50,
    $store: { state: { contest: f.state }, dispatch: () => { assert.fail('a public board cannot request a password-gated problem list'); } },
    applyToTable: rows => { applied = rows; } });
  await state.getContestOutsideScoreboard();
  assert.deepEqual(applied, [{ uid: 'ranked' }]);
  assert.deepEqual(f.state.contestProblems, problems);
});

test('revoking view permission clears the cached problem list and invalidates an earlier list response', async () => {
  const task = deferred();
  const f = fixture({}, true, { getContestProblemList: () => task.promise });
  f.state.contestProblems = [{ title: 'secret' }];
  const pending = f.context.dispatch('getContestProblems');
  f.context.commit('changeContestAccess', { canViewProblems: false, canSubmit: false, accessReason: 'SEALED' });
  assert.deepEqual(f.state.contestProblems, []);
  task.resolve(response([{ title: 'late secret' }]));
  await pending;
  assert.deepEqual(f.state.contestProblems, []);
});

test('a failed permission refresh closes previously released content instead of keeping the old grant', async () => {
  const f = fixture({ released: true }, false, { getContest: async () => { throw new Error('denied'); } });
  await assert.rejects(f.context.dispatch('refreshContestAccess'), /denied/);
  assert.equal(f.getters.canViewContest, false);
});

test('switching accounts immediately removes the previous password/admin grant while released content stays public', () => {
  const f = fixture();
  f.state.intoAccess = true;
  f.state.access = { canViewProblems: true, accessReason: 'ADMIN' };
  assert.equal(f.getters.isContestAdmin, true);
  f.context.commit('resetContestAuthentication');
  assert.equal(f.getters.isContestAdmin, false);
  assert.equal(f.getters.canViewContest, false);
  f.state.contest.released = true;
  f.context.commit('resetContestAuthentication');
  assert.equal(f.getters.canViewContest, true);
});

test('the access timer seals an open page at the boundary and is removed when the page closes', async t => {
  t.mock.timers.enable({ apis: ['setInterval'] });
  const f = fixture();
  let refreshes = 0;
  f.state.access = { canViewProblems: true, canSubmit: true };
  const mixin = load('common/contestAccessPolling.js');
  const state = createState(mixin, { $options: { name: 'ContestDetails' }, $route: { name: 'ContestDetails', params: { contestID: '1' } },
    $store: { state: { contest: f.state }, getters: f.getters, commit: f.context.commit,
      dispatch: async () => { refreshes++; } } });
  mixin.mounted.call(state);
  t.mock.timers.tick(1000);
  assert.equal(f.getters.contestMenuDisabled, true);
  await flush();
  assert.equal(refreshes, 1);
  mixin.beforeDestroy.call(state);
  t.mock.timers.tick(30000);
  assert.equal(refreshes, 1);
});

test('a sealed direct problem URL performs no detail request and clears prior problem contents', async () => {
  let details = 0;
  const component = load('views/oj/problem/Problem.vue', { '@/common/api': { getContestProblem() { details++; } } });
  const f = fixture({ status: 1 });
  const state = createState(component, { $route: { name: 'ContestFullProblemDetails', fullPath: '/contest/1/problem/A/full-screen', params: { contestID: '1', problemID: 'A' } },
    $store: { getters: f.getters, state: { contest: f.state } } });
  state.changeDomTitle = () => {};
  state.problemData.problem.description = 'old secret';
  state.code = 'draft';
  await state.init();
  assert.equal(details, 0);
  assert.equal(state.problemData.problem.description, undefined);
  assert.equal(state.code, 'draft');
});

test('an in-flight problem detail response cannot restore the content after a seal', async () => {
  const task = deferred();
  const component = load('views/oj/problem/Problem.vue', {
    '@/common/api': { getContestProblem: () => task.promise },
    '@/common/utils': { isFocusModePage: () => true },
  });
  const f = fixture({ released: true }, false);
  const state = createState(component, { $route: { name: 'ContestFullProblemDetails', fullPath: '/contest/1/problem/A/full-screen', params: { contestID: '1', problemID: 'A' } },
    $store: { getters: f.getters, state: { contest: f.state } } });
  state.changeDomTitle = () => {};
  await state.init();
  state.revokeProblemAccess();
  task.resolve(response({ problem: { description: 'late secret' } }));
  await flush();
  assert.equal(state.problemData.problem.description, undefined);
});

test('the shared form enforces password-only entry and preserves a legacy null release time', () => {
  const component = load('components/admin/ContestConfidentialSettings.vue');
  const state = createState(component, { contest: { id: 1, auth: 2, openAccountLimit: true, endTime: '2026-01-01T00:00:00Z' },
    $set: (target, key, value) => { target[key] = value; } });
  state.setConfidential(true);
  assert.equal(state.contest.auth, 1);
  assert.equal(state.contest.openAccountLimit, false);
  assert.equal(state.contest.releaseAt, null);
  state.setConfidential(true);
  assert.equal(state.contest.releaseAt, null);
  assert.equal(component.methods.releaseNow, undefined);
  assert.equal(component.methods.sealAgain, undefined);
  const { confidentialFormError } = load('common/contestConfidentialForm.js');
  assert.equal(confidentialFormError(state.contest), null);
  assert.equal(confidentialFormError({ ...state.contest, releaseAt: '2025-01-01T00:00:00Z' }), 'Contest_Release_After_End');
  const oldContest = { auth: 1 };
  assert.equal(confidentialFormError(oldContest), null);
  assert.equal(Object.hasOwn(oldContest, 'confidential'), false);
});

test('short print content is sent without a 50-character threshold; new problems default to the real whitespace option', async () => {
  let submitted;
  const print = load('views/oj/contest/children/ContestPrint.vue', { '@/common/api': { submitPrintText: async data => { submitted = data; } } });
  const state = createState(print, { contestID: 1, ruleForm: { content: 'x' }, $confirm() {} });
  state.onSubmit();
  await flush();
  assert.equal(submitted.content, 'x');
  for (const file of ['views/admin/problem/Problem.vue', 'components/oj/group/Problem.vue']) {
    const component = load(file);
    assert.equal(component.data.call({}).problem.isRemoveEndBlank, true);
  }
});

test('formal submissions and test submissions both accept a nonempty one-character program', () => {
  const submitted = [];
  const pending = new Promise(() => {});
  const api = { submitCode: data => { submitted.push(data.code); return pending; },
    submitTestJudge: data => { submitted.push(data.code); return pending; } };
  const problem = load('views/oj/problem/Problem.vue', { '@/common/api': api });
  const formal = createState(problem, { code: 'x', canSubmit: true, isAuthenticated: true, $route: { params: {} } });
  formal.submitCode();
  const editor = load('components/oj/common/CodeMirror.vue', { '@/common/api': api });
  const quick = createState(editor, { value: 'x', isAuthenticated: true, language: 'C', mode: { C: 'text/x-csrc' } });
  quick.submitTestJudge();
  assert.deepEqual(submitted, ['x', 'x']);
});
