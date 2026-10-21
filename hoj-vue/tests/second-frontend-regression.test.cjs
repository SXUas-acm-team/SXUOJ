const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const babel = require('@babel/core');
const { JSDOM } = require('jsdom');

const dom = new JSDOM('<!doctype html><html><body></body></html>', { url: 'https://oj.example/' });
global.window = dom.window;
global.document = dom.window.document;
const Vue = require('vue');
const compiler = require('vue-template-compiler');
Vue.config.productionTip = false;
Vue.config.devtools = false;

function loadSource(file, mocks = {}) {
  const text = fs.readFileSync(path.join(__dirname, '../src', file), 'utf8');
  const source = file.endsWith('.vue') ? compiler.parseComponent(text).script.content : text;
  const output = babel.transformSync(source, {
    configFile: false, babelrc: false, plugins: ['@babel/plugin-transform-modules-commonjs'],
  }).code;
  const module = { exports: {} };
  const resolve = id => {
    if (Object.hasOwn(mocks, id)) return mocks[id];
    if (id === '@/components/admin/extraFilesMixin') return loadSource('components/admin/extraFilesMixin.js', mocks);
    if (id === '@/common/message') return { error() {}, warning() {}, success() {} };
    if (id.startsWith('@/common/') && !['api', 'utils', 'message', 'time', 'codeblock'].includes(id.slice(9))) {
      return require(path.join(__dirname, '../src/common', id.slice(9)));
    }
    if (['vue', 'vuex', 'moment'].includes(id)) return require(id);
    return {};
  };
  new Function('require', 'module', 'exports', output)(resolve, module, module.exports);
  return module.exports.default || module.exports;
}
function createState(component, extra = {}) {
  const parts = [...(component.mixins || []), component];
  const state = { $i18n: { t: key => key } };
  for (const part of parts) Object.assign(state, part.data ? part.data.call(state) : {});
  Object.assign(state, extra);
  for (const part of parts) for (const [name, method] of Object.entries(part.methods || {})) state[name] = method.bind(state);
  return state;
}
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const response = data => ({ data: { data } });
const flush = async () => { await Promise.resolve(); await Promise.resolve(); };

function contestFixture(api) {
  const module = loadSource('store/contest.js', { '@/common/api': api });
  const state = { ...module.state, requestVersions: { ...module.state.requestVersions } };
  const rootState = { contest: state, route: { params: { contestID: '1' } }, user: { sessionVersion: 1 } };
  const context = {
    state, rootState, rootGetters: { isAuthenticated: true },
    commit(name, payload) { module.mutations[name](state, payload); },
    dispatch(name, payload) { return module.actions[name](context, payload); },
  };
  return { module, state, rootState, context };
}

test('contest access is tied to the original contest and cannot repopulate a cleared store', async () => {
  const requests = [];
  const fixture = contestFixture({ getContestAccess() { const task = deferred(); requests.push(task); return task.promise; } });
  const first = fixture.module.actions.getContestAccess(fixture.context, { auth: 1 });
  fixture.rootState.route.params.contestID = '2';
  requests[0].resolve(response({ access: true }));
  await first;
  assert.equal(fixture.state.intoAccess, false);
  const late = fixture.module.actions.getContestAccess(fixture.context, { auth: 1 });
  fixture.module.mutations.clearContest(fixture.state);
  requests[1].resolve(response({ access: true }));
  await late;
  assert.equal(fixture.state.intoAccess, false);
  assert.equal(fixture.state.submitAccess, false);
});

test('slow contest details and problem-list responses never overwrite newer navigation', async () => {
  const info = [], problems = [];
  const fixture = contestFixture({
    getContest() { const task = deferred(); info.push(task); return task.promise; },
    getContestProblemList() { const task = deferred(); problems.push(task); return task.promise; },
  });
  const oldInfo = fixture.module.actions.getContest(fixture.context);
  const oldProblems = fixture.module.actions.getContestProblems(fixture.context);
  fixture.rootState.route.params.contestID = '2';
  const newInfo = fixture.module.actions.getContest(fixture.context);
  const newProblems = fixture.module.actions.getContestProblems(fixture.context);
  info[1].resolve(response({ id: 2, auth: 0, now: '2026-10-04T12:00:00Z' }));
  problems[1].resolve(response([{ displayId: 'B', color: 'blue' }]));
  await Promise.all([newInfo, newProblems]);
  info[0].resolve(response({ id: 1, auth: 0, now: '2026-10-03T12:00:00Z' }));
  problems[0].resolve(response([{ displayId: 'A', color: 'red' }]));
  await Promise.all([oldInfo, oldProblems]);
  assert.equal(fixture.state.contest.id, 2);
  assert.deepEqual(fixture.state.contestProblems, [{ displayId: 'B', color: 'blue' }]);
});

test('contest rankings keep the latest page and discard responses after destruction', async () => {
  const requests = [];
  const mixin = loadSource('views/oj/contest/children/contestRankMixin.js', {
    '@/common/api': { getContestRank() { const task = deferred(); requests.push(task); return task.promise; } },
  });
  const applied = [];
  const state = createState(mixin, {
    $route: { params: { contestID: '1' } }, showChart: false, limit: 50, concernedList: [],
    applyToTable(rows) { applied.push(rows); }, applyToChart() {},
  });
  const old = state.getContestRankData(1);
  const latest = state.getContestRankData(2);
  requests[1].resolve(response({ total: 20, records: [{ uid: 'latest' }] }));
  await latest;
  requests[0].resolve(response({ total: 99, records: [{ uid: 'old' }] }));
  await old;
  assert.deepEqual(applied, [[{ uid: 'latest' }]]);
  assert.equal(state.total, 20);
  const late = state.getContestRankData(3);
  mixin.beforeDestroy.call(state);
  requests[2].resolve(response({ total: 100, records: [{ uid: 'destroyed' }] }));
  await late;
  assert.equal(applied.length, 1);
});

test('re-enabling rank refresh keeps exactly one interval alive', t => {
  t.mock.timers.enable({ apis: ['setInterval'] });
  let refreshes = 0;
  const mixin = loadSource('views/oj/contest/children/contestRankMixin.js');
  const state = createState(mixin, {
    $store: { dispatch: () => Promise.resolve() }, page: 1,
    getContestRankData() { refreshes++; },
  });
  state.getContestRankData = () => { refreshes++; };
  state.handleAutoRefresh(true);
  state.handleAutoRefresh(true);
  t.mock.timers.tick(10000);
  assert.equal(refreshes, 1);
  mixin.beforeDestroy.call(state);
  t.mock.timers.tick(10000);
  assert.equal(refreshes, 1);
});

for (const file of ['views/admin/problem/Problem.vue', 'components/oj/group/Problem.vue']) {
  test(`${file}: renaming a judge helper file removes the original name`, () => {
    const component = loadSource(file);
    const state = createState(component, {
      judgeExtraFile: { 'old.h': 'old contents' }, $delete: (object, key) => { delete object[key]; },
      $set: (object, key, value) => { object[key] = value; },
    });
    state.upsertFile('judge', 'new.h', 'old.h', 'new contents');
    assert.deepEqual(state.judgeExtraFile, { 'new.h': 'new contents' });
  });

  test(`${file}: malformed helper config does not interrupt testcase loading or erase stored config`, async () => {
    let cases = 0;
    const errors = [];
    const invalid = '{"unfinished"';
    const data = { problemId: 'SXU1', examples: '', userExtraFile: invalid, judgeExtraFile: '{"answer.h":"secret"}' };
    const task = deferred();
    const api = {
      admin_getProblem: () => task.promise, getGroupProblem: () => task.promise,
      admin_getProblemCases: async () => { cases++; return response([]); },
      getGroupProblemCases: async () => { cases++; return response([]); },
      admin_getProblemTags: async () => response([]), getGroupProblemTags: async () => response([]),
    };
    const component = loadSource(file, { '@/common/api': api, '@/common/utils': { stringToExamples: () => [] },
      '@/common/message': { error: value => errors.push(value) } });
    const state = createState(component, { mode: 'edit', routeName: 'admin-edit-problem',
      $route: { params: { problemId: 1 }, query: {} }, pid: 1, group: { shortName: 'SXU' }, $refs: {} });
    state.getProblemCodeTemplateAndLanguage = () => {};
    state.init();
    task.resolve(response(data));
    await flush();
    assert.equal(cases, 1);
    assert.equal(state.extraFilesInvalid, true);
    assert.equal(state.problem.userExtraFile, invalid);
    assert.deepEqual(state.judgeExtraFile, { 'answer.h': 'secret' });
    assert.equal(errors.length, 1);
    state.submit();
    assert.equal(state.problem.userExtraFile, invalid, 'save must not replace damaged raw config with null');
    assert.equal(errors.length, 2);
  });
}

test('extra file parsing supports legacy empty and object configs while rejecting unsafe shapes and keys', () => {
  const { parseExtraFiles } = require('../src/common/extraFiles');
  for (const value of [null, undefined, '', 'null', '{}', {}]) assert.deepEqual(parseExtraFiles(value), {});
  assert.deepEqual(parseExtraFiles({ 'lib.h': 'int value;' }), { 'lib.h': 'int value;' });
  assert.deepEqual(parseExtraFiles('{"lib.h":"int value;"}'), { 'lib.h': 'int value;' });
  for (const value of ['["x"]', 'true', '3', '{', '{"__proto__":"x"}', '{"constructor":"x"}', '{"prototype":"x"}', '{"lib.h":42}']) {
    assert.throws(() => parseExtraFiles(value), value);
  }
});

test('helper filenames cannot mutate prototypes and Vue redraws a renamed file immediately', async () => {
  const errors = [];
  const mixin = loadSource('components/admin/extraFilesMixin.js', {
    '@/common/message': { error: value => errors.push(value) },
  });
  const view = new Vue({
    mixins: [mixin],
    data: () => ({ judgeExtraFile: { 'old.h': 'old contents' }, userExtraFile: {}, problem: {} }),
    render(h) { return h('ul', Object.keys(this.judgeExtraFile).map(name => h('li', name))); },
  });
  view.$i18n = { t: key => key };
  view.$mount();
  const prototype = Object.getPrototypeOf(view.judgeExtraFile);
  for (const name of ['__proto__', 'constructor', 'prototype']) view.upsertFile('judge', name, '', 'data');
  assert.equal(errors.length, 3);
  assert.equal(Object.getPrototypeOf(view.judgeExtraFile), prototype);
  view.upsertFile('judge', 'new.h', 'old.h', 'new contents');
  await Vue.nextTick();
  assert.equal(view.$el.textContent, 'new.h');
  view.$destroy();
});

test('contest metadata, access and group roles can finish in parallel for the same live contest', async () => {
  const access = deferred(), auth = deferred();
  const fixture = contestFixture({
    getContest: async () => response({ id: 1, auth: 1, gid: 5, now: '2026-10-04T12:00:00Z' }),
    getContestAccess: () => access.promise, getGroupAuth: () => auth.promise,
  });
  await fixture.context.dispatch('getContest');
  access.resolve(response({ access: true })); auth.resolve(response(5));
  await flush();
  assert.equal(fixture.state.contest.id, 1);
  assert.equal(fixture.state.intoAccess, true);
  assert.equal(fixture.state.groupContestAuth, 5);
});

test('permission failures reject promptly and account changes invalidate pending grants', async () => {
  const access = deferred();
  const fixture = contestFixture({ getContestAccess: () => access.promise,
    getGroupAuth: () => Promise.reject(new Error('denied')) });
  const pending = fixture.context.dispatch('getContestAccess', { auth: 1 });
  fixture.rootState.user.sessionVersion++;
  access.resolve(response({ access: true }));
  await pending;
  assert.equal(fixture.state.intoAccess, false);
  await assert.rejects(fixture.context.dispatch('getGroupContestAuth', { gid: 5 }), /denied/);
  const failed = contestFixture({ getContestAccess: () => Promise.reject(new Error('expired')) });
  await assert.rejects(failed.context.dispatch('getContestAccess', { auth: 1 }), /expired/);
});

test('a superseded problem-list failure cannot clear the latest results', async () => {
  const requests = [];
  const fixture = contestFixture({ getContestProblemList() { const task = deferred(); requests.push(task); return task.promise; } });
  const old = fixture.context.dispatch('getContestProblems');
  const latest = fixture.context.dispatch('getContestProblems');
  requests[1].resolve(response([{ displayId: 'B' }]));
  await latest;
  requests[0].reject(new Error('slow failure'));
  await assert.rejects(old, /slow failure/);
  assert.deepEqual(fixture.state.contestProblems, [{ displayId: 'B' }]);
});

test('contest details cannot create intervals or apply registration after their view is destroyed', async t => {
  t.mock.timers.enable({ apis: ['setInterval'] });
  const info = deferred(), registration = deferred();
  let ticks = 0, grants = 0, titles = 0;
  const component = loadSource('views/oj/contest/ContestDetails.vue', {
    '@/common/api': { registerContest: () => registration.promise },
    '@/common/codeblock': { addCodeBtn() {} },
  });
  const state = createState(component, {
    contestID: '1', $route: { params: { contestID: '1' } },
    userInfo: { uid: 'u1' }, contestPassword: 'secret', contestEnded: false,
    $store: { dispatch: () => info.promise, commit(name) { if (name === 'nowAdd1s') ticks++; if (name === 'contestIntoAccess') grants++; } },
    $nextTick: callback => callback(),
  });
  state.changeDomTitle = () => { titles++; };
  const pendingInfo = state.loadContestDetails();
  const pendingRegistration = state.checkPassword();
  component.beforeDestroy.call(state);
  info.resolve(response({ title: 'Old contest', endTime: '2026-10-05T12:00:00Z', now: '2026-10-04T12:00:00Z' }));
  registration.resolve(response({}));
  await Promise.all([pendingInfo, pendingRegistration]);
  t.mock.timers.tick(120000);
  assert.equal(ticks, 0);
  assert.equal(grants, 0);
  assert.equal(titles, 0);
});

test('contest registration suppresses double clicks and a changed session can retry without receiving an old grant', async () => {
  const requests = [];
  let grants = 0;
  const component = loadSource('views/oj/contest/ContestDetails.vue', {
    '@/common/api': { registerContest() { const task = deferred(); requests.push(task); return task.promise; } },
  });
  const state = createState(component, { contestID: '1', contestPassword: 'secret',
    $route: { params: { contestID: '1' } }, userInfo: { uid: 'u1' },
    $store: { getters: { sessionVersion: 1 }, commit() { grants++; }, dispatch: async () => {} },
  });
  const old = state.checkPassword();
  state.checkPassword();
  assert.equal(requests.length, 1);
  state.$store.getters.sessionVersion++;
  requests[0].resolve(response({})); await old;
  assert.equal(grants, 0);
  assert.equal(state.btnLoading, false);
  const latest = state.checkPassword();
  requests[1].resolve(response({})); await latest;
  assert.equal(grants, 1);
  assert.equal(state.btnLoading, false);
});

test('public scoreboard preserves the latest page when older responses and errors arrive late', async () => {
  const requests = [], applied = [];
  const mixin = loadSource('views/oj/contest/outside/scoreBoardMixin.js', {
    '@/common/api': { getContestOutsideScoreboard() { const task = deferred(); requests.push(task); return task.promise; } },
  });
  const state = createState(mixin, { $route: { params: { contestID: '1' } },
    $store: { dispatch: () => Promise.resolve(), commit() {} }, loading: { rank: false },
    concernedList: [], page: 1, limit: 50, applyToTable: rows => applied.push(rows),
  });
  const old = state.getContestOutsideScoreboard(); state.page = 2;
  const latest = state.getContestOutsideScoreboard();
  requests[1].resolve(response({ records: [{ uid: 'latest' }], total: 2 })); await latest;
  requests[0].resolve(response({ records: [{ uid: 'old' }], total: 99 })); await old;
  assert.deepEqual(applied, [[{ uid: 'latest' }]]);
  assert.equal(state.total, 2);
  const failure = state.getContestOutsideScoreboard();
  const success = state.getContestOutsideScoreboard();
  requests[2].reject(new Error('old error')); await failure;
  assert.equal(state.loading.rank, true);
  mixin.beforeDestroy.call(state);
  requests[3].resolve(response({ records: [{ uid: 'destroyed' }], total: 99 })); await success;
  assert.equal(applied.length, 1);
});
