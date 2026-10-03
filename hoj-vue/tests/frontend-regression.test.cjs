const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const babel = require('@babel/core');
const compiler = require('vue-template-compiler');
const { JSDOM } = require('jsdom');

const dom = new JSDOM('<!doctype html><html><body></body></html>', { url: 'https://oj.example/' });
global.window = dom.window;
global.document = dom.window.document;
const Vue = require('vue');
const safety = require('../src/common/viewSafety');
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
    if (id === '@/common/viewSafety') return safety;
    if (id === 'vue' || id === 'vuex') return require(id);
    return {};
  };
  new Function('require', 'module', 'exports', output)(resolve, module, module.exports);
  return module.exports.default;
}
function createState(component, extra = {}) {
  const state = { ...component.data.call({ $i18n: { t: key => key } }), ...extra };
  for (const [name, method] of Object.entries(component.methods || {})) state[name] = method.bind(state);
  return state;
}
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const response = data => ({ data: { data } });
const flush = async () => { await Promise.resolve(); await Promise.resolve(); };

test('problem URL filters reject non-array JSON and preserve valid tags/page sizes', () => {
  const component = loadSource('views/oj/problem/ProblemList.vue');
  const state = createState(component, { $route: { name: 'ProblemList', query: {} } });
  for (const tagId of ['null', '{}', 'true', '3', '"12"', '[', undefined]) {
    state.$route.query = { tagId, currentPage: '-5', limit: '2147483647', keyword: ['x', 'y'], oj: ['Mine'] };
    state.init();
    assert.deepEqual(state.query.tagId, []);
    assert.equal(state.query.currentPage, 1);
    assert.equal(state.limit, 30);
    assert.equal(state.query.keyword, '');
    assert.equal(state.query.oj, 'Mine');
  }
  state.$route.query = { tagId: '[1,"2",2,null,{},-1,"1e3",1.5]', currentPage: '2', limit: '15' };
  state.init();
  assert.deepEqual(state.query.tagId, [1, 2]);
  assert.equal(state.query.currentPage, 2);
  assert.equal(state.limit, 15);
  let target;
  state.$router = { push(value) { target = value; } };
  state.filterTagList = [{ id: 3 }];
  state.pushRouter();
  assert.equal(target.query.tagId, '[3]');
  assert.deepEqual(state.query.tagId, [1, 2], 'router serialization must not mutate array filters');
});

test('slow problem-list responses cannot overwrite newer filters or end their loading state', async () => {
  const requests = [];
  const component = loadSource('views/oj/problem/ProblemList.vue', {
    '@/common/api': { getProblemList() { const task = deferred(); requests.push(task); return task.promise; } },
  });
  const state = createState(component, { isAuthenticated: false });
  const old = state.getProblemList();
  const latest = state.getProblemList();
  requests[0].reject(new Error('old request failed'));
  await old;
  assert.equal(state.loadings.table, true);
  requests[1].resolve(response({ total: 1, records: [{ pid: 2 }] }));
  await latest;
  assert.deepEqual(state.problemList, [{ pid: 2 }]);
  const late = state.getProblemList();
  const final = state.getProblemList();
  requests[3].resolve(response({ total: 1, records: [{ pid: 4 }] }));
  await final;
  requests[2].resolve(response({ total: 99, records: [{ pid: 3 }] }));
  await late;
  assert.deepEqual(state.problemList, [{ pid: 4 }]);
  assert.equal(state.total, 1);
});

test('personal problem statuses stay with their originating page and handle omitted entries', async () => {
  const statuses = [];
  let pid = 0;
  const component = loadSource('views/oj/problem/ProblemList.vue', {
    '@/common/api': {
      getProblemList: async () => response({ total: 1, records: [{ pid: ++pid }] }),
      getUserProblemStatus() { const task = deferred(); statuses.push(task); return task.promise; },
    },
  });
  const state = createState(component, { isAuthenticated: true });
  await state.getProblemList();
  await state.getProblemList();
  statuses[0].resolve(response({ 1: { status: 0 } }));
  await flush();
  assert.deepEqual(state.problemList, [{ pid: 2 }]);
  assert.equal(state.isGetStatusOk, false);
  statuses[1].resolve(response({}));
  await flush();
  assert.deepEqual(state.problemList, [{ pid: 2, myStatus: -10 }]);
  assert.equal(state.isGetStatusOk, true);
});

test('tag requests honor current navigation and destroyed views discard pending results', async () => {
  const requests = [];
  const component = loadSource('views/oj/problem/ProblemList.vue', {
    '@/common/api': { getProblemTagsAndClassification() { const task = deferred(); requests.push(task); return task.promise; } },
  });
  const state = createState(component);
  state.query.tagId = [2];
  const first = state.getTagList('Mine');
  const second = state.getTagList('CF');
  const tags = [{ classification: null, tagList: [{ id: 2, name: 'New' }] }];
  requests[1].resolve(response(tags));
  await second;
  requests[0].resolve(response([{ classification: null, tagList: [{ id: 1, name: 'Old' }] }]));
  await first;
  assert.deepEqual(state.tagsAndClassificationList, tags);
  assert.deepEqual(state.filterTagList, [{ id: 2, name: 'New' }]);
  const third = state.getTagList('Mine');
  component.beforeDestroy.call(state);
  requests[2].resolve(response([]));
  await third;
  assert.deepEqual(state.tagsAndClassificationList, tags);
  const calls = [];
  component.watch.$route.call({ init() { calls.push('init'); }, getTagList(oj) { calls.push(oj); }, getData() { calls.push('data'); }, query: { oj: 'Mine' } }, {}, {});
  assert.deepEqual(calls, ['init', 'Mine', 'data'], 'browser history must resync selected tags');
});

function downloadFixture(t, result) {
  const errors = [], success = [], clicks = [], revoked = [], blobs = [];
  Vue.prototype.$axios = { get: async () => result };
  const originalClick = window.HTMLAnchorElement.prototype.click;
  window.HTMLAnchorElement.prototype.click = function () { clicks.push({ name: this.download, href: this.href }); };
  window.URL.createObjectURL = blob => { blobs.push(blob); return `blob:download-${blobs.length}`; };
  window.URL.revokeObjectURL = url => revoked.push(url);
  t.after(() => { window.HTMLAnchorElement.prototype.click = originalClick; delete Vue.prototype.$axios; });
  const utils = loadSource('common/utils.js', { '@/common/message': { error: value => errors.push(value), success: value => success.push(value) } });
  return { utils, errors, success, clicks, revoked, blobs };
}

test('JSON and malformed download errors reject promptly without a false success', async t => {
  const fixture = downloadFixture(t, { headers: { 'content-type': 'application/json' }, data: new window.Blob(['{"msg":"Denied"}']) });
  await assert.rejects(fixture.utils.downloadFile('/api/file/download'), /Denied/);
  Vue.prototype.$axios.get = async () => ({ headers: { 'content-type': 'APPLICATION/PROBLEM+JSON' }, data: new window.Blob(['bad json']) });
  await assert.rejects(fixture.utils.downloadFile('/api/file/download'), /Invalid file format/);
  const reader = window.FileReader;
  window.FileReader = class { readAsText() { this.onerror(); } };
  try { await assert.rejects(fixture.utils.downloadFile('/api/file/download'), /Invalid file format/); }
  finally { window.FileReader = reader; }
  Vue.prototype.$axios.get = async () => ({ status: 500, headers: { 'content-type': 'text/html' }, data: new window.Blob(['server error']) });
  await assert.rejects(fixture.utils.downloadFile('/api/file/download'), /HTTP 500/);
  assert.deepEqual(fixture.errors, ['Denied', 'Invalid file format', 'Invalid file format', 'Download failed (HTTP 500)']);
  assert.equal(fixture.clicks.length, 0);
  assert.equal(fixture.success.length, 0);
});

test('binary/text downloads support absent headers, UTF-8 filenames and release Blob URLs', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const fixture = downloadFixture(t, { headers: {}, data: new window.Blob(['data']) });
  await fixture.utils.downloadFile('/api/file/download');
  assert.equal(fixture.clicks[0].name, 'download');
  Vue.prototype.$axios.get = async () => ({ headers: { 'content-disposition': "attachment; filename=legacy.zip; filename*=UTF-8''%E6%B5%8B%E8%AF%95.zip" }, data: 'data' });
  await fixture.utils.downloadFile('/api/file/download');
  assert.equal(fixture.clicks[1].name, '测试.zip');
  Vue.prototype.$axios.get = async () => ({ headers: { 'content-disposition': 'attachment; filename="data set.zip"; size=5' }, data: 'data' });
  await fixture.utils.downloadFile('/api/file/download');
  assert.equal(fixture.clicks[2].name, 'data set.zip');
  await fixture.utils.downloadFileByText('solution.cpp', 'int main() {}');
  assert.equal(fixture.clicks[3].name, 'solution.cpp');
  assert.equal(document.querySelectorAll('a[download]').length, 0);
  assert.equal(fixture.revoked.length, 0);
  t.mock.timers.tick(1000);
  assert.deepEqual(fixture.revoked, fixture.clicks.map(click => click.href));
});

test('download actions consume handled errors and display success alerts only after a file is saved', async () => {
  for (const file of [
    'views/admin/problem/ProblemList.vue',
    'views/admin/training/TrainingProblemList.vue',
    'components/oj/group/ProblemList.vue',
    'components/oj/group/TrainingProblemList.vue',
  ]) {
    let fail = true, alerts = 0;
    const component = loadSource(file, { '@/common/utils': { downloadFile: () => fail ? Promise.reject(new Error('already reported')) : Promise.resolve() } });
    const state = { $i18n: { t: key => key }, $alert() { alerts++; } };
    await component.methods.downloadTestCase.call(state, 1);
    assert.equal(alerts, 0, file);
    fail = false;
    await component.methods.downloadTestCase.call(state, 1);
    assert.equal(alerts, 1, file);
  }
});

test('email-code validation handles invalid/unchanged input and suppresses duplicate sends', async () => {
  const errors = [], requests = [];
  const component = loadSource('components/oj/setting/Account.vue', {
    '@/common/api': { getChangeEmailCode(email) { const task = deferred(); requests.push({ ...task, email }); return task.promise; } },
    '@/common/message': { error: msg => errors.push(msg), success() {} },
  });
  const state = { formEmail: { oldEmail: 'old@example.com', newEmail: '' }, loading: { btnSendEmail: false }, $i18n: { t: key => key }, $notify: { success() {} } };
  const send = () => component.methods.getChangeEmailCode.call(state);
  send();
  state.formEmail.newEmail = 'invalid'; send();
  state.formEmail.newEmail = 'old@example.com'; send();
  assert.equal(errors.length, 3);
  assert.equal(requests.length, 0);
  state.formEmail.newEmail = 'new@example.com';
  const pending = send(); send();
  assert.equal(requests.length, 1);
  requests[0].reject(new Error('offline'));
  await pending;
  assert.equal(state.loading.btnSendEmail, false);
  const retry = send();
  requests[1].resolve(response({}));
  await retry;
  assert.equal(state.loading.btnSendEmail, false);
});

function testJudgeFixture(api) {
  const component = loadSource('components/oj/common/CodeMirror.vue', {
    '@/common/api': api,
    '@/common/constants': { JUDGE_STATUS_RESERVE: { Pending: 6, ac: 0 } },
  });
  const state = createState(component, { isAuthenticated: true, value: 'int main() {}', language: 'C', pid: 1, problemTestCase: [] });
  return { component, state };
}

test('in-flight test-judge responses cannot restart polling after destruction', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const task = deferred(); let calls = 0;
  const { component, state } = testJudgeFixture({ getTestJudgeResult() { calls++; return task.promise; } });
  state.testJudgeKey = 'old-test';
  state.testJudgeLoding = true;
  state.checkTestJudgeStatus();
  t.mock.timers.tick(1000);
  assert.equal(calls, 1);
  component.beforeDestroy.call(state);
  task.resolve(response({ status: 6 }));
  await flush();
  t.mock.timers.tick(10000);
  assert.equal(calls, 1);
  assert.equal(state.refreshStatus, null);
  assert.equal(state.testJudgeLoding, false);
});

test('test submission is single-flight and switching problems ignores its late response', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const task = deferred(); let calls = 0;
  const { component, state } = testJudgeFixture({ submitTestJudge() { calls++; return task.promise; }, getTestJudgeResult() { assert.fail('stale test should never poll'); } });
  const pending = state.submitTestJudge();
  state.submitTestJudge();
  assert.equal(calls, 1);
  assert.equal(state.testJudgeLoding, true);
  component.watch.pid.call(state);
  task.resolve(response('old-key'));
  await pending;
  t.mock.timers.tick(10000);
  assert.equal(state.testJudgeKey, null);
  assert.equal(state.testJudgeLoding, false);
  assert.equal(state.testJudgeRes.status, -10);
});

test('logout clears local credentials and leaves its blank route after success or rejection', async () => {
  for (const fail of [false, true]) {
    const events = [];
    const component = loadSource('views/oj/user/Logout.vue', {
      '@/common/api': { logout: () => fail ? Promise.reject(new Error('session revoked')) : Promise.resolve({}) },
    });
    await component.mounted.call({
      $store: { async dispatch(action) { events.push(action); } },
      $router: { replace(route) { events.push(route.path); } },
    });
    assert.deepEqual(events, ['clearUserInfoAndToken', '/home']);
  }
});

const judgeConstants = { JUDGE_STATUS_RESERVE: { Pending: 6, Compiling: 5, Judging: 7, ac: 0 }, CONTEST_STATUS: {}, RULE_TYPE: { OI: 1 }, buildProblemCodeAndSettingKey: () => 'code', buildIndividualLanguageAndSettingKey: () => 'language' };

test('formal submission polling ignores responses received after switching problems', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const request = deferred(); let calls = 0;
  const component = loadSource('views/oj/problem/Problem.vue', {
    '@/common/api': { getSubmission() { calls++; return request.promise; } },
    '@/common/constants': judgeConstants,
    '@/common/storage': { set() {} },
  });
  const state = createState(component, { submissionId: 1 });
  state.checkSubmissionStatus();
  t.mock.timers.tick(2000);
  state.beforeLeaveDo(0);
  state.submissionId = 2;
  request.resolve(response({ submission: { status: 6 } }));
  await flush();
  t.mock.timers.tick(10000);
  assert.equal(calls, 1);
  assert.equal(state.result.status, 9);
});

function submissionListFixture(api) {
  const component = loadSource('views/oj/status/SubmissionList.vue', {
    '@/common/api': api,
    '@/common/constants': judgeConstants,
    '@/common/utils': { filterEmptyValue: value => value },
  });
  const state = createState(component);
  state.$refs = { xTable: { getTableData: () => ({ tableData: state.submissions }), reloadRow() {} } };
  return { component, state };
}

test('old status polls cannot write their row positions into a different submission page', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const poll = deferred(); let calls = 0;
  const { state } = submissionListFixture({
    checkSubmissonsStatus() { calls++; return poll.promise; },
    getSubmissionList: async () => response({ records: [{ submitId: 2, status: 0 }], total: 1 }),
  });
  state.submissions = [{ submitId: 1, status: 6 }];
  state.needCheckSubmitIds = { 1: 0 };
  state.checkSubmissionsStatus();
  t.mock.timers.tick(2000);
  await state.getSubmissions();
  await flush();
  poll.resolve(response({ 1: { submitId: 1, status: 6 } }));
  await flush();
  t.mock.timers.tick(10000);
  assert.equal(state.submissions[0].submitId, 2);
  assert.equal(state.submissions[0].status, 0);
  assert.equal(calls, 1);
});

test('leaving the submission list invalidates list responses that would restart polling', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const request = deferred(); let calls = 0;
  const { component, state } = submissionListFixture({
    getSubmissionList: () => request.promise,
    checkSubmissonsStatus() { calls++; return Promise.resolve(response({})); },
  });
  const pending = state.getSubmissions();
  component.beforeRouteLeave.call(state, {}, {}, () => {});
  request.resolve(response({ records: [{ submitId: 1, status: 6 }], total: 1 }));
  await pending;
  await flush();
  t.mock.timers.tick(2000);
  assert.equal(calls, 0);
  assert.deepEqual(state.submissions, []);
});

test('late code-submit and rejudge responses cannot attach themselves to the next page', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const request = deferred();
  const component = loadSource('views/oj/problem/Problem.vue', {
    '@/common/api': { submitCode: () => request.promise, getSubmission() { assert.fail('stale submission started a poll'); } },
    '@/common/constants': judgeConstants,
    '@/common/storage': { set() {} },
  });
  const state = createState(component, { code: 'int main() {}', canSubmit: true, $route: { params: {} }, contestRuleType: 0 });
  state.submitCode();
  state.beforeLeaveDo(0);
  request.resolve(response({ submitId: 1 }));
  await flush();
  t.mock.timers.tick(10000);
  assert.equal(state.submissionId, '');
  for (const method of ['reSubmit', 'handleRejudge']) {
    const task = deferred();
    const fixture = submissionListFixture({
      reSubmitRemoteJudge: () => task.promise,
      submissionRejudge: () => task.promise,
      getSubmissionList: async () => response({ records: [{ submitId: 2, status: 0 }], total: 1 }),
    });
    const row = { submitId: 1, status: 0, index: 0 };
    fixture.state.submissions = [row];
    fixture.state[method](row);
    await fixture.state.getSubmissions();
    task.resolve(response({ submitId: 1, status: 6 }));
    await flush();
    assert.equal(fixture.state.submissions[0].submitId, 2);
    assert.equal(fixture.state.autoCheckOpen, false);
  }
});

test('formal submission polling still advances pending results and finishes normally', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let calls = 0, refreshes = 0;
  const component = loadSource('views/oj/problem/Problem.vue', {
    '@/common/api': { getSubmission: async () => response({ submission: { status: ++calls === 1 ? 6 : 0 } }) },
    '@/common/constants': judgeConstants,
  });
  const state = createState(component, { submissionId: 1, submitted: true });
  state.init = () => refreshes++;
  state.checkSubmissionStatus();
  t.mock.timers.tick(2000); await flush();
  assert.equal(state.result.status, 6);
  t.mock.timers.tick(2000); await flush();
  assert.equal(state.result.status, 0);
  assert.equal(state.submitted, false);
  assert.equal(refreshes, 1);
  t.mock.timers.tick(10000);
  assert.equal(calls, 2);
});

test('submission list polls the current rows until judging completes', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let calls = 0;
  const { state } = submissionListFixture({
    getSubmissionList: async () => response({ records: [{ submitId: 1, status: 6 }], total: 1 }),
    checkSubmissonsStatus: async () => response({ 1: { submitId: 1, status: ++calls === 1 ? 7 : 0 } }),
  });
  await state.getSubmissions();
  t.mock.timers.tick(2000); await flush();
  assert.equal(state.submissions[0].status, 7);
  assert.equal(state.autoCheckOpen, true);
  t.mock.timers.tick(2000); await flush();
  assert.equal(state.submissions[0].status, 0);
  assert.equal(state.autoCheckOpen, false);
  assert.deepEqual(state.needCheckSubmitIds, {});
  t.mock.timers.tick(10000);
  assert.equal(calls, 2);
});
