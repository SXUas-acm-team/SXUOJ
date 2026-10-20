const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const babel = require('@babel/core');
const { JSDOM } = require('jsdom');
const dom = new JSDOM('<!doctype html><html><body></body></html>', { url: 'https://oj.example/' });
global.window = dom.window;
global.document = dom.window.document;
Object.defineProperty(global, 'navigator', { configurable: true, value: dom.window.navigator });
const Vue = require('vue');
const compiler = require('vue-template-compiler');
Vue.config.productionTip = false;
Vue.config.devtools = false;
Vue.config.ignoredElements = [/^el-/, 'router-link'];
Vue.prototype.$t = key => key;
Vue.filter('parseContestType', value => value);
Vue.filter('localtime', value => value);

function load(file, mocks = {}) {
  const source = fs.readFileSync(path.join(__dirname, '../src', file), 'utf8');
  const parsed = file.endsWith('.vue') ? compiler.parseComponent(source) : null;
  const output = babel.transformSync(parsed ? parsed.script.content : source, { configFile: false, babelrc: false,
    plugins: ['@babel/plugin-transform-modules-commonjs'] }).code;
  const module = { exports: {} };
  const resolve = id => {
    if (Object.hasOwn(mocks, id)) return mocks[id];
    if (['vue', 'vuex', 'moment'].includes(id)) return require(id);
    if (id === '@/common/message') return { error() {}, warning() {}, success() {} };
    if (id.startsWith('@/common/') && !['api', 'utils', 'codeblock'].includes(id.slice(9))) return load('common/' + id.slice(9) + '.js', mocks);
    return {};
  };
  new Function('require', 'module', 'exports', output)(resolve, module, module.exports);
  const result = module.exports.default || module.exports;
  return parsed ? { ...result, ...compiler.compileToFunctions(parsed.template.content) } : result;
}
function createState(component, extra = {}) {
  const state = { $i18n: { t: key => key }, ...extra };
  Object.assign(state, component.data ? component.data.call(state) : {}, extra);
  for (const [name, method] of Object.entries(component.methods || {})) state[name] = method.bind(state);
  return state;
}

test('confidential settings expose one release-time input and no manual mode or action buttons', () => {
  const component = load('components/admin/ContestConfidentialSettings.vue');
  const contest = { confidential: true, auth: 1, releaseAt: null };
  const view = new Vue({ ...component, propsData: { contest } });
  view.$mount();
  assert.equal(view.$el.querySelectorAll('el-date-picker').length, 1);
  assert.equal(view.$el.querySelectorAll('el-radio-group,el-radio,el-button').length, 0);
  assert.equal(contest.releaseAt, null, 'rendering an old policy must not silently release it');
  view.setConfidential(true);
  assert.equal(contest.releaseAt, null);
  contest.releaseAt = '2026-10-20T12:00:00Z';
  view.setConfidential(true);
  assert.equal(contest.releaseAt, '2026-10-20T12:00:00Z');
  view.$destroy();
});

test('the requested Chinese contest copy is exact and old password-confidential names are removed', () => {
  const zh = load('i18n/admin/zh-CN.js').m;
  assert.equal(zh.Confidential_Contest, '保密赛');
  assert.equal(zh.Confidential_Contest_Password_Tips, '本场为保密赛，请输入比赛密码查看题目。');
  assert.equal(zh.Confidential_Contest_Tips, '使用密码入场。比赛结束后封存全部比赛内容；手动或定时解封后向所有人公开。');
  assert.equal(zh.Contest_Sealed_Tips, '比赛内容已封存。');
  for (const locale of ['zh-CN', 'zh-TW', 'en-US', 'ja-JP', 'ko-KR']) {
    const text = fs.readFileSync(path.join(__dirname, '../src/i18n/admin', locale + '.js'), 'utf8');
    assert.doesNotMatch(text, /密码保密赛|Password confidential contest|请等待管理员手动或定时解封/);
  }
});

for (const [name, allowed, admin, released, authenticated] of [
  ['password required', false, false, false, true],
  ['anonymous password entry', false, false, false, false],
  ['sealed participant', false, false, false, true],
  ['authorized administrator', true, true, false, true],
  ['released anonymous viewer', true, false, true, false],
]) {
  test(`content tabs and children follow the actual view gate: ${name}`, () => {
    const component = load('views/oj/contest/ContestDetails.vue');
    const computed = Object.fromEntries(Object.keys(component.computed).map(key => [key, () => false]));
    let children = 0;
    Object.assign(computed, {
      contest: () => ({ id: 1, auth: 1, confidential: true, type: 0, title: 'Contest', description: 'Hidden synopsis' }),
      contestMenuDisabled: () => !allowed, isContestAdmin: () => admin, isSuperAdmin: () => admin,
      isAuthenticated: () => authenticated, isContestReleased: () => released,
      passwordFormVisible: () => !allowed, userInfo: () => ({ uid: authenticated ? 'viewer' : null }),
      websiteConfig: () => ({ openContestComment: true }), timeStep: () => 1, progressValue: () => 0,
      confidentialStatusText: () => released ? 'Contest_Released_Public_Tips' : 'Confidential_Contest_Password_Tips',
    });
    const view = new Vue({ ...component, mixins: [], computed, watch: {}, created: [], mounted: [], beforeDestroy: [],
      data() { return { ...component.data.call(this), route_name: 'ContestProblemList', CONTEST_TYPE_REVERSE: { 1: { name: 'Private', tips: 'Private_Tips', color: 'danger' } } }; },
      components: {
        Markdown: { props: ['content'], render: h => h('div', 'Hidden synopsis') },
        'router-view': { mounted() { children++; }, render: h => h('div', 'Hidden child content') },
      },
      beforeCreate() { this.$i18n = { t: key => key }; },
    });
    view.$mount();
    assert.equal(!!view.$el.querySelector('el-tabs'), allowed);
    assert.equal(view.$el.textContent.includes('Hidden synopsis'), allowed);
    assert.equal(view.$el.textContent.includes('Hidden child content'), allowed);
    assert.equal(children, allowed ? 1 : 0);
    view.$destroy();
  });
}

for (const [file, method, expected] of [
  ['views/admin/contest/ContestList.vue', 'admin_deleteContest', [7, ' current-account-password ']],
  ['components/oj/group/ContestList.vue', 'deleteGroupContest', [7, '5', ' current-account-password ']],
]) {
  test(`${method} masks and validates the current account password before deleting`, async () => {
    const calls = [];
    const component = load(file, { '@/common/api': { [method]: async (...args) => { calls.push(args); } } });
    let promptOptions;
    let entered;
    const changes = [];
    const state = createState(component, { $route: { params: { groupID: '5' } },
      $prompt: async (message, title, options) => { promptOptions = options; return { value: entered }; },
      $emit() {}, currentChange: page => changes.push(page) });
    // Bind the overridden list refresh after component methods are installed.
    state.currentChange = page => changes.push(page);
    for (entered of ['', ' \n\t ']) {
      await state.deleteContest(7);
      assert.equal(calls.length, 0);
      assert.notEqual(promptOptions.inputValidator(entered), true);
    }
    assert.equal(promptOptions.inputType, 'password');
    assert.equal(promptOptions.closeOnClickModal, false);
    entered = ' current-account-password ';
    assert.equal(promptOptions.inputValidator(entered), true);
    await state.deleteContest(7);
    assert.deepEqual(calls, [expected]);
    assert.deepEqual(changes, [1]);
  });

  test(`${method} cancellation or backend rejection cannot delete or report success`, async () => {
    let calls = 0, refreshes = 0;
    const component = load(file, { '@/common/api': { [method]: async () => { calls++; throw new Error('wrong account password'); } } });
    const state = createState(component, { $route: { params: { groupID: '5' } }, $emit() {},
      $prompt: () => Promise.reject('cancel') });
    state.currentChange = () => { refreshes++; };
    await state.deleteContest(7);
    assert.equal(calls, 0);
    state.$prompt = async () => ({ value: 'wrong-password' });
    await state.deleteContest(7);
    assert.equal(calls, 1);
    assert.equal(refreshes, 0);
  });
}

test('both contest deletion APIs put identifiers and the account password in DELETE JSON, never the URL', async () => {
  const captured = [];
  const axios = config => { captured.push(config); return Promise.resolve({ data: { status: 200 } }); };
  axios.defaults = {};
  axios.interceptors = { request: { use() {} }, response: { use() {} } };
  const api = load('common/api.js', { axios, '@/common/security': {}, '@/router': {}, '@/store': {}, '@/i18n': {} });
  await api.admin_deleteContest(7, 'account-password');
  await api.deleteGroupContest(8, 5, 'group-owner-password');
  assert.deepEqual(captured.map(config => ({ url: config.url, method: config.method, params: config.params, data: config.data })), [
    { url: '/api/admin/contest', method: 'delete', params: {}, data: { cid: 7, password: 'account-password' } },
    { url: '/api/group/contest', method: 'delete', params: {}, data: { cid: 8, gid: 5, password: 'group-owner-password' } },
  ]);
  assert.ok(captured.every(config => !config.url.includes('?') && !Object.hasOwn(config.params, 'password')));
});

test.after(() => dom.window.close());
