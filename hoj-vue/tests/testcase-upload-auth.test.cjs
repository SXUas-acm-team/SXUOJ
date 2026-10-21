const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const babel = require('@babel/core');
const compiler = require('vue-template-compiler');
const { JSDOM } = require('jsdom');
const dom = new JSDOM('<!doctype html><html><body></body></html>', { url: 'https://oj.example/admin/' });
global.window = dom.window; global.document = dom.window.document;
global.localStorage = window.localStorage;
Object.defineProperty(global, 'navigator', { configurable: true, value: window.navigator });

function load(file, resolve) {
  const text = fs.readFileSync(path.join(__dirname, '../src', file), 'utf8');
  const source = file.endsWith('.vue') ? compiler.parseComponent(text).script.content : text;
  const output = babel.transformSync(source, { configFile: false, babelrc: false,
    plugins: ['@babel/plugin-transform-modules-commonjs'] }).code;
  const module = { exports: {} };
  new Function('require', 'module', 'exports', output)(resolve, module, module.exports);
  return module.exports.default || module.exports;
}

function client() {
  let requestHook, responseHook;
  const calls = [], commits = [];
  const axios = config => {
    const request = requestHook({ headers: {}, ...config }); calls.push(request);
    request.onUploadProgress({ loaded: 5, total: 10 });
    return Promise.resolve(responseHook({ config: request, data: { status: 200, data: { fileList: [] } },
      headers: { authorization: 'refreshed-token', 'refresh-token': 'true' } }));
  };
  axios.defaults = {};
  axios.interceptors = { request: { use(fn) { requestHook = fn; } }, response: { use(fn) { responseHook = fn; } } };
  const api = load('common/api.js', id => {
    if (id === 'axios') return axios;
    if (id === 'vue') return { prototype: {} };
    if (id === '@/store') return { commit: (...args) => commits.push(args) };
    if (id === '@/common/security') return { isLocalApiUrl: value => {
      const url = new URL(value, window.location.href);
      return url.origin === window.location.origin && url.pathname.startsWith('/api/');
    } };
    return {};
  });
  return { api, calls, commits };
}

for (const file of ['views/admin/problem/Problem.vue', 'components/oj/group/Problem.vue']) {
  test(`${file} connects Element UI upload to the authenticated API`, async () => {
    const calls = [];
    const source = fs.readFileSync(path.join(__dirname, '../src', file), 'utf8');
    assert.match(compiler.parseComponent(source).template.content, /:http-request="uploadTestcase"/);
    const component = load(file, id => {
      if (id === '@/common/api') return { uploadTestcaseZip: async options => { calls.push(options); return { status: 200 }; } };
      if (id === 'vuex') return { mapGetters: () => ({}) };
      return {};
    });
    const options = { action: '/api/file/upload-testcase-zip?mode=default' };
    assert.equal((await component.methods.uploadTestcase(options)).status, 200);
    assert.deepEqual(calls, [options]);
  });
}

test('ZIP upload reads the current token, sends multipart, refreshes it and remains abortable', async () => {
  const { api, calls, commits } = client();
  const progress = [];
  const options = { action: '/api/file/upload-testcase-zip?gid=7&mode=default', filename: 'file',
    file: new File(['synthetic zip'], 'cases.zip'), onProgress: value => progress.push(value.percent) };
  localStorage.setItem('token', 'new-current-token');
  const upload = api.uploadTestcaseZip(options);
  assert.equal((await upload).status, 200);
  assert.equal(calls[0].headers.Authorization, 'new-current-token');
  assert.equal(calls[0].data.get('file').name, 'cases.zip');
  assert.deepEqual(progress, [50]);
  assert.deepEqual(commits, [['refreshUserToken', 'refreshed-token']]);
  upload.abort();
  assert.equal(calls[0].signal.aborted, true);
});

test('upload refuses external and unrelated targets before sending credentials', async () => {
  const { api, calls } = client();
  for (const action of ['https://evil.example/api/file/upload-testcase-zip', '/api/admin/contest']) {
    await assert.rejects(api.uploadTestcaseZip({ action }));
  }
  assert.equal(calls.length, 0);
});

test('balloon color request includes the selected contest ID', async () => {
  const requests = [];
  const component = load('views/admin/problem/ProblemList.vue', id => {
    if (id === '@/common/api') return { admin_setContestProblemInfo: async data => { requests.push(data); } };
    if (id === '@/common/message') return { success() {} };
    if (id === 'vuex') return { mapGetters: () => ({}) };
    return {};
  });
  component.methods.changeContestProblemColor.call({ query: { contestId: '7' }, $i18n: { t: key => key } }, 11, '#123456');
  await Promise.resolve();
  assert.deepEqual(requests, [{ id: 11, cid: '7', color: '#123456' }]);
});
test.after(() => dom.window.close());
