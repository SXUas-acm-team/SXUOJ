const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const http = require('node:http');
const { JSDOM } = require('jsdom');
const jquery = fs.readFileSync(path.join(__dirname, '../../hoj-scrollBoard/js/jquery.min.js'), 'utf8');
const boardSource = fs.readFileSync(path.join(__dirname, '../../hoj-scrollBoard/js/scrollboard.js'), 'utf8');

function token(exp = Math.floor(Date.now() / 1000) + 3600, sub = 'administrator') {
  return Buffer.from(JSON.stringify({ alg: 'HS512', type: 'JWT' })).toString('base64url') + '.' +
    Buffer.from(JSON.stringify({ sub, exp, sessionEpoch: 'test-epoch' })).toString('base64url') + '.test_signature';
}
const metadata = { id: 7, rankShowName: 'username', problemCount: 1, startTime: '2026-10-20 12:00:00',
  sealRankTime: '2026-10-20 13:00:00', starUserList: [], balloonColor: { A: 'red' } };
const success = data => ({ ok: true, status: 200, redirected: false, json: async () => ({ status: 200, data }) });
function fixture(url = 'https://oj.example/scrollBoard/?cid=7') {
  const dom = new JSDOM('<!doctype html><html><body><div id="app-loader"></div><div id="myModal"><span id="myModalTitle"></span><span id="errorMsg"></span></div></body></html>',
    { runScripts: 'outside-only', url });
  const context = dom.getInternalVMContext();
  vm.runInContext(jquery, context);
  vm.runInContext(boardSource, context);
  context.$.fn.modal = function() { return this; };
  return { dom, context };
}
const infoPath = '/api/get-contest-scroll-board-info?cid=7';
const submissionPath = '/api/get-contest-scroll-board-submission?cid=7&removeStar=false';
const deferred = () => { let resolve; const promise = new Promise(yes => { resolve = yes; }); return { promise, resolve }; };

test('both scrollboard requests use the Vue raw JWT only in original same-origin Authorization headers', async () => {
  const { dom, context } = fixture();
  const jwt = token();
  context.localStorage.setItem('token', jwt);
  const requests = [];
  context.fetch = async (url, options) => { requests.push({ url, options }); return success(url.includes('-info') ? metadata : []); };
  try {
    assert.equal((await context.getContestInfo(7)).id, 7);
    assert.equal((await context.getSubmissionList(7, false)).length, 0);
    assert.deepEqual(requests.map(item => item.url), ['https://oj.example' + infoPath, 'https://oj.example' + submissionPath]);
    for (const { url, options } of requests) {
      assert.equal(options.headers.Authorization, jwt, 'Vue stores a raw JWT, without a JSON wrapper or Bearer prefix');
      assert.equal(options.mode, 'same-origin');
      assert.equal(options.credentials, 'same-origin');
      assert.equal(options.redirect, 'error');
      assert.equal(options.cache, 'no-store');
      assert.ok(!url.includes(jwt));
    }
  } finally { dom.window.close(); }
});

test('anonymous, expired, malformed and damaged cached credentials leave public scrollboards anonymous', async () => {
  const { dom, context } = fixture();
  const requests = [];
  context.fetch = async (url, options) => { requests.push(options); return success(metadata); };
  try {
    for (const value of [null, token(Math.floor(Date.now() / 1000) - 1), 'damaged-token', JSON.stringify(token()), 'null']) {
      if (value === null) context.localStorage.removeItem('token');
      else context.localStorage.setItem('token', value);
      assert.equal((await context.getContestInfo(7)).id, 7);
      assert.equal(requests.at(-1).headers.Authorization, undefined);
    }
    Object.defineProperty(context, 'localStorage', { configurable: true, get() { throw new Error('storage is disabled'); } });
    assert.equal((await context.getContestInfo(7)).id, 7);
    assert.equal(requests.at(-1).headers.Authorization, undefined);
  } finally { dom.window.close(); }
});

for (const envelopeOnly of [false, true]) {
  test(`a revoked session retries the same endpoint once without credentials (${envelopeOnly ? 'JSON' : 'HTTP'} 401)`, async () => {
    const { dom, context } = fixture();
    const jwt = token();
    const requests = [];
    context.localStorage.setItem('token', jwt);
    context.fetch = async (url, options) => {
      requests.push({ url, options });
      if (requests.length === 1) return { ok: envelopeOnly, status: envelopeOnly ? 200 : 401,
        json: async () => ({ status: 401, msg: 'Session expired' }) };
      return success(url.includes('-info') ? metadata : []);
    };
    try {
      assert.equal((await context.getContestInfo(7)).id, 7);
      await context.getSubmissionList(7, false);
      assert.equal(requests.length, 3);
      assert.equal(requests[0].url, requests[1].url);
      assert.equal(requests[0].options.headers.Authorization, jwt);
      assert.equal(requests[1].options.headers.Authorization, undefined);
      assert.equal(requests[2].options.headers.Authorization, undefined);
      assert.equal(context.localStorage.getItem('token'), jwt, 'the standalone board must not mutate another tab login');
    } finally { dom.window.close(); }
  });
}

test('permission denial is not retried anonymously and a second 401 is not turned into an empty successful board', async () => {
  const { dom, context } = fixture();
  context.localStorage.setItem('token', token());
  let requests = 0;
  context.fetch = async () => { requests++; return { ok: false, status: 403, json: async () => ({ status: 403, msg: 'Sealed' }) }; };
  try {
    await assert.rejects(context.getContestInfo(7), /Sealed/);
    assert.equal(requests, 1);
    context.fetch = async () => { requests++; return { ok: false, status: 401, json: async () => ({ status: 401, msg: 'Login required' }) }; };
    await assert.rejects(context.getContestInfo(7), /Login required/);
    assert.equal(requests, 3, 'one authenticated request and at most one anonymous retry');
  } finally { dom.window.close(); }
});

test('foreign origins, userinfo and unrelated endpoints cannot receive a scrollboard token', async () => {
  const { dom, context } = fixture();
  context.localStorage.setItem('token', token());
  context.fetch = () => assert.fail('an unapproved target must never be requested');
  try {
    for (const url of ['https://outside.example' + infoPath, '//outside.example' + infoPath,
      'https://user:secret@oj.example' + infoPath, '/api/get-user-auth-info', 'javascript:alert(1)']) {
      await assert.rejects(context.requestScrollBoardData(url), /original same-origin API/);
    }
  } finally { dom.window.close(); }
});

test('redirect:error prevents a real redirected request from forwarding the JWT to another origin', async () => {
  let foreignCalls = 0;
  let receivedToken;
  const foreign = http.createServer((request, response) => { foreignCalls++; response.end('{}'); });
  await new Promise(resolve => foreign.listen(0, '127.0.0.1', resolve));
  const original = http.createServer((request, response) => {
    receivedToken = request.headers.authorization;
    response.writeHead(302, { Location: `http://127.0.0.1:${foreign.address().port}/capture`, Connection: 'close' });
    response.end();
  });
  await new Promise(resolve => original.listen(0, '127.0.0.1', resolve));
  const { dom, context } = fixture(`http://127.0.0.1:${original.address().port}/scrollBoard/?cid=7`);
  const jwt = token();
  context.localStorage.setItem('token', jwt);
  context.fetch = global.fetch;
  try {
    await assert.rejects(context.getContestInfo(7));
    assert.equal(receivedToken, jwt);
    assert.equal(foreignCalls, 0);
  } finally {
    dom.window.close();
    await Promise.all([new Promise(resolve => original.close(resolve)), new Promise(resolve => foreign.close(resolve))]);
  }
});

test('the board is only constructed after both asynchronous authorized responses finish', async () => {
  const { dom, context } = fixture();
  const first = deferred(), second = deferred(), secondStarted = deferred();
  const requests = [];
  let rendered = 0;
  context.fetch = (url, options) => {
    requests.push({ url, options });
    if (requests.length === 1) return first.promise;
    secondStarted.resolve();
    return second.promise;
  };
  context.Board.prototype.showInitBoard = function() { rendered++; };
  try {
    const pending = context.initializeScrollBoard();
    assert.equal(requests.length, 1);
    assert.equal(rendered, 0);
    first.resolve(success(metadata));
    await secondStarted.promise;
    assert.equal(requests.length, 2);
    assert.equal(rendered, 0);
    second.resolve(success([]));
    const board = await pending;
    assert.equal(board.contestInfo.id, 7);
    assert.equal(board.teamCount, 0);
    assert.equal(rendered, 1);
    assert.equal(context.document.getElementById('app-loader'), null);
  } finally { dom.window.close(); }
});

test('a failed submission request cannot construct a partial board or hide its permission error', async () => {
  const { dom, context } = fixture();
  let rendered = 0;
  context.Board.prototype.showInitBoard = function() { rendered++; };
  context.fetch = async url => url.includes('-info') ? success(metadata) :
    { ok: false, status: 403, json: async () => ({ status: 403, msg: 'Contest content is sealed' }) };
  try {
    assert.equal(await context.initializeScrollBoard(), null);
    assert.equal(rendered, 0);
    assert.equal(context.document.getElementById('errorMsg').textContent, 'Contest content is sealed');
  } finally { dom.window.close(); }
});

test('changing or clearing the Vue session while a request is in flight discards its private response', async () => {
  const { dom, context } = fixture();
  const task = deferred();
  context.localStorage.setItem('token', token());
  context.fetch = () => task.promise;
  try {
    const pending = context.initializeScrollBoard();
    context.localStorage.removeItem('token');
    task.resolve(success(metadata));
    assert.equal(await pending, null);
    assert.match(context.document.getElementById('errorMsg').textContent, /登录状态已变化/);
  } finally { dom.window.close(); }
});
