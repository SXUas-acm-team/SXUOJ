const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { JSDOM } = require('jsdom');
const dom = new JSDOM('<!doctype html><html><body></body></html>', { url: 'https://oj.example/', runScripts: 'outside-only' });
global.window = dom.window;
global.document = dom.window.document;
global.navigator = dom.window.navigator;
const Vue = require('vue');
const compiler = require('vue-template-compiler');
const babel = require('@babel/core');
const { sanitizeHtml, safeExternalUrl, plugin } = require('../src/common/security');
const { chartRankRecords, normalizeDifficulty, normalizePageSize, normalizePage } = require('../src/common/viewSafety');
Vue.use(plugin);
Vue.directive('highlight', {});
Vue.directive('katex', {});
Vue.config.productionTip = false;
Vue.config.devtools = false;
function loadJS(src, mocks = {}) {
  const module = { exports: {} };
  const output = babel.transformSync(src, { configFile: false, babelrc: false, plugins: ['@babel/plugin-transform-modules-commonjs'] }).code;
  const resolver = id => Object.prototype.hasOwnProperty.call(mocks, id) ? mocks[id] : id.startsWith('@/') ? {} : require(id);
  new Function('require', 'module', 'exports', output)(resolver, module, module.exports);
  return module.exports;
}
function loadComponent(file, mocks = {}) {
  const source = fs.readFileSync(path.join(__dirname, '../src', file), 'utf8');
  const parsed = compiler.parseComponent(source);
  return { ...loadJS(parsed.script.content, mocks).default, ...compiler.compileToFunctions(parsed.template.content) };
}
const payloads = [
  '<img src=x onerror="window.pwned=1">',
  '<svg><a xlink:href="javascript:window.pwned=1">x</a></svg>',
  '<a href="java&#x0a;script:window.pwned=1">click</a>',
  '<iframe srcdoc="<script>window.pwned=1</script>"></iframe>',
  '<math><mtext><table><mglyph><style><!--</style><img title="--><img src=1 onerror=window.pwned=1>">',
];
function assertSafeHtml(html) {
  const container = document.createElement('div'); container.innerHTML = html;
  assert.equal(container.querySelectorAll('script,iframe,object,embed,svg,style,form').length, 0);
  for (const element of container.querySelectorAll('*')) {
    for (const attr of element.attributes) {
      assert.ok(!attr.name.toLowerCase().startsWith('on'), `${attr.name} must be stripped`);
      assert.ok(!/^(javascript|vbscript):/i.test(attr.value.replace(/\s/g, '')), `${attr.value} must be stripped`);
    }
  }
}
test('stored/imported rich text strips active content and keeps formatting', () => {
  payloads.forEach(payload => assertSafeHtml(sanitizeHtml(payload)));
  const clean = sanitizeHtml('<h2>Title</h2><table><tr><td>data</td></tr></table><pre><code>&lt;img onerror=x&gt;</code></pre><a target="_blank" href="https://example.com/a.pdf">PDF</a>');
  assert.match(clean, /<h2>Title<\/h2>/); assert.match(clean, /<table>/); assert.match(clean, /rel="noopener noreferrer"/);
});
test('Markdown remains sanitized when a caller marks an author as trusted', () => {
  const component = loadComponent('components/oj/common/Markdown.vue');
  const app = new Vue({ render: h => h(component, { props: { content: payloads.join(''), isAvoidXss: false } }) });
  Vue.prototype.$markDown = { render: text => text };
  app.$mount(); assertSafeHtml(app.$el.innerHTML); app.$destroy();
});
test('administrator report tags and body are text nodes', () => {
  const component = loadComponent('views/admin/discussion/Discussion.vue');
  const view = new Vue(); let captured;
  component.methods.openReportDialog.call({ $createElement: view.$createElement, $i18n: { t: key => key }, $alert: (content, title, options) => { captured = content; assert.ok(!options.dangerouslyUseHTMLString); } }, '#<img src=x onerror=window.pwned=1># <svg onload=window.pwned=1>report');
  const app = new Vue({ render: () => captured }); app.$mount();
  assert.equal(app.$el.querySelectorAll('img,svg').length, 0); assert.match(app.$el.textContent, /<svg onload/); app.$destroy();
});
test('COPY uses the literal code value without parsing HTML or duplicating handlers', () => {
  const $ = require('jquery'); const copied=[];
  document.body.innerHTML='<pre><code></code></pre>';
  const payload='</textarea><img src=x onerror="window.pwned=1"><svg onload=window.pwned=1>';
  $('code').text(payload);
  document.execCommand = () => { copied.push(document.querySelector('textarea').value); assert.equal(document.querySelectorAll('img,svg').length, 0); return true; };
  const { addCodeBtn } = loadJS(fs.readFileSync(path.join(__dirname, '../src/common/codeblock.js'), 'utf8'), { '@/common/message': { success() {} } });
  addCodeBtn(); addCodeBtn(); $('i.code-copy').trigger('click');
  assert.deepEqual(copied,[payload]); assert.equal($('i.code-copy').length,1); assert.equal($('textarea').length,0);
});
test('profile links only allow absolute HTTP(S) URLs', () => {
  for(const value of ['javascript:alert(1)','JaVaScRiPt:alert(1)','java\nscript:alert(1)','data:text/html,test','vbscript:x','//evil.test',' https://example.com']) assert.equal(safeExternalUrl(value),'');
  assert.equal(safeExternalUrl('https://example.com/blog'), 'https://example.com/blog');
});
test('scrollboard encodes team names, schools, IDs and balloon attributes', () => {
  const boardDom = new JSDOM('<!doctype html><html><body></body></html>', { runScripts:'outside-only', url:'https://oj.example/' });
  const context = boardDom.getInternalVMContext();
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../../hoj-scrollBoard/js/jquery.min.js'),'utf8'),context);
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../../hoj-scrollBoard/js/scrollboard.js'),'utf8'),context);
  const identifier='A"><img src=x onerror=window.pwned=1>';
  const team = { teamId:'uid" onclick="window.pwned=1', teamName:'<img src=x onerror=window.pwned=1>', teamSchool:'<svg onload=window.pwned=1>', solved:0, penalty:0, official:false, girl:false, submitProblemList:{} };
  const fake={ problemCount:1, problemList:[identifier], balloonColor:{[identifier]:'red" onload="window.pwned=1'}, teamCount:1, teamNowSequence:[team], medalRanks:[] };
  context.Board.prototype.showInitBoard.call(fake);
  const doc=boardDom.window.document;
  assert.equal(doc.querySelectorAll('img,script').length,0);
  assert.equal(doc.querySelector('.team-name .fw-bold').textContent,team.teamName);
  assert.equal(doc.querySelector('.univ').textContent,team.teamSchool);
  assert.equal(doc.querySelector('.problem-status').getAttribute('alphabet-id'),identifier);
  for(const element of doc.querySelectorAll('*'))for(const attr of element.attributes)assert.ok(!attr.name.startsWith('on'));
  boardDom.window.close();
});
test('charts remove actual duplicated prefixes and preserve top ten ranking order', () => {
  const ranked=Array.from({length:12},(_,i)=>({uid:String(i),rank:i+1}));
  assert.deepEqual(chartRankRecords([ranked[4],ranked[0],...ranked]),ranked.slice(0,10));
  assert.deepEqual(chartRankRecords(ranked),ranked.slice(0,10));
});
test('difficulty and pagination normalize legacy and hostile URL values', () => {
  assert.deepEqual(['Easy','Mid','Hard','0','1','2'].map(normalizeDifficulty),['0','1','2','0','1','2']);
  assert.equal(normalizeDifficulty('unknown'),'');
  for(const n of ['2147483647','-1','1e20','abc',[],{},'10foo'])assert.equal(normalizePageSize(n),10);
  for(const n of ['0','-2','1e20','1.2'])assert.equal(normalizePage(n),1);
  assert.equal(normalizePageSize('100'),100);
});
test('comments accept s while blank Unicode whitespace is rejected', () => {
  const received=[]; const api={addComment(value){received.push(value);return {then(){}};},addReply(value){received.push(value);return {then(){}};}};
  const component=loadComponent('components/oj/comment/comment.vue',{'@/common/api':api,'@/common/message':{warning(){},error(){},success(){}}});
  const state={isAuthenticated:true, ownInputComment:'s',replyInputComment:'s', replyObj:{},$i18n:{t:key=>key}};
  component.methods.commitComment.call(state); component.methods.commitReply.call(state); assert.equal(received.length,2);
  state.ownInputComment=' \t\n\u3000'; state.replyInputComment=' \t\n\u3000';
  component.methods.commitComment.call(state);component.methods.commitReply.call(state);assert.equal(received.length,2);
});
test('rank and navbar resize listeners coexist and clean up independently', () => {
  const nav=loadComponent('components/oj/common/NavBar.vue'); const rank=loadComponent('views/oj/rank/ACMRank.vue');
  let calls=0; const navState={page_width(){calls++;},setHiddenHeaderHeight(){calls++;}};const rankState={};
  nav.created.call(navState);rank.created.call(rankState);
  window.dispatchEvent(new window.Event('resize'));assert.equal(calls,3);assert.equal(rankState.screenWidth,document.documentElement.clientWidth);
  rank.beforeDestroy.call(rankState);window.dispatchEvent(new window.Event('resize'));assert.equal(calls,5);
  nav.beforeDestroy.call(navState);window.dispatchEvent(new window.Event('resize'));assert.equal(calls,5);
});
test('changing judge mode invalidates uploaded test cases and previous compilation', () => {
  const component=loadComponent('views/admin/problem/Problem.vue');
  const state={testCaseUploaded:true,problem:{uploadTestcaseDir:'old',testCaseScore:[1],spjCompileOk:true},$createElement(){},$i18n:{t:key=>key},$msgbox(){}};
  component.methods.switchMode.call(state,'default'); assert.equal(state.testCaseUploaded,false); assert.equal(state.problem.uploadTestcaseDir,'');assert.deepEqual(state.problem.testCaseScore,[]);assert.equal(state.problem.spjCompileOk,false);
});
test('editor Markdown renderer sanitizes before third-party previews and highlights locally', () => {
  const { secureMarkdownRenderer } = require('../src/common/security');
  const markdown = require('mavon-editor').mavonEditor.getMarkdownIt();
  secureMarkdownRenderer(markdown, require('highlight.js'));
  assertSafeHtml(markdown.render(payloads.join('')));
  assertSafeHtml(markdown.renderInline('<img src=x onerror=window.pwned=1>'));
  const rendered=markdown.render('```js\nconst text = "</textarea><img onerror=x>";\n```');
  assert.match(rendered, /class="hljs"/); assert.match(rendered, /hljs-keyword/);assertSafeHtml(rendered);
});

test('editor previews import payloads without loading third-party scripts', async () => {
  const mavon = require('mavon-editor'); Vue.use(mavon);
  const component = loadComponent('components/admin/Editor.vue');
  component.computed = { isAdminRole: () => false, isGroupAdmin: () => false };
  const scriptsBefore = document.querySelectorAll('script[src]').length;
  const app = new Vue({ render: h => h(component, { props: { value: payloads.join('') } }) });
  app.$mount(); await Vue.nextTick(); await new Promise(resolve => setTimeout(resolve, 300)); await Vue.nextTick();
  const previews = app.$el.querySelectorAll('.v-show-content,.v-note-read-content');
  assert.ok(previews.length >= 1); previews.forEach(preview => assertSafeHtml(preview.innerHTML));
  assert.equal(document.querySelectorAll('script[src]').length, scriptsBefore);
  app.$destroy();
});
test('upgraded Axios keeps API headers, JSON bodies, token refresh and multipart data', async () => {
  const axios = require('axios');
  const { isLocalApiUrl } = require('../src/common/security');
  const commits=[]; let captured;
  global.localStorage=window.localStorage;localStorage.setItem('token','regression-test-token');
  Vue.prototype.$notify={error(){}};
  loadJS(fs.readFileSync(path.join(__dirname,'../src/common/api.js'),'utf8'),{
    '@/common/security':{isLocalApiUrl}, '@/common/message':{error(){}}, '@/router':{push(){}}, '@/store':{commit(...args){commits.push(args);}}, '@/i18n':{t:key=>key}
  });
  const adapter=async config=>{captured=config;return{config,data:{status:200},headers:{'refresh-token':'yes',authorization:'new-regression-token'},status:200,statusText:'OK'};};
  await axios.post('/api/admin/problem',{title:'安全测试'},{adapter});
  assert.equal(captured.headers.get('Authorization'),'regression-test-token');
  assert.equal(captured.headers.get('Url-Type'),'admin');
  assert.deepEqual(JSON.parse(captured.data),{title:'安全测试'});
  assert.deepEqual(commits[0],['changeUserToken','new-regression-token']);
  await axios.get('https://outside.example/api/problem',{adapter});
  assert.equal(captured.headers.get('Authorization'),undefined);
  await axios.post('/api/login',{username:'test'},{adapter});
  assert.equal(captured.headers.get('Authorization'),undefined);
  const form=new window.FormData(); form.append('file',new window.Blob(['safe']),'test.txt');
  await axios.post('/api/file/upload-md-file',form,{adapter});assert.equal(captured.data,form);
  axios.interceptors.request.clear();axios.interceptors.response.clear();localStorage.clear();
});
test('upgraded KaTeX renders ordinary math and rejects untrusted URLs', () => {
  const katex=require('katex');
  const html=katex.renderToString('E=mc^2',{throwOnError:false,trust:false});
  assert.match(html,/class="katex"/);assertSafeHtml(sanitizeHtml(html));
  const payload=katex.renderToString('\\href{javascript:alert(1)}{bad}',{throwOnError:false,trust:false});
  assert.ok(!/href="javascript:/i.test(payload));assertSafeHtml(sanitizeHtml(payload));
});
test('KaTeX auto-render directive remains compatible after its package upgrade', () => {
  const plugin=loadJS(fs.readFileSync(path.join(__dirname,'../src/common/katex.js'),'utf8'),{'katex/dist/katex.min.css':{}}).default;
  let directive;plugin.install({directive(name,definition){directive=definition;}});
  const element=document.createElement('div');element.textContent='$x^2$';directive.bind(element,{});
  assert.ok(element.querySelector('.katex'));assert.equal(element.querySelectorAll('[href^="javascript:"]').length,0);
});
test('ranking chart tooltips render player names without an HTML sink', () => {
  const echarts=require('echarts');
  const oldContext=window.HTMLCanvasElement.prototype.getContext;
  window.HTMLCanvasElement.prototype.getContext=()=>({measureText:text=>({width:String(text).length*8})});
  const utils=loadJS(fs.readFileSync(path.join(__dirname,'../src/common/utils.js'),'utf8')).default; const component=loadComponent('views/oj/rank/OIRank.vue',{'@/common/utils':utils});const state=component.data.call({$i18n:{t:key=>key}});
  assert.equal(state.options.tooltip.renderMode,'richText');
  state.options.xAxis[0].data=['<img src=x onerror=window.pwned=1>'];
  state.options.series.forEach(series=>series.data=[1]);
  const element=document.createElement('div');element.style.width='600px';element.style.height='400px';
  const chart=echarts.init(element,null,{renderer:'svg',width:600,height:400});
  try { state.options.animation=false;chart.setOption(state.options);chart.dispatchAction({type:'showTip',seriesIndex:0,dataIndex:0});
  assert.equal(element.querySelectorAll('img,script').length,0);assert.ok(element.querySelector('svg'));
  } finally { chart.dispose();window.HTMLCanvasElement.prototype.getContext=oldContext; }
});


test('VXE setup/install reject the upstream prototype pollution PoC before merging', () => {
  require('xe-utils');
  const loaded=require('vxe-table');const table=loaded.default||loaded;
  const {hardenVxeTable}=require('../src/common/tableSecurity');hardenVxeTable(table);
  const payloads=[JSON.parse('{"__proto__":{"pollutedKey":123}}'),JSON.parse('{"table":{"constructor":{"prototype":{"pollutedKey":123}}}}')];
  for(const payload of payloads){
    assert.throws(()=>table.setup(payload),/Unsafe table option key/);
    assert.throws(()=>table.install(Vue,payload),/Unsafe table option key/);
    assert.equal(({}).pollutedKey,undefined);
  }
  table.setup({i18n:key=>key});
});
test('guarded VXE still installs and renders ordinary and hostile text cells', async () => {
  const loaded=require('vxe-table');const table=loaded.default||loaded;
  Vue.use(table);
  const name='<img src=x onerror=window.pwned=1>';
  const app=new Vue({render:h=>h('vxe-table',{props:{data:[{name}],height:200}},[h('vxe-table-column',{props:{field:'name',title:'Name'}})])});
  document.body.appendChild(app.$mount().$el);await Vue.nextTick();await new Promise(resolve=>setTimeout(resolve,100));await Vue.nextTick();
  assert.equal(app.$el.querySelectorAll('img,script').length,0);assert.ok(app.$el.textContent.includes(name));
  const element=app.$el;app.$destroy();element.remove();
});
