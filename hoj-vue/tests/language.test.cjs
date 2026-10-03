const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM } = require('jsdom');
const babel = require('@babel/core');

const dom = new JSDOM('<!doctype html><html><body></body></html>', {
  url: 'https://oj.example/',
});
global.window = dom.window;
global.document = dom.window.document;
global.localStorage = dom.window.localStorage;
Object.defineProperty(global, 'navigator', { configurable: true, value: dom.window.navigator });
Object.defineProperty(window.navigator, 'language', { configurable: true, value: 'en-US' });
Object.defineProperty(window.navigator, 'userLanguage', { configurable: true, value: 'en-US' });
Object.defineProperty(window.screen, 'width', { configurable: true, value: 1440 });

const Vue = require('vue');
const compiler = require('vue-template-compiler');
const moment = require('moment');
Vue.config.productionTip = false;
Vue.config.devtools = false;
Vue.config.ignoredElements = [/^el-/, /^mu-/, 'router-view'];

const emptyComponent = { render: h => h('span') };
Vue.component('el-dialog', emptyComponent);
Vue.component('el-dropdown', {
  provide() {
    return { selectLanguage: command => this.$emit('command', command) };
  },
  render(h) {
    return h('div', [this.$slots.default, this.$slots.dropdown]);
  },
});
Vue.component('el-dropdown-item', {
  inject: ['selectLanguage'],
  props: ['command'],
  render(h) {
    return h('button', {
      attrs: { 'data-language': this.command },
      on: { click: () => this.selectLanguage(this.command) },
    }, this.$slots.default);
  },
});

// Transform the same application modules used by webpack; only unrelated APIs,
// store modules and heavyweight child views are replaced for this browser fixture.
const sourceRoot = path.join(__dirname, '../src');
const transformed = new Map();
function createRuntime() {
  const cache = new Map();
  const mocks = {
    '@/common/api': {},
    '@/common/message': {},
    '@/common/utils': { isFocusModePage: () => false },
    '@/common/logo': { LOGO: '', MOTTO: '' },
    '@/components/oj/common/NavBar': emptyComponent,
    '@/assets/backstage.png': 'admin-logo.png',
    '@/store/contest': {},
    '@/store/training': {},
    '@/store/group': {},
    'vue-avatar': emptyComponent,
  };
  function load(file) {
    let absolute = path.resolve(sourceRoot, file);
    if (!path.extname(absolute)) {
      absolute = fs.existsSync(absolute + '.js') ? absolute + '.js' : path.join(absolute, 'index.js');
    }
    if (cache.has(absolute)) return cache.get(absolute).exports;
    const module = { exports: {} };
    cache.set(absolute, module);
    if (!transformed.has(absolute)) {
      let source = fs.readFileSync(absolute, 'utf8');
      if (absolute.endsWith('.vue')) source = compiler.parseComponent(source).script.content;
      transformed.set(absolute, babel.transformSync(source, {
        configFile: false,
        babelrc: false,
        plugins: ['@babel/plugin-transform-modules-commonjs'],
      }).code);
    }
    const resolve = id => {
      if (Object.prototype.hasOwnProperty.call(mocks, id)) return mocks[id];
      if (id.startsWith('@/')) return load(id.slice(2));
      if (id.startsWith('.')) return load(path.resolve(path.dirname(absolute), id));
      return require(id);
    };
    new Function('require', 'module', 'exports', transformed.get(absolute))(resolve, module, module.exports);
    return module.exports;
  }
  function component(file) {
    const parsed = compiler.parseComponent(fs.readFileSync(path.join(sourceRoot, file), 'utf8'));
    return { ...load(file).default, ...compiler.compileToFunctions(parsed.template.content) };
  }
  return {
    load,
    component,
    language: load('i18n/language.js'),
    i18n: load('i18n/index.js').default,
    store: load('store/index.js').default,
  };
}

function initializeApp(runtime, query = {}) {
  return new Vue({
    ...runtime.load('App.vue').default,
    store: runtime.store,
    i18n: runtime.i18n,
    beforeCreate() {
      this.$route = { path: '/home', name: 'Home', query };
    },
  });
}

function destroyApp(app) {
  window.removeEventListener('visibilitychange', app.autoRefreshUserInfo);
  app.$destroy();
}

test.beforeEach(() => {
  localStorage.clear();
  document.documentElement.removeAttribute('lang');
  moment.locale('en');
});
test.after(() => dom.window.close());

test('an English browser starts in Chinese before the first mount', () => {
  const runtime = createRuntime();
  assert.equal(window.navigator.language, 'en-US');
  assert.equal(runtime.language.resolveLanguage(), 'zh-CN');
  assert.equal(runtime.i18n.locale, 'zh-CN');
  assert.equal(runtime.i18n.fallbackLocale, 'zh-CN');
  assert.equal(runtime.store.getters.webLanguage, runtime.i18n.locale);
  const app = initializeApp(runtime);
  try {
    assert.equal(app._isMounted, false);
    assert.equal(document.documentElement.lang, 'zh-CN');
    assert.equal(moment.locale(), 'zh-cn');
    assert.equal(JSON.parse(localStorage.getItem('Web_Language')), 'zh-CN');
  } finally {
    destroyApp(app);
  }
});

test('invalid URL values and malformed preferences fall back to Chinese without throwing', () => {
  for (const raw of ['invalid JSON', '"fr-FR"', '{"language":"en-US"}', '["en-US"]']) {
    localStorage.setItem('Web_Language', raw);
    const runtime = createRuntime();
    assert.equal(runtime.i18n.locale, 'zh-CN');
    for (const query of ['fr-FR', ['en-US', 'zh-CN'], {}, '__proto__']) {
      localStorage.setItem('Web_Language', raw);
      const app = initializeApp(runtime, { l: query });
      try {
        assert.equal(runtime.store.getters.webLanguage, 'zh-CN');
        assert.equal(runtime.i18n.locale, 'zh-CN');
      } finally {
        destroyApp(app);
      }
    }
  }
});

test('supported URL aliases override a saved preference and remain selected after reload', () => {
  localStorage.setItem('Web_Language', JSON.stringify('zh-CN'));
  const runtime = createRuntime();
  const app = initializeApp(runtime, { l: 'EN' });
  try {
    assert.equal(app._isMounted, false);
    assert.equal(runtime.store.getters.webLanguage, 'en-US');
    assert.equal(runtime.i18n.locale, 'en-US');
    assert.equal(document.documentElement.lang, 'en-US');
    assert.equal(JSON.parse(localStorage.getItem('Web_Language')), 'en-US');
  } finally {
    destroyApp(app);
  }
  const reloaded = createRuntime();
  assert.equal(reloaded.i18n.locale, 'en-US');
  assert.equal(reloaded.store.getters.webLanguage, 'en-US');
  assert.equal(reloaded.language.resolveLanguage('unsupported'), 'en-US');
  for (const [alias, expected] of [
    ['zh', 'zh-CN'], ['zh-Hans-CN', 'zh-CN'], ['zh-Hant', 'zh-TW'],
    ['zh-Hant-TW', 'zh-TW'], ['ja', 'ja-JP'], ['ko', 'ko-KR'],
  ]) {
    assert.equal(reloaded.language.resolveLanguage(alias), expected);
  }
});

test('manual changes keep Vuex, translations, dates, HTML and persisted preferences in sync', () => {
  const runtime = createRuntime();
  runtime.store.commit('changeWebLanguage', { language: 'zh-Hant' });
  assert.equal(runtime.store.getters.webLanguage, 'zh-TW');
  assert.equal(runtime.i18n.locale, 'zh-TW');
  assert.equal(moment.locale(), 'zh-tw');
  assert.equal(document.documentElement.lang, 'zh-TW');
  assert.equal(JSON.parse(localStorage.getItem('Web_Language')), 'zh-TW');
  for (const language of ['unsupported', null, ['en-US']]) {
    runtime.store.commit('changeWebLanguage', { language });
    assert.equal(runtime.store.getters.webLanguage, 'zh-TW');
    assert.equal(runtime.i18n.locale, 'zh-TW');
    assert.equal(document.documentElement.lang, 'zh-TW');
    assert.equal(JSON.parse(localStorage.getItem('Web_Language')), 'zh-TW');
  }
  const reloaded = createRuntime();
  assert.equal(reloaded.i18n.locale, 'zh-TW');
  assert.equal(reloaded.store.getters.webLanguage, 'zh-TW');
});

test('a delayed route language is applied while ordinary navigation preserves manual choices', () => {
  const runtime = createRuntime();
  runtime.store.state.route = { meta: { title: 'Home' } };
  const app = initializeApp(runtime);
  const routeWatcher = runtime.load('App.vue').default.watch.$route;
  function navigate(path, query) {
    const previous = app.$route;
    app.$route = { path, name: 'Admin', query };
    routeWatcher.call(app, app.$route, previous);
  }
  try {
    navigate('/admin/dashboard', { l: 'en' });
    assert.equal(runtime.store.getters.webLanguage, 'en-US');
    assert.equal(runtime.i18n.locale, 'en-US');
    runtime.store.commit('changeWebLanguage', { language: 'ja' });
    navigate('/admin/problem', { l: 'en' });
    assert.equal(runtime.i18n.locale, 'ja-JP');
    navigate('/admin/problem', { l: ['en', 'zh'] });
    assert.equal(runtime.i18n.locale, 'ja-JP');
    navigate('/admin/problem', { l: 'zh' });
    assert.equal(runtime.i18n.locale, 'zh-CN');
    assert.equal(runtime.store.getters.webLanguage, 'zh-CN');
  } finally {
    destroyApp(app);
  }
});

test('logout clears authentication and session data but preserves the chosen language', () => {
  const runtime = createRuntime();
  runtime.store.commit('changeWebLanguage', { language: 'ja' });
  runtime.store.commit('changeUserToken', 'test-session-token');
  runtime.store.commit('changeUserInfo', { userInfo: { username: 'test-user' } });
  localStorage.setItem('contest-session', JSON.stringify({ contest: 123 }));
  runtime.store.commit('clearUserInfoAndToken');
  assert.equal(runtime.store.getters.isAuthenticated, false);
  assert.deepEqual(runtime.store.getters.userInfo, {});
  assert.equal(localStorage.getItem('token'), null);
  assert.equal(localStorage.getItem('userInfo'), null);
  assert.equal(localStorage.getItem('contest-session'), null);
  assert.equal(JSON.parse(localStorage.getItem('Web_Language')), 'ja-JP');
  const reloaded = createRuntime();
  assert.equal(reloaded.i18n.locale, 'ja-JP');
  assert.equal(reloaded.store.getters.webLanguage, 'ja-JP');
  assert.equal(reloaded.store.getters.isAuthenticated, false);
});

test('admin logo clicks preserve language while the footer menu still switches it', async () => {
  const runtime = createRuntime();
  const component = runtime.component('views/admin/Home.vue');
  const app = new Vue({
    ...component,
    components: { ...component.components, KatexEditor: emptyComponent },
    store: runtime.store,
    i18n: runtime.i18n,
    beforeCreate() {
      this.$route = { path: '/admin/dashboard', matched: [] };
    },
  });
  const previousResize = window.onresize;
  try {
    app.$mount();
    await Vue.nextTick();
    const logo = app.$el.querySelector('.logo');
    assert.ok(logo, 'desktop admin logo is rendered');
    logo.dispatchEvent(new window.MouseEvent('click', { bubbles: true }));
    await Vue.nextTick();
    assert.equal(runtime.store.getters.webLanguage, 'zh-CN');
    assert.equal(runtime.i18n.locale, 'zh-CN');
    const english = app.$el.querySelector('.footer [data-language="en-US"]');
    assert.ok(english, 'explicit English option remains available');
    english.click();
    await Vue.nextTick();
    assert.equal(runtime.store.getters.webLanguage, 'en-US');
    assert.equal(runtime.i18n.locale, 'en-US');
    logo.click();
    assert.equal(runtime.store.getters.webLanguage, 'en-US');
    app.$el.querySelector('.footer [data-language="zh-CN"]').click();
    await Vue.nextTick();
    assert.equal(runtime.store.getters.webLanguage, 'zh-CN');
    assert.equal(runtime.i18n.locale, 'zh-CN');
  } finally {
    app.$destroy();
    window.onresize = previousResize;
  }
});
