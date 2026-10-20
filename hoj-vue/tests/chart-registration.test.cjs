const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM } = require('jsdom');
const babel = require('@babel/core');
const compiler = require('vue-template-compiler');
const dom = new JSDOM('<!doctype html><html><body></body></html>');
global.window = dom.window;
global.document = dom.window.document;
Object.defineProperty(global, 'navigator', { configurable: true, value: dom.window.navigator });
window.HTMLCanvasElement.prototype.getContext = () => ({ measureText: text => ({ width: String(text).length * 8 }) });

function execute(source, resolve) {
  const output = babel.transformSync(source, { configFile: false, babelrc: false,
    plugins: ['@babel/plugin-transform-modules-commonjs'] }).code;
  const module = { exports: {} };
  new Function('require', 'module', 'exports', output)(resolve, module, module.exports);
  return module.exports.default || module.exports;
}

// Load exactly the application entry's module list, never the all-in-one
// `echarts` entry that would conceal a missing chart import in production.
execute(fs.readFileSync(path.join(__dirname, '../src/common/charts.js'), 'utf8'), require);
// Only the test renderer differs from the browser's canvas renderer.
require('zrender/lib/svg/svg');
const echarts = require('echarts/lib/echarts');

function optionsFor(relative) {
  const source = fs.readFileSync(path.join(__dirname, '../src', relative), 'utf8');
  const component = execute(compiler.parseComponent(source).script.content, name => {
    if (name === 'vuex') return { mapGetters: () => ({}) };
    if (name === '@/common/utils') return { breakLongWords: text => text };
    return {};
  });
  return component.data.call({ $i18n: { t: value => value } }).options;
}

for (const [label, component, labels, values] of [
  ['weekly submissions', 'components/oj/home/SubmissionStatistic.vue', ['10-19', '10-20'], [2, 4]],
  ['ACM ranking', 'views/oj/rank/ACMRank.vue', ['first', 'second'], [10, 8]],
  ['OI ranking', 'views/oj/rank/OIRank.vue', ['first', 'second'], [10, 8]],
]) {
  test(`the production chart module list renders ${label}`, () => {
    const host = document.createElement('div');
    document.body.appendChild(host);
    const chart = echarts.init(host, null, { renderer: 'svg', width: 600, height: 400 });
    try {
      const options = optionsFor(component);
      options.animation = false;
      options.xAxis[0].data = labels;
      options.series.forEach(series => { series.data = values; });
      chart.setOption(options);
      chart.showLoading('default', { maskColor: 'rgba(250,250,250,.8)' });
      chart.hideLoading();
      assert.equal(chart.getOption().series.length, options.series.length);
      assert.ok(host.querySelector('svg'));
      assert.ok(host.querySelectorAll('svg path').length > 1, 'real series shapes must be rendered');
    } finally { chart.dispose(); host.remove(); }
  });
}

test.after(() => dom.window.close());
