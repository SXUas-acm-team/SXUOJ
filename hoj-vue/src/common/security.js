const DOMPurify = require('dompurify');

// Stored/imported HTML crosses the same boundary, regardless of the author's role.
function sanitizeHtml(value) {
  return DOMPurify.sanitize(value == null ? '' : String(value), {
    USE_PROFILES: { html: true, mathMl: true },
    FORBID_TAGS: ['style', 'iframe', 'object', 'embed', 'form', 'input', 'textarea', 'button', 'link', 'meta', 'base'],
    FORBID_ATTR: ['srcdoc'],
    ADD_ATTR: ['target'],
  });
}

function safeExternalUrl(value) {
  if (typeof value !== 'string' || /[\u0000-\u0020\u007f]/.test(value)) return '';
  try {
    const url = new URL(value);
    return url.protocol === 'https:' || url.protocol === 'http:' ? url.href : '';
  } catch (_) {
    return '';
  }
}

DOMPurify.addHook('afterSanitizeAttributes', node => {
  if (node.getAttribute && node.getAttribute('target') === '_blank') {
    node.setAttribute('rel', 'noopener noreferrer');
  }
});

function isLocalApiUrl(value, baseURL) {
  try {
    const base = new URL(baseURL || window.location.href, window.location.href);
    const url = new URL(value, base);
    return url.origin === window.location.origin && url.pathname.startsWith('/api/');
  } catch (_) {
    return false;
  }
}

function renderHtml(el, binding) {
  el.innerHTML = sanitizeHtml(binding.value);
}

const plugin = {
  install(Vue) {
    Vue.directive('dompurify-html', { bind: renderHtml, update: renderHtml });
  },
};

// Mavon uses this same MarkdownIt instance for its editor preview and navigation.
// Sanitize before that third-party component reaches its internal HTML sinks.
function secureMarkdownRenderer(markdown, highlighter) {
  if (markdown.__safeRenderer) return markdown;
  markdown.renderer.rules.fence = (tokens, index) => {
    const token = tokens[index];
    const language = token.info.trim().split(/\s+/)[0];
    const result = language && highlighter.getLanguage(language)
      ? highlighter.highlight(token.content, { language, ignoreIllegals: true }).value
      : highlighter.highlightAuto(token.content).value;
    return '<pre><code class="hljs">' + result + '</code></pre>\n';
  };
  const render = markdown.render.bind(markdown);
  markdown.render = (source, env) => sanitizeHtml(render(source, env));
  const renderInline = markdown.renderInline.bind(markdown);
  markdown.renderInline = (source, env) => sanitizeHtml(renderInline(source, env));
  markdown.__safeRenderer = true;
  return markdown;
}

module.exports = { sanitizeHtml, safeExternalUrl, isLocalApiUrl, secureMarkdownRenderer, plugin };
