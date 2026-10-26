const VERSION_PATTERN = /^[a-f0-9]{32}$/;

function isEditingPage(route = {}) {
  return /\/(?:create|edit)(?:\/|$)/.test(route.path || '') ||
    /^(?:Contest(?:Full)?ProblemDetails|ProblemDetails)$/.test(route.name || '');
}

// HTML and this small manifest are revalidated; content-hashed assets remain immutable.
// A running page needs an explicit version check to notice a deployment.
function startFrontendUpdates({ version, router, onDeferred = () => {}, win = window, doc = document }) {
  if (!VERSION_PATTERN.test(version || '') || !win.fetch) return () => {};
  let pending = null;
  let dirty = false;
  let notified = false;
  let checking = false;
  let stopped = false;
  let reloaded = false;
  let lastCheck = -Infinity;
  let controller;
  const editable = element => element && element.closest && element.closest('input, textarea, select, [contenteditable]:not([contenteditable="false"])');
  const reloadIfSafe = () => {
    if (!pending || stopped || reloaded || doc.visibilityState === 'hidden') return;
    const busy = dirty || isEditingPage(router.currentRoute) || editable(doc.activeElement) ||
      doc.querySelector('.CodeMirror, .el-upload-list__item.is-uploading');
    if (busy) {
      if (!notified) { notified = true; onDeferred(); }
      return;
    }
    // A stale proxy or a rolling deployment must not create an immediate reload loop.
    try {
      const previous = JSON.parse(win.sessionStorage.getItem('sxuoj.frontend-update') || 'null');
      if (previous && Date.now() - previous.at < 60000) return;
      win.sessionStorage.setItem('sxuoj.frontend-update', JSON.stringify({ version: pending, at: Date.now() }));
    } catch (_) { /* Disabled storage must not stop updates. */ }
    reloaded = true;
    win.location.reload();
  };
  const check = async () => {
    reloadIfSafe();
    if (stopped || reloaded || checking || doc.visibilityState === 'hidden' || Date.now() - lastCheck < 15000) return;
    checking = true;
    lastCheck = Date.now();
    controller = win.AbortController ? new win.AbortController() : null;
    const timeout = controller ? win.setTimeout(() => controller.abort(), 8000) : null;
    try {
      const response = await win.fetch('/version.json?_=' + Date.now(), {
        cache: 'no-store', credentials: 'same-origin', ...(controller ? { signal: controller.signal } : {}),
      });
      if (!response.ok) return;
      const data = await response.json();
      if (stopped || !data || !VERSION_PATTERN.test(data.version || '')) return;
      pending = data.version === version ? null : data.version;
      reloadIfSafe();
    } catch (_) { /* Missing manifests, offline tabs and transient deploy failures are harmless. */ }
    finally { win.clearTimeout(timeout); checking = false; }
  };
  const markDirty = event => { if (editable(event.target)) dirty = true; };
  const removeRouteHook = router.afterEach((to, from) => {
    if (to.path !== from.path) dirty = false;
    // Wait for the previous editor to unmount before deciding whether to reload.
    win.setTimeout(check, 0);
  });
  const removeErrorHook = router.onError(error => {
    if (/ChunkLoadError|Loading (?:CSS )?chunk .*failed/i.test(String(error && (error.message || error)))) {
      lastCheck = -Infinity;
      check();
    }
  });
  doc.addEventListener('input', markDirty, true);
  doc.addEventListener('change', markDirty, true);
  doc.addEventListener('visibilitychange', check);
  win.addEventListener('focus', check);
  win.addEventListener('pageshow', check);
  const timer = win.setInterval(check, 60000);
  check();
  return () => {
    stopped = true;
    if (controller) controller.abort();
    win.clearInterval(timer);
    doc.removeEventListener('input', markDirty, true);
    doc.removeEventListener('change', markDirty, true);
    doc.removeEventListener('visibilitychange', check);
    win.removeEventListener('focus', check);
    win.removeEventListener('pageshow', check);
    if (typeof removeRouteHook === 'function') removeRouteHook();
    if (typeof removeErrorHook === 'function') removeErrorHook();
  };
}

module.exports = { startFrontendUpdates, isEditingPage };
