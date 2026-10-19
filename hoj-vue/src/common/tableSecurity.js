const hardenedTables = new WeakSet();
const forbiddenKeys = new Set(['__proto__', 'constructor', 'prototype']);

// Guard VXE's legacy recursive merge before any install/setup option reaches it.
function assertSafeTableOptions(value, ancestors = new Set(), depth = 0) {
  if (value == null || typeof value !== 'object') return;
  if (depth > 100 || ancestors.has(value)) {
    throw new TypeError('Table options must be an acyclic object with bounded depth.');
  }
  ancestors.add(value);
  for (const key of Object.keys(value)) {
    if (forbiddenKeys.has(key)) {
      throw new TypeError('Unsafe table option key.');
    }
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor.get || descriptor.set) {
      throw new TypeError('Table options cannot contain accessor properties.');
    }
    assertSafeTableOptions(descriptor.value, ancestors, depth + 1);
  }
  ancestors.delete(value);
}

function hardenVxeTable(table) {
  if (hardenedTables.has(table)) return table;
  const setup = table.setup;
  const install = table.install;
  table.setup = function guardedSetup(options) {
    assertSafeTableOptions(options);
    return setup.apply(this, arguments);
  };
  table.install = function guardedInstall(Vue, options) {
    assertSafeTableOptions(options);
    return install.apply(this, arguments);
  };
  hardenedTables.add(table);
  return table;
}

module.exports = { assertSafeTableOptions, hardenVxeTable };
