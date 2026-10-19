function isSafeExtraFileName(name) {
  return typeof name === 'string' && name.length > 0 &&
    !['__proto__', 'constructor', 'prototype'].includes(name) &&
    !/[\u0000-\u001f\u007f]/.test(name);
}

function parseExtraFiles(value) {
  if (value == null || value === '') return {};
  const files = typeof value === 'string' ? JSON.parse(value) : value;
  // Legacy empty JSON null is equivalent to an absent configuration.
  if (files == null) return {};
  if (typeof files !== 'object' || Array.isArray(files) ||
      (Object.getPrototypeOf(files) !== Object.prototype && Object.getPrototypeOf(files) !== null)) {
    throw new Error('Invalid extra file configuration');
  }
  const result = {};
  for (const [name, content] of Object.entries(files)) {
    if (!isSafeExtraFileName(name) || typeof content !== 'string') {
      throw new Error('Invalid extra file configuration');
    }
    result[name] = content;
  }
  return result;
}

module.exports = { isSafeExtraFileName, parseExtraFiles };
