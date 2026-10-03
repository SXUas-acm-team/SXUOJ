// Rank responses prepend the viewer/concerned users to the complete ranking.
// Retain the last occurrence so the full ranking keeps its server ordering.
function chartRankRecords(rows) {
  const last = new Map();
  rows.forEach((row, index) => last.set(row.uid || row.username, index));
  return rows.filter((row, index) => last.get(row.uid || row.username) === index).slice(0, 10);
}
function normalizeDifficulty(value) {
  const legacy = { Easy: '0', Mid: '1', Hard: '2' };
  const normalized = Object.prototype.hasOwnProperty.call(legacy, value) ? legacy[value] : String(value == null ? '' : value);
  return ['0', '1', '2'].includes(normalized) ? normalized : '';
}
function normalizePageSize(value, fallback = 10) {
  const size = Number(value);
  return [10, 15, 30, 50, 100].includes(size) ? size : fallback;
}
function normalizePage(value) {
  const page = Number(value);
  return Number.isSafeInteger(page) && page > 0 && page <= 100000 ? page : 1;
}
function normalizeTagIds(value) {
  let ids;
  try {
    ids = typeof value === 'string' ? JSON.parse(value) : value;
  } catch (_) {
    return [];
  }
  if (!Array.isArray(ids)) return [];
  return [...new Set(ids.filter(id =>
    (typeof id === 'number' || (typeof id === 'string' && /^\d+$/.test(id))) &&
    Number.isSafeInteger(Number(id)) && Number(id) > 0
  ).map(Number))];
}
module.exports = { chartRankRecords, normalizeDifficulty, normalizePageSize, normalizePage, normalizeTagIds };
