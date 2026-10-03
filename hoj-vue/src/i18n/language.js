import storage from '@/common/storage'

export const DEFAULT_LANGUAGE = 'zh-CN'
export const LANGUAGE_STORAGE_KEY = 'Web_Language'

const aliases = {
  'zh': 'zh-CN',
  'zh-cn': 'zh-CN',
  'zh-hans': 'zh-CN',
  'zh-hans-cn': 'zh-CN',
  'zh-tw': 'zh-TW',
  'zh-hant': 'zh-TW',
  'zh-hant-tw': 'zh-TW',
  'en': 'en-US',
  'en-us': 'en-US',
  'ja': 'ja-JP',
  'ja-jp': 'ja-JP',
  'ko': 'ko-KR',
  'ko-kr': 'ko-KR',
}

export function normalizeLanguage(value) {
  if (typeof value !== 'string') return null
  const key = value.trim().toLowerCase()
  return Object.prototype.hasOwnProperty.call(aliases, key) ? aliases[key] : null
}

export function getSavedLanguage() {
  try {
    return normalizeLanguage(storage.get(LANGUAGE_STORAGE_KEY))
  } catch (error) {
    return null
  }
}

export function resolveLanguage(queryLanguage) {
  // Only an explicit supported choice overrides the Chinese default.
  return normalizeLanguage(queryLanguage) || getSavedLanguage() || DEFAULT_LANGUAGE
}
