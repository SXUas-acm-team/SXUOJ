const localStorage = window.localStorage

export default {
  name: 'storage',

  /**
   * save value(Object) to key
   * @param {string} key 键
   * @param {Object} value 值
   */
  set (key, value) {
    localStorage.setItem(key, JSON.stringify(value))
  },

  /**
   * get value(Object) by key
   * @param {string} key 键
   * @return {Object}
   */
  get (key) {
    try {
      return JSON.parse(localStorage.getItem(key))
    } catch (error) {
      // A stale or damaged preference must not prevent the application from starting.
      return null
    }
  },

  /**
   * remove key from localStorage
   * @param {string} key 键
   */
  remove (key) {
    localStorage.removeItem(key)
  },
  /**
   * clear all
   */
  clear () {
    localStorage.clear()
  },
}
