// The server decides access. The local clock only hides content at a known boundary.
module.exports = {
  data() {
    return { contestAccessTimer: null, contestAccessPending: false, contestAccessTicks: 0 }
  },
  computed: {
    contestSessionVersion() { return this.$store.getters.sessionVersion },
  },
  mounted() {
    if (!this.ownsContestAccessPolling()) return
    window.addEventListener('focus', this.refreshContestPermission)
    this.contestAccessTimer = setInterval(() => {
      const contest = this.$store.state.contest.contest
      const restricted = contest.confidential || contest.identityVerificationEnabled
      const wasSealed = this.$store.getters.isContestSealed
      if (contest.confidential || (restricted && this.$options.name !== 'ContestDetails')) this.$store.commit('nowAdd1s')
      this.contestAccessTicks += 1
      if (wasSealed !== this.$store.getters.isContestSealed || this.contestAccessTicks % (restricted ? 15 : 60) === 0) {
        this.refreshContestPermission()
      }
    }, 1000)
  },
  methods: {
    ownsContestAccessPolling() {
      return this.$options.name === 'ContestDetails' ||
        ['ContestFullProblemDetails', 'ACMScoreBoard', 'OIScoreBoard'].includes(this.$route.name)
    },
    refreshContestPermission() {
      if (!this.$route.params.contestID || this.contestAccessPending) return
      this.contestAccessPending = true
      return this.$store.dispatch('refreshContestAccess').catch(() => {}).finally(() => {
        this.contestAccessPending = false
      })
    },
  },
  watch: {
    contestSessionVersion() {
      if (this.ownsContestAccessPolling()) {
        this.$store.commit('resetContestAuthentication')
        this.refreshContestPermission()
      }
    },
  },
  beforeDestroy() {
    clearInterval(this.contestAccessTimer)
    window.removeEventListener('focus', this.refreshContestPermission)
  },
}
