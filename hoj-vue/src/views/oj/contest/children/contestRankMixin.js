import api from '@/common/api'
import { mapGetters, mapState } from 'vuex'
import { CONTEST_STATUS, buildContestRankConcernedKey } from '@/common/constants'
import storage from '@/common/storage';
export default {
  data() {
    return { rankRequestVersion: 0, rankViewDestroyed: false, rankRefreshPending: null, refreshFunc: null }
  },
  methods: {
    initConcernedList(){
      let key = buildContestRankConcernedKey(this.$route.params.contestID);
      const saved = storage.get(key);
      this.concernedList = Array.isArray(saved) ? saved : [];
    },
    getContestRankData (page = 1, refresh = false) {
      if (this.rankViewDestroyed || (refresh && this.rankRefreshPending)) return Promise.resolve()
      const version = ++this.rankRequestVersion
      const cid = this.$route.params.contestID
      const current = () => !this.rankViewDestroyed && version === this.rankRequestVersion &&
        String(this.$route.params.contestID) === String(cid)
      if (refresh) this.rankRefreshPending = version
      if (this.showChart && !refresh && this.$refs.chart) {
        this.$refs.chart.showLoading({maskColor: 'rgba(250, 250, 250, 0.8)'})
      }
      let data = {
        currentPage:page,
        limit: this.limit,
        cid,
        forceRefresh: this.forceUpdate ? true: false,
        removeStar: !this.showStarUser,
        concernedList:this.concernedList,
        keyword: this.keyword == null? null: this.keyword.trim(),
        containsEnd: this.isContainsAfterContestJudge,
      }
      return api.getContestRank(data).then(res => {
        if (!current()) return
        if (this.$refs && this.$refs.chart) {
          this.$refs.chart.hideLoading()
        }
        this.total = res.data.data.total
        if (page === 1) {
          this.applyToChart(res.data.data.records)
        }
        this.applyToTable(res.data.data.records)
      }).catch(() => {
        if (current() && this.$refs && this.$refs.chart) this.$refs.chart.hideLoading()
      }).finally(() => {
        if (refresh && this.rankRefreshPending === version) this.rankRefreshPending = null
      })
    },
    handleAutoRefresh (status) {
      clearInterval(this.refreshFunc)
      this.refreshFunc = null
      if (status == true) {
        this.refreshFunc = setInterval(() => {
          this.$store.dispatch('getContestProblems').catch(() => {});
          this.getContestRankData(this.page, true);
        }, 10000)
      } else {
        clearInterval(this.refreshFunc)
      }
    },
    updateConcernedList(uid, isConcerned){
      if(isConcerned){
        this.concernedList.push(uid);
      }else{
        var index = this.concernedList.indexOf(uid);
        if (index > -1) {
        this.concernedList.splice(index, 1);
        }
      }
      let key = buildContestRankConcernedKey(this.contestID);
      storage.set(key, this.concernedList);
      this.getContestRankData(this.page, true);
    },
    getRankShowName(rankShowName, username){
      let finalShowName = rankShowName;
      if(rankShowName == null || rankShowName == '' || rankShowName.trim().length == 0){
        finalShowName = username;
      }
      return finalShowName;
    }
  },
  computed: {
    ...mapGetters(['isContestAdmin','userInfo', "isContainsAfterContestJudge"]),
    ...mapState({
      'contest': state => state.contest.contest,
      'contestProblems': state => state.contest.contestProblems
    }),
    showChart: {
      get () {
        return this.$store.state.contest.itemVisible.chart
      },
      set (value) {
        this.$store.commit('changeContestItemVisible', {chart: value})
      }
    },
    showStarUser:{
      get () {
        return !this.$store.state.contest.removeStar
      },
      set (value) {
        this.$store.commit('changeRankRemoveStar', {value: !value})
      }
    },
    showTable: {
      get () {
        return this.$store.state.contest.itemVisible.table
      },
      set (value) {
        this.$store.commit('changeContestItemVisible', {table: value})
      }
    },
    forceUpdate: {
      get () {
        return this.$store.state.contest.forceUpdate
      },
      set (value) {
        this.$store.commit('changeRankForceUpdate', {value: value})
      }
    },
    concernedList:{
      get () {
        return this.$store.state.contest.concernedList
      },
      set (value) {
        this.$store.commit('changeConcernedList', {value: value})
      }
    },
    refreshDisabled () {
      return this.contest.status == CONTEST_STATUS.ENDED
    }
  },
  watch: {
    '$route.params.contestID'(cid, previous) {
      if (String(cid) === String(previous)) return
      this.rankRequestVersion += 1
      this.rankRefreshPending = null
      this.contestID = cid
      this.page = 1
      this.initConcernedList()
      if (cid != null) this.getContestRankData(1)
    },
  },
  beforeDestroy () {
    this.rankViewDestroyed = true
    this.rankRequestVersion += 1
    clearInterval(this.refreshFunc)
  }
}
