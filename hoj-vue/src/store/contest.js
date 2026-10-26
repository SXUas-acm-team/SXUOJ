import moment from 'moment'
import api from '@/common/api'
import { CONTEST_STATUS, CONTEST_TYPE } from '@/common/constants'
import time from '@/common/time'
const state = {
  requestEpoch: 0,
  requestVersions: { info: 0, problems: 0, access: 0, groupAuth: 0 },
  now: moment(),
  intoAccess: false, // 比赛进入权限
  submitAccess:false, // 保护比赛的提交权限
  access: null,
  forceUpdate: false, // 强制实时榜单
  removeStar: false, // 榜单去除打星队伍
  concernedList:[], // 关注队伍
  isContainsAfterContestJudge: false, // 是否包含比赛结束后的提交
  contest: {
    auth: CONTEST_TYPE.PUBLIC,
    openPrint: false,
    rankShowName:'username',
    allowEndSubmit: false,
  },
  contestProblems: [],
  itemVisible: {
    table: true,
    chart: true,
  },
  disPlayIdMapColor:{}, // 展示id对应的气球颜色
  groupContestAuth: 0,
}

const getters = {
  contestStatus: (state, getters) => {
    return state.contest.status;
  },
  contestRuleType: (state,getters) => {
    return state.contest.type;
  },
  isContestAdmin: (state, getters, _, rootGetters) => {
    return rootGetters.isAuthenticated &&
      (state.contest.author === rootGetters.userInfo.username || rootGetters.isSuperAdmin || rootGetters.isContentAdmin || state.groupContestAuth == 5 ||
        !!(state.access && state.access.accessReason === 'ADMIN'))
  },
  isContainsAfterContestJudge: (state, getters) => {
    return state.isContainsAfterContestJudge;
  },
  isContestReleased: state => state.contest.released === true,
  isContestSealed: (state, getters) => state.contest.confidential === true && !getters.isContestReleased &&
    (getters.contestStatus === CONTEST_STATUS.ENDED ||
      (state.contest.endTime && !state.now.isBefore(moment(state.contest.endTime)))),
  canViewContest: (state, getters) => {
    if (state.contest.id == null) return false
    if (getters.isContestAdmin || getters.isContestReleased) return true
    if (getters.isContestSealed || getters.contestStatus === CONTEST_STATUS.SCHEDULED) return false
    if (state.contest.confidential || state.contest.identityVerificationEnabled) return !!(state.access && state.access.canViewProblems)
    return state.contest.auth !== CONTEST_TYPE.PRIVATE || state.intoAccess
  },
  canViewContestDescription: (state, getters) => {
    if (state.contest.id == null) return false
    return state.contest.confidential ? getters.canViewContest : true
  },
  contestAccessReason: (state, getters) => getters.isContestSealed ? 'SEALED' :
    (state.access && state.access.accessReason) || (getters.contestStatus === CONTEST_STATUS.SCHEDULED ? 'SCHEDULED' : 'PASSWORD_REQUIRED'),
  canSubmit:(state, getters, _, rootGetters)=>{
     if (getters.isContestAdmin) return true
     if (getters.isContestReleased) return rootGetters.isAuthenticated
     if (getters.isContestSealed) return false
     if (state.contest.confidential || state.contest.identityVerificationEnabled) return !!(state.access && state.access.canSubmit) && rootGetters.isAuthenticated
     return state.intoAccess||state.submitAccess || state.contest.auth === CONTEST_TYPE.PUBLIC ||getters.isContestAdmin
  },
  contestMenuDisabled: (state, getters) => {
    return !getters.canViewContest
  },

  // 榜单是否实时刷新
  ContestRealTimePermission: (state, getters, _, rootGetters) => {
    // 比赛若是已结束，便是最后榜单
    if (getters.contestStatus === CONTEST_STATUS.ENDED) {
      return true
    }
    // 比赛管理员直接可看到实时榜单
    if(getters.isContestAdmin){
      return true
    }
    // 比赛是否开启
    if(state.contest.sealRank === true){
      // 当前时间在封榜时间之后，即不刷新榜单
      return !state.now.isAfter(moment(state.contest.sealRankTime))
    }else{
      return true
    }
  },
  problemSubmitDisabled: (state, getters, _, rootGetters) => {
    if (!rootGetters.isAuthenticated) return true
    if (state.contest.confidential || state.contest.identityVerificationEnabled) return !getters.canSubmit
    // 比赛结束不可交题, 除非开启了允许
    if (getters.contestStatus === CONTEST_STATUS.ENDED) {
      return !state.contest.allowEndSubmit
    } else if (getters.contestStatus === CONTEST_STATUS.SCHEDULED) {
      // 比赛未开始不可交题，除非是比赛管理者
      return !getters.isContestAdmin
    }
    // 未登录不可交题
    return !rootGetters.isAuthenticated
  },
  // 是否需要显示密码验证框
  passwordFormVisible: (state, getters) => {
    // 如果是公开赛，保护赛，或已注册过，管理员都不用再显示
    return !getters.isContestReleased && !getters.isContestSealed &&
      state.contest.auth !== CONTEST_TYPE.PUBLIC &&state.contest.auth !== CONTEST_TYPE.PROTECTED &&!state.intoAccess && !getters.isContestAdmin
  },
  contestStartTime: (state) => {
    return moment(state.contest.startTime)
  },
  contestEndTime: (state) => {
    return moment(state.contest.endTime)
  },
  // 比赛计时文本显示
  countdown: (state, getters) => {
    // 还未开始的显示
    if (getters.contestStatus === CONTEST_STATUS.SCHEDULED) {

      let durationMs = getters.contestStartTime.diff(state.now, 'seconds')

      let duration = moment.duration(durationMs, 'seconds')
      // time is too long
      if (duration.weeks() > 0) {
        return 'Start At ' + duration.humanize()
      }

      if(duration.asSeconds()<=0){
        state.contest.status = CONTEST_STATUS.RUNNING
      }

      let texts = time.secondFormat(durationMs)
      return '-' + texts
      // 比赛进行中的显示
    } else if (getters.contestStatus === CONTEST_STATUS.RUNNING) {
      // 倒计时文本显示
      if(getters.contestEndTime.diff(state.now, 'seconds')>0){
        let texts = time.secondFormat(getters.contestEndTime.diff(state.now, 'seconds'))
        return '-' + texts
      }else{
        state.contest.status = CONTEST_STATUS.ENDED
        return "00:00:00"
      }
      
    } else {
      return 'Ended'
    }
  },
  // 比赛开始到现在经过的秒数
  BeginToNowDuration:(state,getters)=>{
    return moment.duration(state.now.diff(getters.contestStartTime, 'seconds'), 'seconds').asSeconds()
  },

  // 比赛进度条显示
  progressValue:(state,getters)=>{
      // 还未开始的显示
    if (getters.contestStatus === CONTEST_STATUS.SCHEDULED) {
      return 0;
      // 比赛进行中的显示
    } else if (getters.contestStatus === CONTEST_STATUS.RUNNING) {
      // 获取比赛开始到现在经过的秒数
      let duration = getters.BeginToNowDuration
      // 消耗时间除以整体时间
      return (duration / state.contest.duration)*100
    }else{
      return 100;
    }
  },

  isShowContestSetting:(state, getters)=>{
    return getters.contestStatus === CONTEST_STATUS.ENDED && state.contest.allowEndSubmit
  }
}

const mutations = {
  beginContestRequest(state, payload) {
    state.requestVersions[payload.type] += 1
  },
  changeContest (state, payload) {
    state.contest = payload.contest
  },
  changeContestItemVisible(state, payload) {
    state.itemVisible = {...state.itemVisible, ...payload}
  },
  changeRankForceUpdate (state, payload) {
    state.forceUpdate = payload.value
  },
  changeRankRemoveStar(state, payload){
    state.removeStar = payload.value
  },
  changeContainsAfterContestJudge(state, payload){
    state.isContainsAfterContestJudge = payload.value
  },
  changeConcernedList(state, payload){
    state.concernedList = payload.value
  },
  changeContestProblems(state, payload) {
    state.contestProblems = payload.contestProblems;
    let tmp={};
    for(var j = 0,len = payload.contestProblems.length; j < len; j++){
      tmp[payload.contestProblems[j].displayId] = payload.contestProblems[j].color;
    }
    state.disPlayIdMapColor = tmp;
  },
  changeContestRankLimit(state, payload) {
    state.rankLimit = payload.rankLimit
  },
  contestIntoAccess(state, payload) {
    state.intoAccess = payload.intoAccess
  },
  changeGroupContestAuth(state, payload) {
    state.groupContestAuth = payload.groupContestAuth
  },
  contestSubmitAccess(state, payload) {
    state.submitAccess = payload.submitAccess
  },
  changeContestAccess(state, payload) {
    state.access = payload
    if (payload.released != null) state.contest = { ...state.contest, released: payload.released }
    if (payload.canViewProblems === false) {
      state.contestProblems = []
      state.disPlayIdMapColor = {}
      state.requestVersions.problems += 1
    }
  },
  denyContestAccess(state) {
    if (state.contest.confidential) state.contest = { ...state.contest, released: false }
    state.access = { canViewProblems: false, canSubmit: false, accessReason: 'SEALED' }
    state.contestProblems = []
    state.disPlayIdMapColor = {}
    state.requestVersions.problems += 1
  },
  resetContestAuthentication(state) {
    state.intoAccess = false
    state.submitAccess = false
    state.access = null
    state.requestVersions.access += 1
    state.requestVersions.problems += 1
    if (!state.contest.released) {
      state.contestProblems = []
      state.disPlayIdMapColor = {}
    }
  },
  clearContest (state) {
    state.requestEpoch += 1
    state.contest = {}
    state.contestProblems = []
    state.intoAccess = false
    state.submitAccess = false
    state.access = null
    state.itemVisible = {
      table: true,
      chart: true,
      realName: false
    }
    state.forceUpdate = false
    state.removeStar = false
    state.groupContestAuth = 0
    state.isContainsAfterContestJudge = false
    state.concernedList = []
    state.disPlayIdMapColor = {}
  },
  now(state, payload) {
    state.now = payload.now
  },
  nowAdd1s (state) {
    state.now = moment(state.now.add(1, 's'))
  },
}

// The different request types may run together. A navigation, cleared store,
// account change, or newer request of the same type invalidates old responses.
function contestRequest(context, type) {
  const { commit, state, rootState } = context
  commit('beginContestRequest', { type })
  const cid = rootState.route.params.contestID
  const epoch = state.requestEpoch
  const version = state.requestVersions[type]
  const session = rootState.user && rootState.user.sessionVersion
  return {
    cid,
    current: () => String(rootState.route.params.contestID) === String(cid) &&
      state.requestEpoch === epoch && state.requestVersions[type] === version &&
      (!rootState.user || rootState.user.sessionVersion === session),
  }
}

const actions = {
  getContest (context) {
    const { commit, dispatch } = context
    const request = contestRequest(context, 'info')
    commit('contestIntoAccess', { intoAccess: false })
    commit('contestSubmitAccess', { submitAccess: false })
    commit('changeContestAccess', {})
    commit('changeGroupContestAuth', { groupContestAuth: 0 })
    return api.getContest(request.cid).then(res => {
      if (!request.current()) return res
      const contest = res.data.data
      commit('changeContest', { contest })
      commit('now', { now: moment(contest.now) })
      if (contest.gid && context.rootGetters.isAuthenticated) {
        dispatch('getGroupContestAuth', { gid: contest.gid }).catch(() => {})
      }
      if (contest.confidential || contest.identityVerificationEnabled || contest.auth === CONTEST_TYPE.PRIVATE || contest.auth === CONTEST_TYPE.PROTECTED) {
        const access = dispatch('getContestAccess', { auth: contest.auth })
        if (contest.confidential || contest.identityVerificationEnabled) return access.then(() => res).catch(() => res)
        access.catch(() => {})
      }
      return res
    })
  },
  getScoreBoardContestInfo (context) {
    const request = contestRequest(context, 'info')
    return api.getScoreBoardContestInfo(request.cid).then(res => {
      if (request.current()) {
        const contest = res.data.data.contest
        context.commit('changeContest', { contest })
        context.commit('now', { now: moment(contest.now) })
        if (Array.isArray(res.data.data.problemList)) {
          context.commit('changeContestProblems', { contestProblems: res.data.data.problemList })
        }
        if (contest.confidential || contest.identityVerificationEnabled) return context.dispatch('getContestAccess').then(() => res)
      }
      return res
    })
  },
  getContestProblems (context) {
    const request = contestRequest(context, 'problems')
    const containsEnd = context.state.isContainsAfterContestJudge
    const current = () => request.current() && context.state.isContainsAfterContestJudge === containsEnd
    return api.getContestProblemList(request.cid, containsEnd).then(res => {
      if (current()) context.commit('changeContestProblems', { contestProblems: res.data.data })
      return res
    }, err => {
      if (current()) context.commit('changeContestProblems', { contestProblems: [] })
      throw err
    })
  },
  getContestAccess (context, contestType = {}) {
    const request = contestRequest(context, 'access')
    return api.getContestAccess(request.cid).then(res => {
      if (request.current()) {
        const data = res.data.data
        context.commit('changeContestAccess', data)
        const privateContest = (contestType.auth == null ? context.state.contest.auth : contestType.auth) === CONTEST_TYPE.PRIVATE
        context.commit(privateContest ? 'contestIntoAccess' : 'contestSubmitAccess',
          privateContest ? { intoAccess: res.data.data.access } : { submitAccess: res.data.data.access })
      }
      return res
    }, err => {
      if (request.current() && (context.state.contest.confidential || context.state.contest.identityVerificationEnabled)) context.commit('denyContestAccess')
      throw err
    })
  },
  refreshContestAccess (context) {
    const request = contestRequest(context, 'info')
    return api.getContest(request.cid).then(res => {
      if (!request.current()) return res
      context.commit('changeContest', { contest: res.data.data })
      context.commit('now', { now: moment(res.data.data.now) })
      return context.dispatch('getContestAccess').then(() => res)
    }).catch(err => {
      if (request.current() && (context.state.contest.confidential || context.state.contest.identityVerificationEnabled)) context.commit('denyContestAccess')
      throw err
    })
  },
  getGroupContestAuth (context, gid) {
    const request = contestRequest(context, 'groupAuth')
    return api.getGroupAuth(gid.gid).then(res => {
      if (request.current()) context.commit('changeGroupContestAuth', { groupContestAuth: res.data.data })
      return res
    })
  },
}

export default {
  state,
  mutations,
  getters,
  actions
}
