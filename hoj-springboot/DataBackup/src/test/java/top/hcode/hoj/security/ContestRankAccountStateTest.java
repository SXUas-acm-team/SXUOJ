package top.hcode.hoj.security;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.dao.contest.ContestRecordEntityService;
import top.hcode.hoj.dao.judge.JudgeEntityService;
import top.hcode.hoj.dao.user.UserInfoEntityService;
import top.hcode.hoj.manager.oj.*;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.user.UserInfo;
import top.hcode.hoj.pojo.vo.*;
import top.hcode.hoj.utils.RedisUtils;
import top.hcode.hoj.validator.ContestValidator;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ContestRankAccountStateTest {
    private UserInfoEntityService users;
    private ActiveContestUserFilter filter;
    private ContestCalculateRankManager calculator;
    private ContestRecordEntityService records;
    private Set<String> active;
    private ValueOperations<String, Object> values;
    private RedisTemplate<String, Object> template;
    private Object oldRedis;
    private Contest contest;

    private static void inject(Object target, String name, Object dependency) { ReflectionTestUtils.setField(target, name, dependency); }

    @BeforeEach void setup() {
        users = mock(UserInfoEntityService.class); active = new HashSet<>(Arrays.asList("alice", "bob"));
        when(users.list(any(Wrapper.class))).thenAnswer(call -> active.stream()
                .map(id -> new UserInfo().setUuid(id).setStatus(0)).collect(Collectors.toList()));
        when(users.getSuperAdminUidList()).thenAnswer(call -> new ArrayList<String>());
        filter = new ActiveContestUserFilter(); inject(filter, "userInfoEntityService", users);
        records = mock(ContestRecordEntityService.class);
        calculator = new ContestCalculateRankManager();
        org.springframework.test.util.ReflectionTestUtils.setField(calculator, "identityManager", org.mockito.Mockito.mock(top.hcode.hoj.manager.oj.ContestIdentityManager.class)); inject(calculator, "userInfoEntityService", users);
        inject(calculator, "activeContestUserFilter", filter); inject(calculator, "contestRecordEntityService", records);
        oldRedis = ReflectionTestUtils.getField(RedisUtils.class, "redisTemplate");
        template = mock(RedisTemplate.class); values = mock(ValueOperations.class);
        when(template.opsForValue()).thenReturn(values);
        ReflectionTestUtils.setField(RedisUtils.class, "redisTemplate", template);
        inject(calculator, "redisUtils", new RedisUtils());
        contest = new Contest().setId(7L).setUid("owner").setIsGroup(false).setVisible(true).setType(0)
                .setStatus(1).setSealRank(true).setStartTime(new Date(0)).setEndTime(new Date(3600000))
                .setDuration(3600L).setStarAccount(null).setAwardType(2)
                .setAwardConfig("{\"config\":[{\"priority\":1,\"name\":\"Gold\",\"num\":1}]}");
    }

    @AfterEach void restore() { ReflectionTestUtils.setField(RedisUtils.class, "redisTemplate", oldRedis); }

    private ACMContestRankVO acm(String uid, long time) {
        HashMap<String, Object> attempt = new HashMap<>(); attempt.put("isAC", true); attempt.put("ACTime", time);
        attempt.put("isFirstAC", uid.equals("alice"));
        HashMap<String, HashMap<String, Object>> info = new HashMap<>(); info.put("A", attempt);
        return new ACMContestRankVO().setUid(uid).setUsername(uid).setAc(1).setTotal(1).setTotalTime(time).setSubmissionInfo(info);
    }

    private OIContestRankVO oi(String uid, int score) {
        return new OIContestRankVO().setUid(uid).setUsername(uid).setTotalScore(score).setTotalTime(10);
    }

    private ContestRecordVO record(String uid, long time, int score) {
        ContestRecordVO row = new ContestRecordVO(); row.setUid(uid); row.setUsername(uid); row.setDisplayId("A");
        row.setStatus(1); row.setTime(time); row.setSubmitTime(new Date(time * 1000)); row.setScore(score); row.setUseTime(10);
        return row;
    }

    @Test void acmCacheImmediatelyReflectsBanUnbanAndRecalculatesAwardsFirstAC() {
        List<ACMContestRankVO> cached = Arrays.asList(acm("alice", 10), acm("bob", 20));
        when(values.get(anyString())).thenReturn(cached);
        assertEquals(2, calculator.calcACMRank(false, false, contest, null, null, null, true, 15L, false).size());
        active.remove("alice");
        List<ACMContestRankVO> rank = calculator.calcACMRank(false, false, contest, "alice",
                Arrays.asList("alice", "bob"), null, true, 15L, false);
        assertTrue(rank.stream().allMatch(r -> r.getUid().equals("bob"))); // includes the concerned-user copy
        assertEquals(Integer.valueOf(1), rank.get(0).getRank()); assertEquals("Gold", rank.get(0).getAwardName());
        assertTrue((Boolean) rank.get(0).getSubmissionInfo().get("A").get("isFirstAC"));
        assertNull(cached.get(0).getRank()); assertFalse((Boolean) cached.get(1).getSubmissionInfo().get("A").get("isFirstAC"));
        active.add("alice");
        assertEquals(2, calculator.calcACMRank(false, false, contest, null, null, null, true, 15L, false).size());
        verifyNoInteractions(records);
    }

    @Test void oiCachedBannedLeaderCannotBePinnedAndLiveUserGetsFirstPlace() {
        when(values.get(anyString())).thenReturn(Arrays.asList(oi("alice", 100), oi("bob", 80)));
        active.remove("alice");
        List<OIContestRankVO> rank = calculator.calcOIRank(false, false, contest, "alice",
                Collections.singletonList("alice"), null, true, 15L, false);
        assertEquals(1, rank.size()); assertEquals("bob", rank.get(0).getUid());
        assertEquals(Integer.valueOf(1), rank.get(0).getRank()); assertTrue(rank.get(0).getIsWinAward());
        verifyNoInteractions(records);
    }

    @Test void mergedAcmAndExportCalculationOverloadRemoveDisabledUser() {
        active.remove("alice"); List<Integer> merged = Collections.singletonList(9);
        when(records.getACMContestRecord(anyString(), eq(7L), eq(merged), any(Date.class)))
                .thenReturn(Arrays.asList(record("alice", 10, 100), record("bob", 20, 100)));
        List<ACMContestRankVO> rank = calculator.calcACMRank(false, false, contest, null, null, merged, false);
        assertEquals(1, rank.size()); assertEquals("bob", rank.get(0).getUid());
        assertTrue((Boolean) rank.get(0).getSubmissionInfo().get("A").get("isFirstAC"));
    }

    @Test void mergedOiAndExportCalculationOverloadRemoveDisabledUser() {
        active.remove("alice"); List<Integer> merged = Collections.singletonList(9);
        when(records.getOIContestRecord(eq(contest), eq(merged), eq(false), eq(false)))
                .thenReturn(Arrays.asList(record("alice", 10, 100), record("bob", 20, 80)));
        List<OIContestRankVO> rank = calculator.calcOIRank(false, false, contest, null, null, merged, false);
        assertEquals(1, rank.size()); assertEquals("bob", rank.get(0).getUid());
    }

    @Test void paginationAndOutsideScoreboardTotalCountExcludeBannedCachedRows() {
        when(values.get(anyString())).thenReturn(Arrays.asList(acm("alice", 10), acm("bob", 20)));
        active.remove("alice"); ContestRankManager rankManager = new ContestRankManager();
        inject(rankManager, "contestCalculateRankManager", calculator);
        IPage<ACMContestRankVO> page = rankManager.getACMContestScoreboard(false, false, contest, null,
                null, null, 1, 1, null, true, 15L, false);
        assertEquals(1L, page.getTotal()); assertEquals("bob", page.getRecords().get(0).getUid());
    }

    @Test void scrollBoardSubmissionFeedChecksLiveAccountState() throws Exception {
        active.remove("alice"); ContestEntityService contests = mock(ContestEntityService.class);
        when(contests.getById(7L)).thenReturn(contest);
        JudgeEntityService judges = mock(JudgeEntityService.class);
        ContestScrollBoardSubmissionVO blocked = new ContestScrollBoardSubmissionVO(); blocked.setUid("alice"); blocked.setSubmitTime(new Date(100));
        ContestScrollBoardSubmissionVO enabled = new ContestScrollBoardSubmissionVO(); enabled.setUid("bob"); enabled.setSubmitTime(new Date(200));
        when(judges.getContestScrollBoardSubmission(eq(7L), anyList())).thenReturn(Arrays.asList(blocked, enabled));
        ContestScrollBoardManager scroll = new ContestScrollBoardManager();
        org.springframework.test.util.ReflectionTestUtils.setField(scroll, "identityManager", org.mockito.Mockito.mock(top.hcode.hoj.manager.oj.ContestIdentityManager.class)); inject(scroll, "contestEntityService", contests);
        inject(scroll, "contestValidator", mock(ContestValidator.class)); inject(scroll, "judgeEntityService", judges);
        inject(scroll, "contestCalculateRankManager", calculator); inject(scroll, "activeContestUserFilter", filter);
        List<ContestScrollBoardSubmissionVO> feed = scroll.getContestScrollBoardSubmission(7L, false);
        assertEquals(1, feed.size()); assertEquals("bob", feed.get(0).getUid());
    }

    @Test void unknownDisabledAndMissingUsersNeverEnterRankAndNullStatusIsRejectedInSql() {
        when(users.list(any(Wrapper.class))).thenReturn(Arrays.asList(new UserInfo().setUuid("alice").setStatus(-1),
                new UserInfo().setUuid("bob").setStatus(2), new UserInfo().setUuid("extra").setStatus(0)));
        assertTrue(filter.getActiveUids(Arrays.asList("alice", "bob", "missing")).isEmpty());
        ArgumentCaptor<Wrapper> query = ArgumentCaptor.forClass(Wrapper.class); verify(users).list(query.capture());
        assertTrue(query.getValue().getSqlSegment().contains("status ="));
        assertTrue(((QueryWrapper) query.getValue()).getParamNameValuePairs().containsValue(0));
    }

    @Test void statusLookupFailureNeverFallsBackToCachedDisabledUser() {
        when(values.get(anyString())).thenReturn(Collections.singletonList(acm("alice", 10)));
        when(users.list(any(Wrapper.class))).thenThrow(new DataAccessResourceFailureException("offline"));
        assertThrows(DataAccessResourceFailureException.class, () -> calculator.calcACMRank(false, false,
                contest, null, null, null, true, 15L, false));
    }

    @Test void largeBoardsUseBoundedLiveQueriesAndEmptyBoardNeedsNoStatusLookup() {
        assertTrue(filter.getActiveUids(Collections.emptyList()).isEmpty()); verifyNoInteractions(users);
        List<String> ids = new ArrayList<>(); for (int i = 0; i < 1201; i++) ids.add("u" + i);
        filter.getActiveUids(ids); verify(users, times(3)).list(any(Wrapper.class));
    }
}
