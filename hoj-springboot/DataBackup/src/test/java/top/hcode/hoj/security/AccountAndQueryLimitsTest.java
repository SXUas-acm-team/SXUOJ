package top.hcode.hoj.security;

import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.*;
import org.springframework.data.redis.core.*;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.dao.user.*;
import top.hcode.hoj.manager.email.EmailManager;
import top.hcode.hoj.manager.oj.AccountManager;
import top.hcode.hoj.pojo.vo.*;
import top.hcode.hoj.pojo.entity.discussion.*;
import top.hcode.hoj.pojo.dto.TestJudgeDTO;
import top.hcode.hoj.validator.JudgeValidator;
import top.hcode.hoj.validator.AccessValidator;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.utils.*;
import top.hcode.hoj.validator.CommonValidator;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class AccountAndQueryLimitsTest {
    private AccountManager manager;
    private EmailManager email;
    private ValueOperations<String, Object> values;
    private RedisTemplate<String, Object> template;
    private AtomicLong sent;
    @BeforeEach @SuppressWarnings("unchecked") void setup() {
        AccountProfile user = new AccountProfile(); user.setUid("member"); user.setUsername("member");
        Subject subject = mock(Subject.class); when(subject.getPrincipal()).thenReturn(user); ThreadContext.bind(subject);
        manager = new AccountManager(); email = mock(EmailManager.class);
        ReflectionTestUtils.setField(manager, "emailManager", email);
        ReflectionTestUtils.setField(manager, "userInfoEntityService", mock(UserInfoEntityService.class));
        ReflectionTestUtils.setField(manager, "commonValidator", new CommonValidator());
        template = mock(RedisTemplate.class); values = mock(ValueOperations.class); when(template.opsForValue()).thenReturn(values);
        Set<String> locks = new HashSet<>();
        when(values.setIfAbsent(anyString(), any(), anyLong(), any(TimeUnit.class))).thenAnswer(call -> locks.add(call.getArgument(0)));
        sent = new AtomicLong(); when(values.increment(anyString(), anyLong())).thenAnswer(call -> sent.incrementAndGet());
        when(template.expire(anyString(), anyLong(), any(TimeUnit.class))).thenReturn(true);
        RedisUtils redis = new RedisUtils(); redis.setRedisTemplate(template); ReflectionTestUtils.setField(manager, "redisUtils", redis);
    }
    @AfterEach void clear() { ThreadContext.unbindSubject(); }
    @Test void changingRecipientCannotBypassPerUserEmailCooldown() throws Exception {
        manager.getChangeEmailCode("first@example.com");
        assertThrows(StatusFailException.class, () -> manager.getChangeEmailCode("second@example.com"));
        verify(email, times(1)).sendChangeEmailCode(anyString(), eq("member"), anyString());
        verify(values, times(2)).setIfAbsent(eq("email:change:user:member"), anyString(), eq(60L), eq(TimeUnit.SECONDS));
        verify(template).expire(startsWith("email:change:budget:member:"), eq(86400L), eq(TimeUnit.SECONDS));
        verify(template, never()).delete(anyCollection());
    }
    @Test void dailyEmailBudgetRejectsTheEleventhRequest() {
        sent.set(10);
        assertThrows(StatusFailException.class, () -> manager.getChangeEmailCode("first@example.com"));
        verifyNoInteractions(email);
    }
    @Test void invalidEmailIsRejectedBeforeMailAndRedisWork() {
        assertThrows(StatusFailException.class, () -> manager.getChangeEmailCode("first@example.com\r\nBcc: attacker@example.com"));
        verifyNoInteractions(email, values);
    }
    @Test void javascriptAndCredentialUrlsAreRejectedServerSide() {
        UserInfoVO info = new UserInfoVO(); info.setBlog("javascript:alert(1)");
        assertThrows(StatusFailException.class, () -> manager.changeUserInfo(info));
        assertFalse(RequestLimits.isSafeProfileUrl("https://admin:password@example.com/"));
        assertTrue(RequestLimits.isSafeProfileUrl("https://example.com/"));
    }
    @Test void heatmapUsesDailyCountsInsteadOfLoadingSubmissions() throws Exception {
        UserRecordEntityService records = mock(UserRecordEntityService.class); ReflectionTestUtils.setField(manager, "userRecordEntityService", records);
        Map<String,Object> day = new HashMap<>(); day.put("date", "2026-10-01"); day.put("count", 100000L);
        when(records.getLastYearUserJudgeCounts("member", null)).thenReturn(Collections.singletonList(day));
        UserCalendarHeatmapVO result = manager.getUserCalendarHeatmap("member", null);
        assertEquals(1, result.getDataList().size()); assertEquals(100000L, result.getDataList().get(0).get("count"));
        verify(records, never()).getLastYearUserJudgeList(any(), any());
    }
    @Test void requestCollectionsAndPageSizesAreBounded() {
        assertEquals(100, RequestLimits.pageSize(Integer.MAX_VALUE, 10));
        assertEquals(1000000, RequestLimits.pageNumber(Integer.MAX_VALUE));
        assertEquals(2, RequestLimits.boundedDistinct(Arrays.asList(1,1,2,3), 3).size());
        assertEquals("alice", RequestLimits.normalizeAccount("\u00a0alice\u3000"));
    }
    @Test void rateLimitUsesOnlyAtomicSetWithExpiry() {
        RedisUtils redis = new RedisUtils(); redis.setRedisTemplate(template);
        assertTrue(redis.isWithinRateLimit("request", 5)); assertFalse(redis.isWithinRateLimit("request", 5));
        verify(values, times(2)).setIfAbsent(eq("request"), eq("1"), eq(5L), eq(TimeUnit.SECONDS));
        verify(values, never()).get(anyString()); verify(values, never()).set(anyString(), any());
    }
    @Test void debugExpectedOutputIsBoundedBeforeDispatch() {
        JudgeValidator validator = new JudgeValidator(); ReflectionTestUtils.setField(validator, "accessValidator", mock(AccessValidator.class));
        TestJudgeDTO request = new TestJudgeDTO().setType("public").setPid(1L).setLanguage("Python3")
                .setCode("print(1)").setUserInput("1").setExpectedOutput(String.join("", Collections.nCopies(1001, "x")));
        assertThrows(StatusFailException.class, () -> validator.validateTestJudgeInfo(request));
    }
    @Test void publicProfileDoesNotLoadAnotherUsersLoginHistory() throws Exception {
        UserRecordEntityService records = mock(UserRecordEntityService.class); UserAcproblemEntityService solved = mock(UserAcproblemEntityService.class);
        SessionEntityService sessions = mock(SessionEntityService.class);
        ReflectionTestUtils.setField(manager, "userRecordEntityService", records); ReflectionTestUtils.setField(manager, "userAcproblemEntityService", solved);
        ReflectionTestUtils.setField(manager, "sessionEntityService", sessions);
        UserHomeVO profile = new UserHomeVO(); profile.setUid("victim"); when(records.getUserHomeInfo("victim", null)).thenReturn(profile);
        when(solved.list(any(com.baomidou.mybatisplus.core.conditions.Wrapper.class))).thenReturn(Collections.emptyList());
        assertNull(manager.getUserHomeInfo("victim", null).getRecentLoginTime()); verifyNoInteractions(sessions);
    }
}
