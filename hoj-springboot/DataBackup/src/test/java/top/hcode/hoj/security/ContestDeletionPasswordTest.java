package top.hcode.hoj.security;

import cn.hutool.crypto.SecureUtil;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.controller.admin.AdminContestController;
import top.hcode.hoj.controller.group.GroupContestController;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.dao.group.GroupEntityService;
import top.hcode.hoj.dao.user.UserInfoEntityService;
import top.hcode.hoj.manager.admin.contest.AdminContestManager;
import top.hcode.hoj.manager.group.contest.GroupContestManager;
import top.hcode.hoj.manager.oj.ContestAccessPolicyManager;
import top.hcode.hoj.pojo.dto.DeleteContestDTO;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.group.Group;
import top.hcode.hoj.pojo.entity.user.UserInfo;
import top.hcode.hoj.service.admin.contest.impl.AdminContestServiceImpl;
import top.hcode.hoj.service.group.contest.impl.GroupContestServiceImpl;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.utils.RedisUtils;
import top.hcode.hoj.validator.ContestDeletionPasswordValidator;
import top.hcode.hoj.validator.GroupValidator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

class ContestDeletionPasswordTest {
    private UserInfoEntityService users;
    private ContestEntityService contests;
    private ContestAccessPolicyManager policies;
    private ContestDeletionPasswordValidator passwordValidator;
    private AdminContestManager admin;
    private GroupContestManager group;
    private GroupValidator groupValidator;
    private Subject subject;
    private AccountProfile operator;
    private UserInfo storedOperator;
    private Contest contest;
    private RedisTemplate<String, Object> template;
    private Object previousRedis;
    private Map<String, Long> attempts;

    private static void inject(Object target, String name, Object dependency) { ReflectionTestUtils.setField(target, name, dependency); }
    private DeleteContestDTO request(String password) {
        DeleteContestDTO dto = new DeleteContestDTO(); dto.setCid(7L); dto.setPassword(password); return dto;
    }

    @BeforeEach void setup() {
        operator = new AccountProfile(); operator.setUid("operator-id"); operator.setUsername("operator");
        subject = mock(Subject.class); when(subject.getPrincipal()).thenReturn(operator); when(subject.hasRole("root")).thenReturn(true);
        ThreadContext.bind(subject);
        users = mock(UserInfoEntityService.class);
        storedOperator = new UserInfo().setUuid(operator.getUid()).setStatus(0).setPassword(SecureUtil.md5("own-pass"));
        when(users.getOne(any(Wrapper.class), eq(false))).thenReturn(storedOperator);
        previousRedis = ReflectionTestUtils.getField(RedisUtils.class, "redisTemplate");
        template = mock(RedisTemplate.class); ValueOperations<String, Object> values = mock(ValueOperations.class);
        when(template.opsForValue()).thenReturn(values); attempts = new HashMap<>();
        when(values.increment(anyString(), eq(1L))).thenAnswer(call -> attempts.merge(call.getArgument(0), 1L, Long::sum));
        when(template.expire(anyString(), eq(1800L), eq(TimeUnit.SECONDS))).thenReturn(true);
        when(template.delete(anyString())).thenAnswer(call -> attempts.remove(call.getArgument(0)) != null);
        ReflectionTestUtils.setField(RedisUtils.class, "redisTemplate", template);
        passwordValidator = new ContestDeletionPasswordValidator(); inject(passwordValidator, "userInfoEntityService", users);
        inject(passwordValidator, "redisUtils", new RedisUtils());
        contests = mock(ContestEntityService.class); policies = mock(ContestAccessPolicyManager.class);
        contest = new Contest().setId(7L).setUid("other-owner").setPwd("contest-pass").setIsGroup(false);
        when(contests.getByIdForUpdate(7L)).thenReturn(contest); when(contests.removeById(7L)).thenReturn(true);
        admin = new AdminContestManager(); inject(admin, "contestEntityService", contests);
        inject(admin, "contestAccessPolicyManager", policies); inject(admin, "contestDeletionPasswordValidator", passwordValidator);
        group = new GroupContestManager(); inject(group, "contestEntityService", contests);
        inject(group, "contestAccessPolicyManager", policies); inject(group, "contestDeletionPasswordValidator", passwordValidator);
        GroupEntityService groups = mock(GroupEntityService.class);
        when(groups.getById(9L)).thenReturn(new Group().setId(9L).setStatus(0)); inject(group, "groupEntityService", groups);
        groupValidator = mock(GroupValidator.class); inject(group, "groupValidator", groupValidator);
    }

    @AfterEach void cleanup() { ThreadContext.unbindSubject(); ReflectionTestUtils.setField(RedisUtils.class, "redisTemplate", previousRedis); }

    @Test void missingAccountPasswordHasNoDeletionSideEffects() {
        assertThrows(StatusFailException.class, () -> admin.deleteContest(request(null)));
        assertThrows(StatusFailException.class, () -> admin.deleteContest(request("   ")));
        verifyNoInteractions(policies, users, template); verify(contests, never()).removeById(anyLong());
    }

    @Test void contestPasswordAndAnotherOwnersPasswordCannotDeleteTheContest() {
        assertThrows(StatusFailException.class, () -> admin.deleteContest(request("contest-pass")));
        assertThrows(StatusFailException.class, () -> admin.deleteContest(request("owner-pass")));
        verifyNoInteractions(policies); verify(contests, never()).removeById(anyLong());
        ArgumentCaptor<Wrapper> lookup = ArgumentCaptor.forClass(Wrapper.class); verify(users, times(2)).getOne(lookup.capture(), eq(false));
        lookup.getValue().getSqlSegment(); // Build the bound parameters as MyBatis would.
        assertTrue(((QueryWrapper) lookup.getValue()).getParamNameValuePairs().containsValue("operator-id"));
    }

    @Test void currentOperatorsPasswordDeletesOnlyAfterPermissionAndPasswordValidation() throws Exception {
        admin.deleteContest(request("own-pass"));
        InOrder order = inOrder(contests, users, policies);
        order.verify(contests).getByIdForUpdate(7L);
        order.verify(users).getOne(any(Wrapper.class), eq(false));
        order.verify(policies).deletePolicy(7L); order.verify(contests).removeById(7L);
        assertTrue(attempts.isEmpty());
    }

    @Test void adminPermissionIsCheckedBeforePasswordsOrContestLookup() {
        when(subject.hasRole("root")).thenReturn(false);
        assertThrows(StatusForbiddenException.class, () -> admin.deleteContest(request("own-pass")));
        verifyNoInteractions(contests, users, policies, template);
    }

    @Test void groupPermissionIsCheckedBeforePasswordVerification() {
        when(subject.hasRole("root")).thenReturn(false); contest.setIsGroup(true).setGid(9L);
        assertThrows(StatusForbiddenException.class, () -> group.deleteContest(request("own-pass")));
        verifyNoInteractions(users, policies, template); verify(contests, never()).removeById(anyLong());
    }

    @Test void authorizedGroupOwnerAlsoNeedsTheirOwnPassword() throws Exception {
        when(subject.hasRole("root")).thenReturn(false); contest.setIsGroup(true).setGid(9L).setUid(operator.getUid());
        assertThrows(StatusFailException.class, () -> group.deleteContest(request("contest-pass")));
        verifyNoInteractions(policies); verify(contests, never()).removeById(anyLong());
        group.deleteContest(request("own-pass")); verify(policies).deletePolicy(7L); verify(contests).removeById(7L);
    }

    @Test void forgedGroupIdCannotAuthorizeDeletionEvenWithCorrectPassword() {
        contest.setIsGroup(true).setGid(9L); DeleteContestDTO dto = request("own-pass"); dto.setGid(99L);
        assertThrows(StatusForbiddenException.class, () -> group.deleteContest(dto));
        verifyNoInteractions(users, policies, template); verify(contests, never()).removeById(anyLong());
    }

    @Test void disabledOperatorsCannotDeleteEvenWithAStaleRootSession() {
        storedOperator.setStatus(-1);
        assertThrows(StatusForbiddenException.class, () -> admin.deleteContest(request("own-pass")));
        verifyNoInteractions(policies); verify(contests, never()).removeById(anyLong());
    }

    @Test void livePasswordChangeInvalidatesPreviouslyCorrectPassword() {
        storedOperator.setPassword(SecureUtil.md5("changed-pass"));
        assertThrows(StatusFailException.class, () -> admin.deleteContest(request("own-pass")));
        verifyNoInteractions(policies); verify(contests, never()).removeById(anyLong());
    }

    @Test void repeatedFailureIsRateLimitedBeforeCheckingAnotherPassword() {
        for (int i = 0; i < 5; i++) assertThrows(StatusFailException.class, () -> passwordValidator.validate(operator, "wrong"));
        StatusFailException locked = assertThrows(StatusFailException.class, () -> passwordValidator.validate(operator, "own-pass"));
        assertTrue(locked.getMessage().contains("30分钟")); verify(users, times(5)).getOne(any(Wrapper.class), eq(false));
    }

    @Test void unavailableCredentialStoreCannotCauseDeletion() {
        when(users.getOne(any(Wrapper.class), eq(false))).thenThrow(new DataAccessResourceFailureException("offline"));
        assertThrows(DataAccessResourceFailureException.class, () -> admin.deleteContest(request("own-pass")));
        verifyNoInteractions(policies); verify(contests, never()).removeById(anyLong());
    }

    @Test void passwordKeepsWhitespaceAndNeverAppearsInDtoSerializationOrLogs() throws Exception {
        storedOperator.setPassword(SecureUtil.md5(" own-pass "));
        assertThrows(StatusFailException.class, () -> passwordValidator.validate(operator, "own-pass"));
        passwordValidator.validate(operator, " own-pass ");
        DeleteContestDTO dto = request(" own-pass ");
        assertFalse(dto.toString().contains("own-pass")); assertFalse(new ObjectMapper().writeValueAsString(dto).contains("own-pass"));
    }

    private MockMvc adminHttp() {
        AdminContestServiceImpl service = new AdminContestServiceImpl(); inject(service, "adminContestManager", admin);
        AdminContestController controller = new AdminContestController(); inject(controller, "adminContestService", service);
        return MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test void adminDeleteAcceptsJsonBodyAndRejectsLegacyQueryOrMissingPassword() throws Exception {
        MockMvc http = adminHttp();
        http.perform(delete("/api/admin/contest?cid=7&password=own-pass"))
                .andExpect(status().isOk()).andExpect(content().string(containsString("JSON")));
        http.perform(delete("/api/admin/contest").contentType(MediaType.APPLICATION_JSON).content("{\"cid\":7}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.msg", containsString("当前登录账号的密码")));
        verifyNoInteractions(policies); verify(contests, never()).removeById(anyLong());
        http.perform(delete("/api/admin/contest").contentType(MediaType.APPLICATION_JSON)
                .content("{\"cid\":7,\"password\":\"own-pass\"}"))
                .andExpect(status().isOk()).andExpect(content().string(not(containsString("own-pass"))));
        verify(contests).removeById(7L);
    }

    @Test void groupDeleteAcceptsJsonBodyAndGetNeverDeletesAnything() throws Exception {
        contest.setIsGroup(true).setGid(9L);
        GroupContestServiceImpl service = new GroupContestServiceImpl(); inject(service, "groupContestManager", group);
        GroupContestController controller = new GroupContestController(); inject(controller, "groupContestService", service);
        MockMvc http = MockMvcBuilders.standaloneSetup(controller).build();
        http.perform(get("/api/group/contest").param("cid", "7").param("password", "own-pass"))
                .andExpect(status().isOk());
        http.perform(delete("/api/group/contest").contentType(MediaType.APPLICATION_JSON).content("{\"cid\":7,\"gid\":9}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.msg", containsString("当前登录账号的密码")));
        verifyNoInteractions(policies); verify(contests, never()).removeById(anyLong());
        http.perform(delete("/api/group/contest").contentType(MediaType.APPLICATION_JSON)
                .content("{\"cid\":7,\"gid\":9,\"password\":\"own-pass\"}"))
                .andExpect(status().isOk()); verify(contests).removeById(7L);
    }
}
