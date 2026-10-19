package top.hcode.hoj.security;

import cn.hutool.crypto.SecureUtil;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.builder.StaticSqlSource;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.plugin.Invocation;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.dao.contest.ContestProblemEntityService;
import top.hcode.hoj.dao.contest.ContestAnnouncementEntityService;
import top.hcode.hoj.dao.discussion.DiscussionEntityService;
import top.hcode.hoj.dao.discussion.CommentEntityService;
import top.hcode.hoj.dao.problem.ProblemEntityService;
import top.hcode.hoj.dao.user.UserInfoEntityService;
import top.hcode.hoj.dao.user.UserRoleEntityService;
import top.hcode.hoj.interceptor.ConfidentialResourceWriteInterceptor;
import top.hcode.hoj.interceptor.ProtectedProblemResourceInterceptor;
import top.hcode.hoj.manager.file.ProtectedProblemResourceManager;
import top.hcode.hoj.manager.oj.ContestAccessPolicyManager;
import top.hcode.hoj.mapper.ContestProtectedResourceMapper;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.contest.ContestProblem;
import top.hcode.hoj.pojo.entity.problem.Problem;
import top.hcode.hoj.pojo.entity.user.UserInfo;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.shiro.ShiroConstant;
import top.hcode.hoj.utils.JwtUtils;
import top.hcode.hoj.utils.RedisUtils;
import top.hcode.hoj.validator.ContestValidator;
import top.hcode.hoj.validator.ProblemContentAccessValidator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ProtectedProblemResourceTest {
    private ProtectedProblemResourceManager manager;
    private ContestAccessPolicyManager policies;
    private ContestProtectedResourceMapper references;
    private ContestValidator validator;
    private Contest contest;
    private Map<String, Object> cache;
    private AccountProfile viewer;
    private Object previousRedisTemplate;

    @BeforeEach void setup() {
        viewer = new AccountProfile(); viewer.setUid("student"); viewer.setUsername("student");
        Subject subject = mock(Subject.class); when(subject.getPrincipal()).thenReturn(viewer); ThreadContext.bind(subject);
        contest = new Contest().setId(10L).setUid("owner").setIsGroup(false);
        policies = mock(ContestAccessPolicyManager.class); when(policies.isConfidential(contest)).thenReturn(true);
        references = mock(ContestProtectedResourceMapper.class);
        when(references.findContestIds(anyString(), anyString())).thenReturn(Collections.singletonList(10L));
        ContestEntityService contests = mock(ContestEntityService.class); when(contests.getById(10L)).thenReturn(contest);
        validator = mock(ContestValidator.class);
        UserInfoEntityService users = mock(UserInfoEntityService.class);
        when(users.getById("student")).thenReturn(new UserInfo().setUuid("student").setUsername("student").setStatus(0));
        UserRoleEntityService roles = mock(UserRoleEntityService.class);
        when(roles.getRolesByUid(anyString())).thenReturn(Collections.emptyList());
        RedisUtils redis = new RedisUtils(); cache = new HashMap<>();
        cache.put(ShiroConstant.SHIRO_TOKEN_KEY + "student", "current-session");
        previousRedisTemplate = ReflectionTestUtils.getField(RedisUtils.class, "redisTemplate");
        RedisTemplate<String, Object> template = mock(RedisTemplate.class);
        ValueOperations<String, Object> values = mock(ValueOperations.class);
        when(template.opsForValue()).thenReturn(values);
        when(values.get(anyString())).thenAnswer(call -> cache.get(call.getArgument(0)));
        doAnswer(call -> { cache.put(call.getArgument(0), call.getArgument(1)); return null; })
                .when(values).set(anyString(), any(), anyLong(), eq(TimeUnit.SECONDS));
        ReflectionTestUtils.setField(RedisUtils.class, "redisTemplate", template);
        manager = new ProtectedProblemResourceManager();
        inject(manager, "contestAccessPolicyManager", policies); inject(manager, "protectedResourceMapper", references);
        inject(manager, "contestEntityService", contests); inject(manager, "contestValidator", validator);
        inject(manager, "userInfoEntityService", users); inject(manager, "redisUtils", redis);
        inject(manager, "userRoleEntityService", roles);
        inject(manager, "problemContentAccessValidator", mock(ProblemContentAccessValidator.class));
        inject(manager, "jwtUtils", mock(JwtUtils.class));
    }
    @AfterEach void cleanup() {
        ThreadContext.unbindSubject();
        ReflectionTestUtils.setField(RedisUtils.class, "redisTemplate", previousRedisTemplate);
    }
    private static void inject(Object object, String field, Object value) { ReflectionTestUtils.setField(object, field, value); }
    private String capability() {
        String text = manager.protectContent("![diagram](/api/public/img/secret.png)", 10L);
        assertTrue(text.contains("?contestResourceToken="));
        return text.substring(text.indexOf("?contestResourceToken=") + 22, text.length() - 1);
    }
    private MockHttpServletRequest request(String prefix, String path, String token) {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", prefix + path);
        if (token != null) request.setParameter("contestResourceToken", token);
        return request;
    }

    @Test void bareAttachmentAndItsImageAliasAreRejectedEvenWithKnownFilename() {
        assertThrows(StatusForbiddenException.class, () -> manager.validateResourceRequest(request("/api/public/file/", "secret.png", null)));
        assertThrows(StatusForbiddenException.class, () -> manager.validateResourceRequest(request("/api/public/img/", "secret.png", null)));
        verify(references, times(2)).findContestIds(SecureUtil.sha256("secret.png"), "secret.png");
    }

    @Test void scopedCapabilityWorksForSameResourceButNotAnotherFile() throws Exception {
        String token = capability();
        assertEquals(43, token.length());
        manager.validateResourceRequest(request("/api/public/img/", "secret.png", token));
        assertThrows(StatusForbiddenException.class, () -> manager.validateResourceRequest(request("/api/public/file/", "other.zip", token)));
        verify(references).remember(10L, SecureUtil.sha256("secret.png"), "secret.png");
    }

    @Test void existingCapabilityStopsWorkingImmediatelyWhenSittingEnds() throws Exception {
        String token = capability();
        doThrow(new StatusForbiddenException("ended")).when(validator).validateContestAuth(any(), any(), eq(false));
        assertThrows(StatusForbiddenException.class, () -> manager.validateResourceRequest(request("/api/public/img/", "secret.png", token)));
    }

    @Test void logoutRevokesResourceCapabilities() {
        String token = capability(); cache.remove(ShiroConstant.SHIRO_TOKEN_KEY + "student");
        assertThrows(StatusForbiddenException.class, () -> manager.validateResourceRequest(request("/api/public/img/", "secret.png", token)));
    }

    @Test void publicationReopensRawResourceWithoutPasswordOrLogin() throws Exception {
        when(policies.isReleased(contest)).thenReturn(true);
        manager.validateResourceRequest(request("/api/public/file/", "secret.png", null));
        verifyNoInteractions(validator);
    }

    @Test void encodedAliasesCannotEvadeThePersistentLookup() {
        assertThrows(StatusForbiddenException.class, () -> manager.validateResourceRequest(request("/api/public/file/", "%73ecret.png", null)));
        verify(references).findContestIds(SecureUtil.sha256("secret.png"), "secret.png");
        assertThrows(IllegalArgumentException.class, () -> manager.validateResourceRequest(request("/api/public/file/", "%252e%252e/secret.png", null)));
    }

    @Test void filenameCaseAndHtmlEntitiesCannotEvadeProtection() {
        String protectedText = manager.protectContent("<img src=\"&#47;api&#47;public&#47;img&#47;SECRET&#46;PNG\">", 10L);
        assertTrue(protectedText.contains("contestResourceToken="));
        verify(references).remember(10L, SecureUtil.sha256("secret.png"), "secret.png");
        assertThrows(StatusForbiddenException.class, () -> manager.validateResourceRequest(request("/api/public/img/", "SECRET.PNG", null)));
        verify(references).findContestIds(SecureUtil.sha256("secret.png"), "secret.png");
    }

    @Test void headConditionalAndRangeRequestsAreBlockedBeforeResourceHandler() throws Exception {
        ProtectedProblemResourceInterceptor interceptor = new ProtectedProblemResourceInterceptor();
        inject(interceptor, "protectedProblemResourceManager", manager);
        for (String method : new String[]{"HEAD", "GET"}) {
            MockHttpServletRequest request = request("/api/public/file/", "secret.png", null);
            request.setMethod(method); request.addHeader("Range", "bytes=0-99"); request.addHeader("If-None-Match", "known-etag");
            MockHttpServletResponse response = new MockHttpServletResponse();
            assertFalse(interceptor.preHandle(request, response, new Object())); assertEquals(403, response.getStatus());
            assertEquals("private, no-store, max-age=0", response.getHeader("Cache-Control"));
        }
    }

    @Test void writesRememberOldAndNewResourceReferencesBeforeReturning() throws Throwable {
        ProtectedProblemResourceManager recorder = mock(ProtectedProblemResourceManager.class);
        ConfidentialResourceWriteInterceptor interceptor = new ConfidentialResourceWriteInterceptor();
        inject(interceptor, "protectedProblemResourceManager", recorder);
        Executor executor = mock(Executor.class);
        Configuration configuration = new Configuration();
        MappedStatement statement = new MappedStatement.Builder(configuration,
                "top.hcode.hoj.mapper.ProblemMapper.updateById", new StaticSqlSource(configuration, "UPDATE problem SET description = ?"),
                SqlCommandType.UPDATE).build();
        when(executor.update(eq(statement), any())).thenReturn(1);
        interceptor.intercept(new Invocation(executor, Executor.class.getMethod("update", MappedStatement.class, Object.class),
                new Object[]{statement, new Object()}));
        InOrder order = inOrder(recorder, executor);
        order.verify(recorder).rememberAllConfidentialResources(); order.verify(executor).update(eq(statement), any());
        order.verify(recorder).rememberAllConfidentialResources();
    }

    @Test void editingAwayAnAttachmentDoesNotForgetItsHistoricalEmbargo() {
        Set<String> remembered = new HashSet<>();
        when(references.remember(eq(10L), anyString(), anyString())).thenAnswer(call -> {
            remembered.add(call.getArgument(2)); return 1;
        });
        when(references.findContestIds(anyString(), anyString())).thenAnswer(call -> remembered.contains(call.getArgument(1))
                ? Collections.singletonList(10L) : Collections.emptyList());
        ContestProblemEntityService problemsInContest = mock(ContestProblemEntityService.class);
        when(problemsInContest.list(any())).thenReturn(Collections.singletonList(new ContestProblem().setPid(3L)));
        Problem problem = new Problem().setId(3L).setProblemId("HOJ-3")
                .setDescription("[attachment](/api/public/file/secret.zip)");
        ProblemEntityService problems = mock(ProblemEntityService.class);
        when(problems.listByIds(anyCollection())).thenReturn(Collections.singletonList(problem));
        inject(manager, "contestProblemEntityService", problemsInContest); inject(manager, "problemEntityService", problems);
        inject(manager, "discussionEntityService", mock(DiscussionEntityService.class));
        inject(manager, "commentEntityService", mock(CommentEntityService.class));
        inject(manager, "contestAnnouncementEntityService", mock(ContestAnnouncementEntityService.class));
        manager.rememberContestResources(10L);
        assertTrue(remembered.contains("secret.zip"));
        problem.setDescription("attachment removed from edited statement");
        manager.rememberContestResources(10L);
        assertFalse(manager.findUnreleasedReferences("secret.zip").isEmpty());
        assertThrows(StatusForbiddenException.class, () -> manager.validateResourceRequest(request("/api/public/file/", "secret.zip", null)));
    }

    @Test void incrementingDiscussionCountersDoesNotRescanStatements() throws Throwable {
        ProtectedProblemResourceManager recorder = mock(ProtectedProblemResourceManager.class);
        ConfidentialResourceWriteInterceptor interceptor = new ConfidentialResourceWriteInterceptor();
        inject(interceptor, "protectedProblemResourceManager", recorder);
        Executor executor = mock(Executor.class); Configuration configuration = new Configuration();
        MappedStatement statement = new MappedStatement.Builder(configuration,
                "top.hcode.hoj.mapper.DiscussionMapper.update", new StaticSqlSource(configuration,
                "UPDATE discussion SET view_num=view_num+1,like_num=GREATEST(like_num-1,0) WHERE id = ?"), SqlCommandType.UPDATE).build();
        interceptor.intercept(new Invocation(executor, Executor.class.getMethod("update", MappedStatement.class, Object.class),
                new Object[]{statement, new Object()}));
        verifyNoInteractions(recorder); verify(executor).update(eq(statement), any());
    }
}
