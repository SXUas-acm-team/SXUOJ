package top.hcode.hoj.security;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.common.exception.StatusAccessDeniedException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.dao.contest.ContestProblemEntityService;
import top.hcode.hoj.dao.discussion.DiscussionEntityService;
import top.hcode.hoj.dao.discussion.CommentEntityService;
import top.hcode.hoj.dao.judge.JudgeEntityService;
import top.hcode.hoj.dao.problem.ProblemEntityService;
import top.hcode.hoj.manager.oj.*;
import top.hcode.hoj.mapper.ContestProtectedResourceMapper;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.contest.ContestProblem;
import top.hcode.hoj.pojo.entity.judge.Judge;
import top.hcode.hoj.pojo.entity.discussion.Comment;
import top.hcode.hoj.pojo.entity.discussion.Discussion;
import top.hcode.hoj.pojo.entity.problem.Problem;
import top.hcode.hoj.pojo.vo.DiscussionVO;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.validator.ContestValidator;
import top.hcode.hoj.validator.GroupValidator;
import top.hcode.hoj.validator.ProblemContentAccessValidator;
import top.hcode.hoj.judge.self.JudgeDispatcher;
import top.hcode.hoj.utils.Constants;
import top.hcode.hoj.utils.RedisUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ConfidentialProblemContentTest {
    private Subject subject;
    private AccountProfile viewer;
    private Contest secret;
    private ContestAccessPolicyManager policies;
    private ContestValidator contests;
    private ProblemContentAccessValidator content;

    @BeforeEach void setup() {
        viewer = new AccountProfile(); viewer.setUid("student"); viewer.setUsername("student");
        subject = mock(Subject.class); when(subject.getPrincipal()).thenReturn(viewer); ThreadContext.bind(subject);
        secret = new Contest().setId(10L).setUid("owner").setVisible(true).setIsGroup(false);
        policies = mock(ContestAccessPolicyManager.class);
        when(policies.isConfidential(secret)).thenReturn(true);
        contests = mock(ContestValidator.class);
        ContestEntityService entities = mock(ContestEntityService.class);
        when(entities.getById(10L)).thenReturn(secret);
        ContestProblemEntityService references = mock(ContestProblemEntityService.class);
        when(references.list(any(Wrapper.class))).thenReturn(Collections.singletonList(new ContestProblem().setCid(10L).setPid(3L)));
        content = new ProblemContentAccessValidator();
        inject(content, "contestAccessPolicyManager", policies); inject(content, "contestEntityService", entities);
        inject(content, "contestProblemEntityService", references); inject(content, "contestValidator", contests);
        inject(content, "groupValidator", mock(GroupValidator.class));
        inject(content, "protectedResourceMapper", mock(ContestProtectedResourceMapper.class));
    }
    @AfterEach void cleanup() { ThreadContext.unbindSubject(); }
    private static void inject(Object object, String name, Object value) { ReflectionTestUtils.setField(object, name, value); }

    @Test void publicProblemAndOldPublicContestCannotBypassUnreleasedSitting() {
        assertThrows(StatusForbiddenException.class, () -> content.validateProblemAccess(3L, null));
        assertThrows(StatusForbiddenException.class, () -> content.validateProblemAccess(3L, 5L));
        verifyNoInteractions(contests);
    }

    @Test void authorizedCurrentSittingCanUseSameProblemWhileAnotherSittingRemainsLocked() throws Exception {
        content.validateProblemAccess(3L, 10L);
        verify(contests).validateContestAuth(secret, viewer, false);
        doThrow(new StatusForbiddenException("password missing / sitting ended"))
                .when(contests).validateContestAuth(secret, viewer, false);
        assertThrows(StatusForbiddenException.class, () -> content.validateProblemAccess(3L, 10L));
    }

    @Test void allReferencesMustBeReleasedBeforeOrdinaryProblemRoutesReopen() throws Exception {
        when(policies.isReleased(secret)).thenReturn(true);
        content.validateProblemAccess(3L, null);
        verifyNoInteractions(contests);
    }

    @Test void releasingOneSittingDoesNotOverrideAnotherUnreleasedReference() {
        Contest other = new Contest().setId(11L).setUid("other-owner").setIsGroup(false);
        ContestEntityService entities = mock(ContestEntityService.class);
        when(entities.getById(10L)).thenReturn(secret); when(entities.getById(11L)).thenReturn(other);
        ContestProblemEntityService references = mock(ContestProblemEntityService.class);
        when(references.list(any(Wrapper.class))).thenReturn(Arrays.asList(
                new ContestProblem().setCid(10L), new ContestProblem().setCid(11L)));
        inject(content, "contestEntityService", entities); inject(content, "contestProblemEntityService", references);
        when(policies.isConfidential(other)).thenReturn(true); when(policies.isReleased(secret)).thenReturn(true);
        assertThrows(StatusForbiddenException.class, () -> content.validateProblemAccess(3L, 10L));
    }

    @Test void directProblemDetailIsRejectedEvenIfItsAuthIsPublic() {
        ProblemEntityService problems = mock(ProblemEntityService.class);
        when(problems.getOne(any(Wrapper.class), eq(false))).thenReturn(new Problem().setId(3L).setAuth(1));
        ProblemManager manager = new ProblemManager(); inject(manager, "problemEntityService", problems);
        inject(manager, "problemContentAccessValidator", content);
        assertThrows(StatusForbiddenException.class, () -> manager.getProblemInfo("HOJ-3", null));
    }

    @Test void ownLastAcceptedCodeCannotBeRecoveredFromSealedSitting() throws Exception {
        doThrow(new StatusForbiddenException("sealed")).when(contests).validateContestAuth(secret, viewer, false);
        ProblemManager manager = new ProblemManager(); inject(manager, "problemContentAccessValidator", content);
        JudgeEntityService judges = mock(JudgeEntityService.class); inject(manager, "judgeEntityService", judges);
        assertThrows(StatusForbiddenException.class, () -> manager.getUserLastAcceptedCode(3L, 10L));
        verifyNoInteractions(judges);
    }

    @Test void legacySharedCodeAndItsCaseOutputAreHiddenAfterProblemEntersEmbargo() {
        JudgeEntityService judges = mock(JudgeEntityService.class);
        when(judges.getById(7L)).thenReturn(new Judge().setSubmitId(7L).setPid(3L).setCid(0L).setUid("student").setShare(true));
        JudgeManager manager = new JudgeManager(); inject(manager, "judgeEntityService", judges);
        inject(manager, "problemContentAccessValidator", content);
        assertThrows(StatusAccessDeniedException.class, () -> manager.getSubmission(7L));
        assertThrows(StatusForbiddenException.class, () -> manager.getALLCaseResult(7L));
    }

    @Test void existingPublicEditorialCannotRevealAnEmbargoedProblem() {
        ProblemEntityService problems = mock(ProblemEntityService.class);
        when(problems.getOne(any(Wrapper.class), eq(false))).thenReturn(new Problem().setId(3L));
        inject(content, "problemEntityService", problems);
        DiscussionEntityService discussions = mock(DiscussionEntityService.class);
        DiscussionVO editorial = new DiscussionVO(); editorial.setPid("HOJ-3"); editorial.setContent("answer");
        when(discussions.getDiscussion(9, "student")).thenReturn(editorial);
        DiscussionManager manager = new DiscussionManager(); inject(manager, "discussionEntityService", discussions);
        inject(manager, "problemContentAccessValidator", content);
        assertThrows(StatusForbiddenException.class, () -> manager.getDiscussion(9));
        verify(discussions, never()).update(any(Wrapper.class));
    }

    @Test void unavailablePolicyStoreNeverSilentlyMakesProblemPublic() {
        when(policies.isConfidential(secret)).thenThrow(new IllegalStateException("database unavailable"));
        assertThrows(IllegalStateException.class, () -> content.canAccessProblem(3L, null));
    }

    @Test void removingProblemFromContestDoesNotReleaseItsHistoricalEmbargo() {
        ContestProblemEntityService references = mock(ContestProblemEntityService.class);
        ContestProtectedResourceMapper remembered = mock(ContestProtectedResourceMapper.class);
        when(remembered.findContestIds(anyString(), eq("problem:3"))).thenReturn(Collections.singletonList(10L));
        inject(content, "contestProblemEntityService", references); inject(content, "protectedResourceMapper", remembered);
        assertThrows(StatusForbiddenException.class, () -> content.validateProblemAccess(3L, null));
    }

    @Test void editorialCommentsAndRepliesCannotBypassTheEditorialLock() {
        ProblemEntityService problems = mock(ProblemEntityService.class);
        when(problems.getOne(any(Wrapper.class), eq(false))).thenReturn(new Problem().setId(3L));
        inject(content, "problemEntityService", problems);
        DiscussionEntityService discussions = mock(DiscussionEntityService.class);
        when(discussions.getOne(any(Wrapper.class))).thenReturn(new Discussion().setId(9).setPid("HOJ-3"));
        CommentEntityService comments = mock(CommentEntityService.class);
        when(comments.getById(7)).thenReturn(new Comment().setId(7).setDid(9));
        CommentManager manager = new CommentManager(); inject(manager, "discussionEntityService", discussions);
        inject(manager, "commentEntityService", comments); inject(manager, "problemContentAccessValidator", content);
        assertThrows(StatusForbiddenException.class, () -> manager.getComments(null, 9, 10, 1));
        assertThrows(StatusForbiddenException.class, () -> manager.getAllReply(7, null));
        verify(comments, never()).getCommentList(anyInt(), anyInt(), any(), any(), anyBoolean(), any());
    }

    @Test void publishedConfidentialTeamSubmissionDoesNotRequireTeamMembershipAgain() throws Exception {
        secret.setIsGroup(true).setGid(7L).setType(0).setStatus(Constants.Contest.STATUS_ENDED.getCode());
        when(contests.isConfidential(secret)).thenReturn(true);
        when(policies.isReleased(secret)).thenReturn(true);
        ContestEntityService entities = mock(ContestEntityService.class); when(entities.getById(10L)).thenReturn(secret);
        JudgeEntityService judges = mock(JudgeEntityService.class);
        when(judges.getById(7L)).thenReturn(new Judge().setSubmitId(7L).setUid("student").setPid(3L)
                .setCid(10L).setGid(7L).setStatus(0));
        ProblemEntityService problems = mock(ProblemEntityService.class);
        when(problems.getById(3L)).thenReturn(new Problem().setId(3L).setOpenCaseResult(false));
        JudgeManager manager = new JudgeManager(); inject(manager, "judgeEntityService", judges);
        inject(manager, "contestEntityService", entities); inject(manager, "problemEntityService", problems);
        inject(manager, "contestValidator", contests); inject(manager, "groupValidator", mock(GroupValidator.class));
        inject(manager, "problemContentAccessValidator", content);
        assertNotNull(manager.getSubmission(7L));
        assertNull(manager.getALLCaseResult(7L));
    }

    @Test void publishedContestAllowsOwnerToRetryFailedLocalSubmission() throws Exception {
        Object previous = ReflectionTestUtils.getField(RedisUtils.class, "redisTemplate");
        try {
            RedisTemplate<String, Object> template = mock(RedisTemplate.class);
            ValueOperations<String, Object> values = mock(ValueOperations.class);
            when(template.opsForValue()).thenReturn(values);
            when(values.setIfAbsent(anyString(), eq("1"), anyLong(), eq(TimeUnit.SECONDS))).thenReturn(true);
            ReflectionTestUtils.setField(RedisUtils.class, "redisTemplate", template);
            when(policies.isReleased(secret)).thenReturn(true);
            when(contests.isReleased(secret)).thenReturn(true);
            secret.setStatus(Constants.Contest.STATUS_ENDED.getCode());
            ContestEntityService entities = mock(ContestEntityService.class); when(entities.getById(10L)).thenReturn(secret);
            JudgeEntityService judges = mock(JudgeEntityService.class);
            when(judges.getById(7L)).thenReturn(new Judge().setSubmitId(7L).setUid("student").setPid(3L).setCid(10L)
                    .setStatus(Constants.Judge.STATUS_SUBMITTED_FAILED.getStatus()).setVersion(0));
            ProblemEntityService problems = mock(ProblemEntityService.class);
            when(problems.getOne(any(Wrapper.class))).thenReturn(new Problem().setId(3L).setIsRemote(false));
            JudgeDispatcher dispatcher = mock(JudgeDispatcher.class);
            JudgeManager manager = new JudgeManager(); inject(manager, "judgeEntityService", judges);
            inject(manager, "contestEntityService", entities); inject(manager, "contestValidator", contests);
            inject(manager, "problemContentAccessValidator", content); inject(manager, "problemEntityService", problems);
            inject(manager, "redisUtils", new RedisUtils()); inject(manager, "judgeDispatcher", dispatcher);
            Judge retried = manager.resubmit(7L);
            assertEquals(Constants.Judge.STATUS_PENDING.getStatus(), retried.getStatus());
            assertEquals(Integer.valueOf(1), retried.getVersion());
            verify(contests).validateJudgeAuth(secret, "student"); verify(dispatcher).sendTask(7L, 3L, true);
        } finally {
            ReflectionTestUtils.setField(RedisUtils.class, "redisTemplate", previous);
        }
    }
}
