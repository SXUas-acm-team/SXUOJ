package top.hcode.hoj.security;

import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import java.util.concurrent.TimeUnit;
import top.hcode.hoj.common.exception.*;
import top.hcode.hoj.dao.contest.*;
import top.hcode.hoj.dao.discussion.*;
import top.hcode.hoj.dao.judge.JudgeEntityService;
import top.hcode.hoj.dao.problem.*;
import top.hcode.hoj.manager.oj.*;
import top.hcode.hoj.pojo.dto.ReplyDTO;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.discussion.*;
import top.hcode.hoj.pojo.entity.judge.Judge;
import top.hcode.hoj.pojo.entity.problem.Problem;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.utils.Constants;
import top.hcode.hoj.utils.RedisUtils;
import top.hcode.hoj.validator.*;

import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class BackendAuthorizationTest {
    private Subject subject;
    private AccountProfile user;
    @BeforeEach void login() {
        subject = mock(Subject.class);
        user = new AccountProfile(); user.setUid("member"); user.setUsername("member");
        when(subject.getPrincipal()).thenReturn(user);
        ThreadContext.bind(subject);
    }
    @AfterEach void logout() { ThreadContext.unbindSubject(); }
    private static void inject(Object target, String name, Object dependency) { ReflectionTestUtils.setField(target, name, dependency); }
    private static Contest contest(int status) {
        return new Contest().setId(7L).setUid("owner").setAuthor("owner").setGid(9L).setIsGroup(true)
                .setStatus(status).setVisible(true).setAuth(0).setSealRank(false).setOpenRank(true);
    }
    @Test void groupOutsiderCannotSubmitEvenAfterContestEnds() {
        ContestValidator validator = new ContestValidator();
        inject(validator, "groupValidator", mock(GroupValidator.class));
        assertThrows(StatusForbiddenException.class, () -> validator.validateJudgeAuth(contest(Constants.Contest.STATUS_ENDED.getCode()), user.getUid()));
    }
    @Test void anonymousPrivateContestReadIsDeniedWithoutNullDereference() {
        ContestValidator validator = new ContestValidator();
        inject(validator, "groupValidator", mock(GroupValidator.class));
        Contest privateContest = contest(Constants.Contest.STATUS_RUNNING.getCode()).setIsGroup(false).setAuth(1);
        assertThrows(StatusForbiddenException.class, () -> validator.validateContestAuth(privateContest, null, false));
    }
    @Test void externalRankCannotReadPrivateContestOrScheduledContest() throws Exception {
        ContestValidator validator = new ContestValidator();
        ContestEntityService service = mock(ContestEntityService.class);
        inject(validator, "contestEntityService", service);
        inject(validator, "groupValidator", mock(GroupValidator.class));
        inject(validator, "contestRegisterEntityService", mock(ContestRegisterEntityService.class));
        when(service.getById(8L)).thenReturn(contest(Constants.Contest.STATUS_ENDED.getCode()).setIsGroup(false).setAuth(1));
        assertThrows(StatusForbiddenException.class, () -> validator.validateExternalRanks(Collections.singletonList(8), null, false));
        when(service.getById(8L)).thenReturn(contest(Constants.Contest.STATUS_SCHEDULED.getCode()).setIsGroup(false));
        assertThrows(StatusForbiddenException.class, () -> validator.validateExternalRanks(Collections.singletonList(8), user, false));
        assertThrows(StatusForbiddenException.class, () -> validator.validateExternalRanks(Collections.nCopies(11, 8), user, false));
        assertNull(validator.validateExternalRanks(Collections.emptyList(), user, false));
    }
    @Test void endedContestResubmitDoesNotMutateJudgingState() {
        JudgeManager manager = new JudgeManager();
        JudgeEntityService judges = mock(JudgeEntityService.class);
        ContestEntityService contests = mock(ContestEntityService.class);
        RedisUtils redis = new RedisUtils(); RedisTemplate template = mock(RedisTemplate.class); redis.setRedisTemplate(template);
        inject(manager, "judgeEntityService", judges); inject(manager, "contestEntityService", contests); inject(manager, "redisUtils", redis);
        when(judges.getById(1L)).thenReturn(new Judge().setUid(user.getUid()).setCid(7L).setStatus(Constants.Judge.STATUS_SYSTEM_ERROR.getStatus()));
        when(contests.getById(7L)).thenReturn(contest(Constants.Contest.STATUS_ENDED.getCode()));
        assertThrows(StatusForbiddenException.class, () -> manager.resubmit(1L));
        verifyNoInteractions(template); verify(judges, never()).updateById(any(Judge.class));
    }
    @Test void successfulSubmissionCannotBeRetriedToChangeItsVerdict() {
        JudgeManager manager = new JudgeManager(); JudgeEntityService judges = mock(JudgeEntityService.class);
        inject(manager, "judgeEntityService", judges);
        when(judges.getById(1L)).thenReturn(new Judge().setUid(user.getUid()).setCid(0L).setStatus(Constants.Judge.STATUS_ACCEPTED.getStatus()));
        assertThrows(StatusForbiddenException.class, () -> manager.resubmit(1L));
    }
    @Test void testcaseResultsCannotBeEnumeratedByAnotherUser() {
        JudgeManager manager = new JudgeManager(); JudgeEntityService judges = mock(JudgeEntityService.class);
        inject(manager, "judgeEntityService", judges);
        when(judges.getById(1L)).thenReturn(new Judge().setUid("victim").setCid(0L));
        assertThrows(StatusForbiddenException.class, () -> manager.getALLCaseResult(1L));
    }
    @Test void privateSubmissionMetadataIsDeniedToAnonymousCaller() {
        when(subject.getPrincipal()).thenReturn(null);
        JudgeManager manager = new JudgeManager(); JudgeEntityService judges = mock(JudgeEntityService.class);
        inject(manager, "judgeEntityService", judges);
        when(judges.getById(1L)).thenReturn(new Judge().setUid("victim").setCid(0L).setShare(false));
        assertThrows(StatusAccessDeniedException.class, () -> manager.getSubmission(1L));
    }
    @Test void hiddenProblemTemplatesAreNotExposedByPid() {
        when(subject.getPrincipal()).thenReturn(null);
        CommonManager manager = new CommonManager(); ProblemEntityService problems = mock(ProblemEntityService.class);
        CodeTemplateEntityService templates = mock(CodeTemplateEntityService.class);
        inject(manager, "problemEntityService", problems); inject(manager, "codeTemplateEntityService", templates);
        when(problems.getById(1L)).thenReturn(new Problem().setAuth(3).setIsGroup(false));
        assertTrue(manager.getProblemCodeTemplate(1L).isEmpty()); verifyNoInteractions(templates);
    }
    @Test void createCannotOverwriteAnExistingDiscussionOrComment() {
        DiscussionManager discussions = new DiscussionManager();
        assertThrows(StatusFailException.class, () -> discussions.addDiscussion(new Discussion().setId(10)));
        CommentManager comments = new CommentManager();
        assertThrows(StatusFailException.class, () -> comments.addComment(new Comment().setId(10).setDid(1)));
        assertThrows(StatusFailException.class, () -> comments.addComment(new Comment().setDid(1).setCid(7L)));
    }
    @Test void suppliedContestIdCannotAuthorizePrivateGroupReplies() {
        CommentManager manager = new CommentManager(); CommentEntityService comments = mock(CommentEntityService.class);
        ReplyEntityService replies = mock(ReplyEntityService.class);
        inject(manager, "commentEntityService", comments); inject(manager, "replyEntityService", replies);
        when(comments.getById(10)).thenReturn(new Comment().setId(10).setDid(9).setCid(null));
        assertThrows(StatusForbiddenException.class, () -> manager.getAllReply(10, 7L)); verifyNoInteractions(replies);
    }
    @Test void forgedReplyRecipientAndDiscussionAreReplacedByStoredContext() throws Exception {
        when(subject.hasRole("admin")).thenReturn(true);
        CommentManager manager = new CommentManager(); CommentEntityService comments = mock(CommentEntityService.class);
        ReplyEntityService replies = mock(ReplyEntityService.class); DiscussionEntityService discussions = mock(DiscussionEntityService.class);
        inject(manager, "commentEntityService", comments); inject(manager, "replyEntityService", replies);
        inject(manager, "discussionEntityService", discussions); inject(manager, "accessValidator", mock(AccessValidator.class));
        inject(manager, "commonValidator", new CommonValidator());
        when(comments.getById(10)).thenReturn(new Comment().setId(10).setDid(20).setFromUid("actual-author").setFromName("author"));
        when(discussions.getOne(any())).thenReturn(new Discussion().setId(20));
        when(replies.save(any(Reply.class))).thenReturn(true);
        Reply reply = new Reply().setCommentId(10).setToUid("unrelated-admin").setContent("reply");
        ReplyDTO dto = new ReplyDTO().setReply(reply).setDid(999).setQuoteType("Comment").setQuoteId(777);
        manager.addReply(dto);
        assertEquals("actual-author", reply.getToUid()); assertEquals(20, dto.getDid()); assertEquals(10, dto.getQuoteId());
        verify(replies).updateReplyMsg(eq(20), eq("Discussion"), anyString(), eq(10), eq("Comment"), eq("actual-author"), eq(user.getUid()));
    }
    @Test void duplicateDiscussionLikeDoesNotChangeCountersOrSendNotifications() throws Exception {
        DiscussionManager manager = new DiscussionManager(); DiscussionEntityService discussions = mock(DiscussionEntityService.class);
        DiscussionLikeEntityService likes = mock(DiscussionLikeEntityService.class); RedisUtils redis = new RedisUtils();
        RedisTemplate template = mock(RedisTemplate.class); ValueOperations values = mock(ValueOperations.class);
        redis.setRedisTemplate(template); when(template.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(anyString(), any(), anyLong(), any(TimeUnit.class))).thenReturn(true);
        inject(manager, "discussionEntityService", discussions); inject(manager, "discussionLikeEntityService", likes); inject(manager, "redisUtils", redis);
        when(discussions.getById(2)).thenReturn(new Discussion().setId(2).setUid("owner").setAuthor("owner"));
        when(likes.getOne(any(), eq(false))).thenReturn(new DiscussionLike().setId(3));
        manager.addDiscussionLike(2, true);
        verify(discussions, never()).update(any(com.baomidou.mybatisplus.core.conditions.Wrapper.class));
        verify(discussions, never()).updatePostLikeMsg(anyString(), anyString(), anyInt(), any());
    }
    @Test void unicodeSpacesAreNormalizedForStarredAccounts() {
        Map<String, Boolean> stars = ReflectionTestUtils.invokeMethod(new ContestCalculateRankManager(), "starAccountToMap", "{\"star_account\":[\"\\u00a0alice\\u3000\"]}");
        assertTrue(stars.containsKey("alice"));
    }
    @Test void extremeRankPageReturnsEmptyInsteadOfIntegerOverflow() {
        com.baomidou.mybatisplus.extension.plugins.pagination.Page<?> page = ReflectionTestUtils.invokeMethod(new ContestRankManager(), "getPagingRankList", Arrays.asList("first", "second"), Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertTrue(page.getRecords().isEmpty()); assertTrue(page.getSize() <= 100);
    }
}
