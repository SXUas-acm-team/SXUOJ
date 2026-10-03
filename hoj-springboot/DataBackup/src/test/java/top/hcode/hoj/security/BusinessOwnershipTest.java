package top.hcode.hoj.security;

import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.annotation.HOJAccessEnum;
import top.hcode.hoj.common.exception.*;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.dao.contest.ContestProblemEntityService;
import top.hcode.hoj.dao.group.GroupEntityService;
import top.hcode.hoj.dao.judge.JudgeEntityService;
import top.hcode.hoj.dao.problem.ProblemEntityService;
import top.hcode.hoj.dao.problem.ProblemLanguageEntityService;
import top.hcode.hoj.dao.problem.LanguageEntityService;
import top.hcode.hoj.dao.problem.CodeTemplateEntityService;
import top.hcode.hoj.dao.user.UserInfoEntityService;
import top.hcode.hoj.dao.training.TrainingEntityService;
import top.hcode.hoj.dao.discussion.DiscussionEntityService;
import top.hcode.hoj.manager.admin.contest.AdminContestProblemManager;
import top.hcode.hoj.manager.admin.contest.AdminContestManager;
import top.hcode.hoj.manager.admin.training.AdminTrainingManager;
import top.hcode.hoj.manager.group.GroupManager;
import top.hcode.hoj.manager.group.discussion.GroupDiscussionManager;
import top.hcode.hoj.manager.oj.*;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.contest.ContestProblem;
import top.hcode.hoj.pojo.vo.ProblemInfoVO;
import top.hcode.hoj.utils.Constants;
import java.util.*;
import top.hcode.hoj.pojo.entity.discussion.Discussion;
import top.hcode.hoj.pojo.entity.group.Group;
import top.hcode.hoj.pojo.entity.judge.Judge;
import top.hcode.hoj.pojo.entity.problem.Problem;
import top.hcode.hoj.pojo.entity.training.Training;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.validator.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class BusinessOwnershipTest {
    private Subject subject;
    private AccountProfile user;
    @BeforeEach void setup() {
        subject = mock(Subject.class); user = new AccountProfile(); user.setUid("member"); user.setUsername("member");
        when(subject.getPrincipal()).thenReturn(user); ThreadContext.bind(subject);
    }
    @AfterEach void clear() { ThreadContext.unbindSubject(); }
    private static void inject(Object target, String name, Object dependency) { ReflectionTestUtils.setField(target, name, dependency); }
    @Test void suppliedAuthorDoesNotPermitTrainingStatusChange() {
        AdminTrainingManager manager = new AdminTrainingManager(); TrainingEntityService trainings = mock(TrainingEntityService.class);
        inject(manager, "trainingEntityService", trainings);
        when(trainings.getById(7L)).thenReturn(new Training().setId(7L).setAuthor("actual-owner"));
        assertThrows(StatusForbiddenException.class, () -> manager.changeTrainingStatus(7L, "member", false));
        verify(trainings, never()).updateById(any(Training.class));
    }
    @Test void suppliedGroupOwnerCannotMoveQuotaAccounting() throws Exception {
        GroupManager manager = new GroupManager(); GroupEntityService groups = mock(GroupEntityService.class);
        GroupValidator validator = mock(GroupValidator.class); inject(manager, "groupEntityService", groups); inject(manager, "groupValidator", validator);
        when(validator.isGroupRoot("member", 7L)).thenReturn(true);
        when(groups.getById(7L)).thenReturn(new Group().setId(7L).setUid("member").setOwner("member"));
        when(groups.updateById(any(Group.class))).thenReturn(true);
        Group update = new Group().setId(7L).setUid("victim").setOwner("victim").setName("groupname").setShortName("group").setAuth(1);
        manager.updateGroup(update);
        assertEquals("member", update.getUid()); assertEquals("member", update.getOwner());
    }
    @Test void problemAdminCannotDeleteAnotherOwnersContestSubmissions() {
        when(subject.hasRole("problem_admin")).thenReturn(true);
        AdminContestProblemManager manager = new AdminContestProblemManager(); ContestEntityService contests = mock(ContestEntityService.class);
        JudgeEntityService judges = mock(JudgeEntityService.class); inject(manager, "contestEntityService", contests); inject(manager, "judgeEntityService", judges);
        when(contests.getById(7L)).thenReturn(new Contest().setUid("owner").setIsGroup(false));
        assertThrows(StatusFailException.class, () -> manager.deleteProblem(2L, 7L)); verifyNoInteractions(judges);
    }
    @Test void publicProblemIgnoresForgedGroupAndUsesPublicJudgeSwitch() throws Exception {
        BeforeDispatchInitManager manager = new BeforeDispatchInitManager(); ProblemEntityService problems = mock(ProblemEntityService.class);
        JudgeEntityService judges = mock(JudgeEntityService.class); AccessValidator access = mock(AccessValidator.class);
        inject(manager, "problemEntityService", problems); inject(manager, "judgeEntityService", judges);
        inject(manager, "accessValidator", access); inject(manager, "trainingManager", mock(TrainingManager.class));
        when(problems.getOne(any(), eq(false))).thenReturn(new Problem().setId(2L).setProblemId("P2").setAuth(1).setIsGroup(false));
        Judge judge = new Judge().setGid(99L).setUid("member");
        manager.initCommonSubmission("P2", 99L, judge);
        assertNull(judge.getGid()); verify(access).validateAccess(HOJAccessEnum.PUBLIC_JUDGE);
    }
    @Test void groupDiscussionCannotResolveProblemFromAnotherTenant() {
        GroupDiscussionManager manager = new GroupDiscussionManager(); GroupEntityService groups = mock(GroupEntityService.class);
        GroupValidator validator = mock(GroupValidator.class); ProblemEntityService problems = mock(ProblemEntityService.class);
        inject(manager, "commonValidator", new CommonValidator()); inject(manager, "groupEntityService", groups);
        inject(manager, "groupValidator", validator); inject(manager, "problemEntityService", problems);
        when(groups.getById(7L)).thenReturn(new Group().setId(7L).setStatus(0)); when(validator.isGroupMember("member", 7L)).thenReturn(true);
        when(problems.getOne(any())).thenReturn(new Problem().setIsGroup(true).setGid(8L));
        Discussion discussion = new Discussion().setGid(7L).setPid("P2").setTitle("title").setDescription("description").setContent("content").setCategoryId(1);
        assertThrows(StatusForbiddenException.class, () -> manager.addDiscussion(discussion)); assertEquals(7L, discussion.getGid());
    }
    @Test void revokedPrivateGroupTrainingMembershipDoesNotCountProgress() throws Exception {
        TrainingValidator validator = new TrainingValidator(); inject(validator, "groupValidator", mock(GroupValidator.class));
        Training training = new Training().setId(1L).setAuth("Private").setAuthor("member").setIsGroup(true).setGid(7L).setStatus(true);
        assertFalse(validator.isInTrainingOrAdmin(training, user));
    }
    @Test void discussionRoleCannotBeSpoofedByARegularGroupMember() throws Exception {
        when(subject.hasRole("admin")).thenReturn(true);
        GroupDiscussionManager manager = new GroupDiscussionManager(); GroupEntityService groups = mock(GroupEntityService.class);
        GroupValidator validator = mock(GroupValidator.class); DiscussionEntityService discussions = mock(DiscussionEntityService.class);
        inject(manager, "commonValidator", new CommonValidator()); inject(manager, "groupEntityService", groups);
        inject(manager, "groupValidator", validator); inject(manager, "discussionEntityService", discussions);
        when(groups.getById(7L)).thenReturn(new Group().setId(7L).setStatus(0)); when(validator.isGroupMember("member", 7L)).thenReturn(true);
        when(discussions.save(any(Discussion.class))).thenReturn(true);
        Discussion input = new Discussion().setGid(7L).setRole("root").setUid("victim").setTitle("title")
                .setDescription("description").setContent("content").setCategoryId(1).setTopPriority(true);
        manager.addDiscussion(input);
        assertEquals("user", input.getRole()); assertEquals("member", input.getUid()); assertFalse(input.getTopPriority());
    }
    @Test void cloneCannotCopyAnotherOwnersContestSecrets() {
        when(subject.hasRole("problem_admin")).thenReturn(true);
        AdminContestManager manager = new AdminContestManager(); ContestEntityService contests = mock(ContestEntityService.class);
        inject(manager, "contestEntityService", contests);
        when(contests.getById(7L)).thenReturn(new Contest().setId(7L).setUid("victim").setPwd("secret").setTitle("contest"));
        assertThrows(StatusSystemErrorException.class, () -> manager.cloneContest(7L));
        verify(contests, never()).save(any(Contest.class));
    }
    @Test void contestProblemResponseScrubsSpecialJudgeAndJudgeOnlyFiles() throws Exception {
        ContestManager manager = new ContestManager(); ContestEntityService contests = mock(ContestEntityService.class);
        ContestProblemEntityService assignments = mock(ContestProblemEntityService.class); ProblemEntityService problems = mock(ProblemEntityService.class);
        ProblemLanguageEntityService problemLanguages = mock(ProblemLanguageEntityService.class); LanguageEntityService languages = mock(LanguageEntityService.class);
        CodeTemplateEntityService templates = mock(CodeTemplateEntityService.class); UserInfoEntityService users = mock(UserInfoEntityService.class);
        inject(manager, "contestEntityService", contests); inject(manager, "contestProblemEntityService", assignments);
        inject(manager, "problemEntityService", problems); inject(manager, "problemLanguageEntityService", problemLanguages);
        inject(manager, "languageEntityService", languages); inject(manager, "codeTemplateEntityService", templates);
        inject(manager, "userInfoEntityService", users); inject(manager, "judgeEntityService", mock(JudgeEntityService.class));
        inject(manager, "contestValidator", mock(ContestValidator.class));
        when(contests.getById(1L)).thenReturn(new Contest().setId(1L).setUid("owner").setIsGroup(false)
                .setStatus(Constants.Contest.STATUS_ENDED.getCode()).setAllowEndSubmit(false));
        when(assignments.getOne(any())).thenReturn(new ContestProblem().setId(3L).setPid(2L).setCid(1L).setDisplayTitle("title"));
        when(problems.getById(2L)).thenReturn(new Problem().setId(2L).setAuth(1).setSpjCode("secret checker")
                .setSpjLanguage("C++").setJudgeExtraFile("{\"answer.txt\":\"secret answers\"}"));
        when(problemLanguages.list(any(com.baomidou.mybatisplus.core.conditions.Wrapper.class))).thenReturn(Collections.emptyList());
        when(languages.listByIds(anyCollection())).thenReturn(Collections.emptyList());
        when(templates.list(any(com.baomidou.mybatisplus.core.conditions.Wrapper.class))).thenReturn(Collections.emptyList());
        when(users.getSuperAdminUidList()).thenReturn(new ArrayList<>());
        ProblemInfoVO result = manager.getContestProblemDetails(1L, "A", false);
        assertNull(result.getProblem().getSpjCode()); assertNull(result.getProblem().getSpjLanguage()); assertNull(result.getProblem().getJudgeExtraFile());
    }
}
