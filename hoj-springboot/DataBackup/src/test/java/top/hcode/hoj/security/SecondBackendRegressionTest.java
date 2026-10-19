package top.hcode.hoj.security;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.common.exception.StatusNotFoundException;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.dao.contest.ContestProblemEntityService;
import top.hcode.hoj.dao.discussion.DiscussionEntityService;
import top.hcode.hoj.dao.group.GroupEntityService;
import top.hcode.hoj.dao.group.GroupMemberEntityService;
import top.hcode.hoj.dao.judge.JudgeEntityService;
import top.hcode.hoj.dao.training.TrainingCategoryEntityService;
import top.hcode.hoj.dao.training.TrainingEntityService;
import top.hcode.hoj.dao.training.TrainingProblemEntityService;
import top.hcode.hoj.dao.training.TrainingRegisterEntityService;
import top.hcode.hoj.manager.admin.training.AdminTrainingRecordManager;
import top.hcode.hoj.manager.group.GroupManager;
import top.hcode.hoj.manager.group.contest.GroupContestProblemManager;
import top.hcode.hoj.manager.group.discussion.GroupDiscussionManager;
import top.hcode.hoj.manager.oj.TrainingManager;
import top.hcode.hoj.pojo.dto.RegisterTrainingDTO;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.contest.ContestProblem;
import top.hcode.hoj.pojo.entity.discussion.Discussion;
import top.hcode.hoj.pojo.entity.group.Group;
import top.hcode.hoj.pojo.entity.group.GroupMember;
import top.hcode.hoj.pojo.entity.training.Training;
import top.hcode.hoj.pojo.entity.training.TrainingRegister;
import top.hcode.hoj.pojo.vo.TrainingVO;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.utils.RedisUtils;
import top.hcode.hoj.validator.GroupValidator;
import top.hcode.hoj.validator.TrainingValidator;

import java.util.Collections;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Offline behavior regressions: services and Redis transport are mocked. */
class SecondBackendRegressionTest {
    private Subject subject;
    private GroupEntityService groups;
    private GroupValidator groupValidator;

    @BeforeEach void setup() {
        AccountProfile user = new AccountProfile(); user.setUid("operator"); user.setUsername("operator");
        subject = mock(Subject.class); when(subject.getPrincipal()).thenReturn(user); ThreadContext.bind(subject);
        groups = mock(GroupEntityService.class); groupValidator = mock(GroupValidator.class);
        when(groups.getById(7L)).thenReturn(group(0));
        when(groupValidator.isGroupRoot("operator", 7L)).thenReturn(true);
        when(groupValidator.isGroupAdmin("operator", 7L)).thenReturn(true);
        when(groupValidator.isGroupMember("operator", 7L)).thenReturn(true);
    }
    @AfterEach void cleanup() { ThreadContext.unbindSubject(); }
    private static void inject(Object target, String field, Object value) { ReflectionTestUtils.setField(target, field, value); }
    private Group group(int status) {
        return new Group().setId(7L).setName("team name").setShortName("teamshort").setBrief("team brief")
                .setAuth(1).setStatus(status).setUid("owner").setOwner("owner").setVisible(true);
    }
    private GroupManager groupManager() {
        GroupManager manager = new GroupManager(); inject(manager, "groupEntityService", groups);
        inject(manager, "groupValidator", groupValidator); when(groups.updateById(any(Group.class))).thenReturn(true);
        return manager;
    }
    private GroupContestProblemManager contestManager(ContestProblemEntityService problems) {
        GroupContestProblemManager manager = new GroupContestProblemManager();
        ContestEntityService contests = mock(ContestEntityService.class);
        when(contests.getById(11L)).thenReturn(new Contest().setId(11L).setGid(7L).setUid("operator"));
        inject(manager, "contestEntityService", contests); inject(manager, "contestProblemEntityService", problems);
        inject(manager, "groupEntityService", groups); inject(manager, "groupValidator", groupValidator);
        when(problems.updateById(any(ContestProblem.class))).thenReturn(true);
        return manager;
    }

    @Test void authorizedContestCannotBeCombinedWithAnotherContestRow() {
        ContestProblemEntityService problems = mock(ContestProblemEntityService.class);
        when(problems.getById(90L)).thenReturn(new ContestProblem().setId(90L).setCid(99L).setPid(3L));
        assertThrows(StatusForbiddenException.class, () -> contestManager(problems).updateContestProblem(
                new ContestProblem().setId(90L).setCid(11L).setPid(3L).setDisplayId("A")));
        verify(problems, never()).updateById(any(ContestProblem.class));
        verify(problems, never()).saveOrUpdate(any(ContestProblem.class));
        verify(problems, never()).syncContestRecord(anyLong(), anyLong(), anyString());
    }
    @Test void contestProblemCannotBeReassignedToAnotherProblem() {
        ContestProblemEntityService problems = mock(ContestProblemEntityService.class);
        when(problems.getById(90L)).thenReturn(new ContestProblem().setId(90L).setCid(11L).setPid(3L));
        assertThrows(StatusForbiddenException.class, () -> contestManager(problems).updateContestProblem(
                new ContestProblem().setId(90L).setCid(11L).setPid(4L).setDisplayId("A")));
        verify(problems, never()).updateById(any(ContestProblem.class));
    }
    @Test void contestProblemEditWritesOnlySupportedPresentationFields() throws Exception {
        ContestProblemEntityService problems = mock(ContestProblemEntityService.class);
        when(problems.getById(90L)).thenReturn(new ContestProblem().setId(90L).setCid(11L).setPid(3L));
        contestManager(problems).updateContestProblem(new ContestProblem().setId(90L).setCid(11L).setPid(3L)
                .setDisplayId("B").setDisplayTitle("new title").setColor("#123456").setGmtCreate(new Date(0)));
        ArgumentCaptor<ContestProblem> edit = ArgumentCaptor.forClass(ContestProblem.class);
        verify(problems).updateById(edit.capture()); ContestProblem saved = edit.getValue();
        assertEquals(Long.valueOf(90), saved.getId()); assertEquals("B", saved.getDisplayId());
        assertEquals("new title", saved.getDisplayTitle()); assertEquals("#123456", saved.getColor());
        assertNull(saved.getCid()); assertNull(saved.getPid()); assertNull(saved.getGmtCreate());
        verify(problems).syncContestRecord(3L, 11L, "B");
    }
    @Test void partialContestPresentationEditSyncsPersistedDisplayId() throws Exception {
        ContestProblemEntityService problems = mock(ContestProblemEntityService.class);
        when(problems.getById(90L)).thenReturn(new ContestProblem().setId(90L).setCid(11L).setPid(3L).setDisplayId("A"));
        contestManager(problems).updateContestProblem(new ContestProblem().setId(90L).setCid(11L).setColor("#123456"));
        verify(problems).syncContestRecord(3L, 11L, "A");
    }
    @Test void missingContestRowReturnsBusinessErrorRatherThanInsertingIt() {
        ContestProblemEntityService problems = mock(ContestProblemEntityService.class);
        assertThrows(StatusNotFoundException.class, () -> contestManager(problems).updateContestProblem(
                new ContestProblem().setId(90L).setCid(11L).setPid(3L)));
        verify(problems, never()).saveOrUpdate(any(ContestProblem.class));
    }
    @Test void bannedTeamCannotBeUnbannedByItsOwnTeamAdministrator() {
        when(groups.getById(7L)).thenReturn(group(1));
        assertThrows(StatusForbiddenException.class, () -> groupManager().updateGroup(group(0)));
        verify(groups, never()).updateById(any(Group.class));
    }
    @Test void ordinaryTeamEditsPreserveModerationStatusAndOwner() throws Exception {
        groupManager().updateGroup(group(1).setUid("forged").setOwner("forged").setGmtCreate(new Date(0)));
        ArgumentCaptor<Group> edit = ArgumentCaptor.forClass(Group.class); verify(groups).updateById(edit.capture());
        assertEquals(Integer.valueOf(0), edit.getValue().getStatus()); assertEquals("owner", edit.getValue().getUid());
        assertEquals("owner", edit.getValue().getOwner()); assertNull(edit.getValue().getGmtCreate());
    }
    @Test void siteRootCanStillModerateBannedTeam() throws Exception {
        when(subject.hasRole("root")).thenReturn(true); when(groups.getById(7L)).thenReturn(group(1));
        groupManager().updateGroup(group(0)); verify(groups).updateById(any(Group.class));
    }
    @Test void newTeamsCannotImportClientPrimaryKeysOrModerationState() throws Exception {
        when(subject.hasRole("admin")).thenReturn(true);
        GroupMemberEntityService members = mock(GroupMemberEntityService.class); when(members.save(any(GroupMember.class))).thenReturn(true);
        when(groups.save(any(Group.class))).thenAnswer(inv -> { Group saved = inv.getArgument(0);
            assertNull(saved.getId()); assertEquals(Integer.valueOf(0), saved.getStatus()); assertNull(saved.getGmtCreate());
            assertEquals("operator", saved.getOwner()); assertEquals("operator", saved.getUid()); saved.setId(8L); return true;
        });
        GroupManager manager = groupManager(); inject(manager, "groupMemberEntityService", members);
        manager.addGroup(group(1).setGmtCreate(new Date(0)));
        ArgumentCaptor<GroupMember> owner = ArgumentCaptor.forClass(GroupMember.class); verify(members).save(owner.capture());
        assertEquals(Long.valueOf(8), owner.getValue().getGid()); assertEquals(Integer.valueOf(5), owner.getValue().getAuth());
    }
    @Test void failedOwnerMembershipDoesNotReportTeamCreationSuccess() {
        when(subject.hasRole("admin")).thenReturn(true); when(groups.save(any(Group.class))).thenReturn(true);
        GroupManager manager = groupManager(); inject(manager, "groupMemberEntityService", mock(GroupMemberEntityService.class));
        assertThrows(StatusFailException.class, () -> manager.addGroup(group(0)));
    }

    private TrainingEntityService trainings;
    private TrainingRegisterEntityService registers;
    private TrainingProblemEntityService trainingProblems;
    private AdminTrainingRecordManager trainingRecords;
    private ValueOperations<String, Object> values;
    private TrainingManager trainingManager() {
        TrainingManager manager = new TrainingManager(); trainings = mock(TrainingEntityService.class);
        registers = mock(TrainingRegisterEntityService.class); trainingProblems = mock(TrainingProblemEntityService.class);
        trainingRecords = mock(AdminTrainingRecordManager.class);
        @SuppressWarnings("unchecked") RedisTemplate<String, Object> template = mock(RedisTemplate.class);
        @SuppressWarnings("unchecked") ValueOperations<String, Object> mockedValues = mock(ValueOperations.class); values = mockedValues;
        when(template.opsForValue()).thenReturn(values); RedisUtils redis = new RedisUtils(); redis.setRedisTemplate(template);
        inject(manager, "trainingEntityService", trainings); inject(manager, "trainingRegisterEntityService", registers);
        inject(manager, "trainingProblemEntityService", trainingProblems); inject(manager, "redisUtils", redis);
        inject(manager, "adminTrainingRecordManager", trainingRecords); inject(manager, "groupValidator", groupValidator);
        when(trainings.getById(20L)).thenReturn(training()); return manager;
    }
    private Training training() { return new Training().setId(20L).setStatus(true).setIsGroup(false).setAuth("Private").setPrivatePwd("secret"); }
    private RegisterTrainingDTO registration(String password) { RegisterTrainingDTO request = new RegisterTrainingDTO(); request.setTid(20L); request.setPassword(password); return request; }
    @Test void incorrectTrainingPasswordConsumesRateLimitBeforeNextAttempt() {
        TrainingManager manager = trainingManager();
        when(values.setIfAbsent("training:register:operator", "1", 60L, TimeUnit.SECONDS)).thenReturn(true, false);
        assertThrows(StatusFailException.class, () -> manager.toRegisterTraining(registration("wrong")));
        assertThrows(StatusForbiddenException.class, () -> manager.toRegisterTraining(registration("secret")));
        verify(values, times(2)).setIfAbsent("training:register:operator", "1", 60L, TimeUnit.SECONDS);
        verify(registers, never()).save(any(TrainingRegister.class)); verifyNoInteractions(trainingRecords);
    }
    @Test void permittedTrainingRegistrationStillWritesAndSyncs() throws Exception {
        TrainingManager manager = trainingManager();
        when(values.setIfAbsent("training:register:operator", "1", 60L, TimeUnit.SECONDS)).thenReturn(true);
        when(registers.save(any(TrainingRegister.class))).thenReturn(true); manager.toRegisterTraining(registration("secret"));
        ArgumentCaptor<TrainingRegister> saved = ArgumentCaptor.forClass(TrainingRegister.class); verify(registers).save(saved.capture());
        assertEquals(Long.valueOf(20), saved.getValue().getTid()); assertEquals("operator", saved.getValue().getUid());
        verify(trainingRecords).syncUserSubmissionToRecordByTid(20L, "operator");
    }
    @Test void disabledTrainingRegistrationDoesNotGrantAccess() throws Exception {
        TrainingManager manager = trainingManager(); when(registers.getOne(any(Wrapper.class), eq(false)))
                .thenReturn(new TrainingRegister().setTid(20L).setUid("operator").setStatus(false));
        assertFalse(manager.getTrainingAccess(20L).getAccess());
    }
    @Test void enabledTrainingRegistrationStillGrantsAccess() throws Exception {
        TrainingManager manager = trainingManager(); when(registers.getOne(any(Wrapper.class), eq(false)))
                .thenReturn(new TrainingRegister().setTid(20L).setUid("operator").setStatus(true));
        assertTrue(manager.getTrainingAccess(20L).getAccess());
    }
    @Test void trainingWithoutPasswordReturnsBusinessError() {
        TrainingManager manager = trainingManager(); when(trainings.getById(20L)).thenReturn(training().setPrivatePwd(null));
        when(values.setIfAbsent("training:register:operator", "1", 60L, TimeUnit.SECONDS)).thenReturn(true);
        assertThrows(StatusFailException.class, () -> manager.toRegisterTraining(registration("guess")));
        verify(registers, never()).save(any(TrainingRegister.class));
    }
    @Test void missingTrainingCategoryDoesNotBreakTrainingDetail() throws Exception {
        TrainingManager manager = trainingManager(); inject(manager, "trainingCategoryEntityService", mock(TrainingCategoryEntityService.class));
        inject(manager, "trainingValidator", mock(TrainingValidator.class)); when(trainingProblems.getTrainingProblemIdList(20L)).thenReturn(Collections.emptyList());
        TrainingVO detail = manager.getTraining(20L); assertEquals(Long.valueOf(20), detail.getId());
        assertEquals(Integer.valueOf(0), detail.getProblemCount()); assertNull(detail.getCategoryName());
    }
    @Test void emptyTrainingDoesNotQueryAllOfTheUsersSubmissions() throws Exception {
        TrainingManager manager = trainingManager(); inject(manager, "trainingValidator", mock(TrainingValidator.class));
        JudgeEntityService judges = mock(JudgeEntityService.class); inject(manager, "judgeEntityService", judges);
        when(trainingProblems.getTrainingFullScreenProblemList(20L)).thenReturn(Collections.emptyList());
        assertTrue(manager.getProblemFullScreenList(20L).isEmpty()); verifyNoInteractions(judges);
    }

    private GroupDiscussionManager discussionManager(DiscussionEntityService discussions) {
        GroupDiscussionManager manager = new GroupDiscussionManager(); inject(manager, "groupEntityService", groups);
        inject(manager, "groupValidator", groupValidator); inject(manager, "discussionEntityService", discussions); return manager;
    }
    @Test void missingDiscussionDeletionReturnsNotFound() {
        DiscussionEntityService discussions = mock(DiscussionEntityService.class);
        assertThrows(StatusNotFoundException.class, () -> discussionManager(discussions).deleteDiscussion(404L));
        verify(discussions, never()).removeById(anyLong());
    }
    @Test void teamDiscussionListsCannotDisableDatabasePagination() throws Exception {
        DiscussionEntityService discussions = mock(DiscussionEntityService.class); GroupDiscussionManager manager = discussionManager(discussions);
        when(discussions.page(any(IPage.class), any(Wrapper.class))).thenAnswer(invocation -> invocation.getArgument(0));
        manager.getDiscussionList(-1, null, 7L, null); manager.getAdminDiscussionList(Integer.MAX_VALUE, -2, 7L);
        ArgumentCaptor<IPage> pages = ArgumentCaptor.forClass(IPage.class);
        verify(discussions, times(2)).page(pages.capture(), any(Wrapper.class));
        assertEquals(1L, pages.getAllValues().get(0).getCurrent()); assertEquals(10L, pages.getAllValues().get(0).getSize());
        assertEquals(1L, pages.getAllValues().get(1).getCurrent()); assertEquals(100L, pages.getAllValues().get(1).getSize());
    }
}
