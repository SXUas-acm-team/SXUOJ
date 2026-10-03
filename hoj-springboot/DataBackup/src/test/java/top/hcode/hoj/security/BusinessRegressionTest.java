package top.hcode.hoj.security;

import com.baomidou.mybatisplus.core.conditions.AbstractWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.context.ApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.dao.common.AnnouncementEntityService;
import top.hcode.hoj.dao.contest.ContestAnnouncementEntityService;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.dao.group.GroupEntityService;
import top.hcode.hoj.dao.group.GroupMemberEntityService;
import top.hcode.hoj.dao.msg.MsgRemindEntityService;
import top.hcode.hoj.dao.msg.UserSysNoticeEntityService;
import top.hcode.hoj.dao.training.TrainingEntityService;
import top.hcode.hoj.dao.training.TrainingProblemEntityService;
import top.hcode.hoj.dao.training.TrainingCategoryEntityService;
import top.hcode.hoj.dao.training.MappingTrainingCategoryEntityService;
import top.hcode.hoj.manager.group.announcement.GroupAnnouncementManager;
import top.hcode.hoj.manager.group.contest.GroupContestAnnouncementManager;
import top.hcode.hoj.manager.group.contest.GroupContestManager;
import top.hcode.hoj.manager.group.member.GroupMemberManager;
import top.hcode.hoj.manager.group.training.GroupTrainingProblemManager;
import top.hcode.hoj.manager.group.training.GroupTrainingManager;
import top.hcode.hoj.manager.msg.NoticeManager;
import top.hcode.hoj.manager.msg.UserMessageManager;
import top.hcode.hoj.pojo.dto.AnnouncementDTO;
import top.hcode.hoj.pojo.dto.TrainingDTO;
import top.hcode.hoj.pojo.entity.common.Announcement;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.group.Group;
import top.hcode.hoj.pojo.entity.group.GroupMember;
import top.hcode.hoj.pojo.entity.training.Training;
import top.hcode.hoj.pojo.entity.training.TrainingProblem;
import top.hcode.hoj.pojo.entity.training.TrainingCategory;
import top.hcode.hoj.pojo.entity.training.MappingTrainingCategory;
import top.hcode.hoj.pojo.vo.AdminContestVO;
import top.hcode.hoj.pojo.vo.SysMsgVO;
import top.hcode.hoj.pojo.vo.UserMsgVO;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.validator.CommonValidator;
import top.hcode.hoj.validator.GroupValidator;
import top.hcode.hoj.validator.ContestValidator;
import top.hcode.hoj.validator.TrainingValidator;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** All services are mocks; these cases must never contact development data or mail. */
class BusinessRegressionTest {
    private GroupEntityService groups;
    private GroupValidator validator;

    @BeforeEach void setup() {
        AccountProfile user = new AccountProfile();
        user.setUid("operator");
        user.setUsername("operator");
        Subject subject = mock(Subject.class);
        when(subject.getPrincipal()).thenReturn(user);
        ThreadContext.bind(subject);
        groups = mock(GroupEntityService.class);
        validator = mock(GroupValidator.class);
        when(groups.getById(7L)).thenReturn(new Group().setId(7L).setUid("owner").setStatus(0)
                .setName("test group").setVisible(true).setAuth(1));
    }

    @AfterEach void cleanup() { ThreadContext.unbindSubject(); }

    private static void inject(Object target, String name, Object value) {
        ReflectionTestUtils.setField(target, name, value);
    }

    private GroupMemberManager memberManager(GroupMemberEntityService members, int targetRole) {
        GroupMemberManager manager = new GroupMemberManager();
        inject(manager, "groupEntityService", groups);
        inject(manager, "groupMemberEntityService", members);
        when(members.getOne(any())).thenReturn(new GroupMember().setId(1L).setGid(7L).setUid("operator").setAuth(4),
                new GroupMember().setId(2L).setGid(7L).setUid("applicant").setAuth(targetRole));
        when(members.updateById(any(GroupMember.class))).thenReturn(true);
        return manager;
    }

    private GroupMember memberRequest(int auth) {
        return new GroupMember().setId(2L).setGid(7L).setUid("applicant").setAuth(auth);
    }

    @Test void authorizedMembershipCannotBeCombinedWithAnotherRecordId() {
        GroupMemberEntityService members = mock(GroupMemberEntityService.class);
        GroupMemberManager manager = memberManager(members, 1);
        assertThrows(StatusForbiddenException.class, () -> manager.updateMember(memberRequest(3).setId(900L)));
        verify(members, never()).updateById(any(GroupMember.class));
    }

    @Test void membershipUpdateOnlyWritesRoleToTheStoredRecord() throws Exception {
        GroupMemberEntityService members = mock(GroupMemberEntityService.class);
        GroupMemberManager manager = memberManager(members, 1);
        manager.updateMember(memberRequest(3).setReason("tampered reason").setGmtCreate(new Date(0)));
        ArgumentCaptor<GroupMember> update = ArgumentCaptor.forClass(GroupMember.class);
        verify(members).updateById(update.capture());
        assertEquals(Long.valueOf(2), update.getValue().getId());
        assertEquals(Integer.valueOf(3), update.getValue().getAuth());
        assertNull(update.getValue().getUid());
        assertNull(update.getValue().getGid());
        assertNull(update.getValue().getReason());
        assertNull(update.getValue().getGmtCreate());
        verify(members).addWelcomeNoticeToGroupNewMember(7L, "test group", "applicant");
    }

    @Test void invalidMembershipRoleIsRejectedBeforeAnyWrite() {
        GroupMemberEntityService members = mock(GroupMemberEntityService.class);
        GroupMemberManager manager = memberManager(members, 1);
        for (Integer role : new Integer[]{null, 0, -1, 6, Integer.MAX_VALUE}) {
            assertThrows(StatusFailException.class, () -> manager.updateMember(memberRequest(1).setAuth(role)));
        }
        verify(members, never()).updateById(any(GroupMember.class));
    }

    @Test void rejectingAnApplicationDoesNotSendAWelcomeMessage() throws Exception {
        GroupMemberEntityService members = mock(GroupMemberEntityService.class);
        memberManager(members, 1).updateMember(memberRequest(2));
        verify(members).updateById(any(GroupMember.class));
        verify(members, never()).addWelcomeNoticeToGroupNewMember(anyLong(), anyString(), anyString());
    }

    @Test void omittedPrivateGroupInvitationCodeIsABusinessError() {
        GroupMemberEntityService members = mock(GroupMemberEntityService.class);
        GroupMemberManager manager = memberManager(members, 1);
        when(groups.getById(7L)).thenReturn(new Group().setId(7L).setStatus(0).setVisible(true)
                .setAuth(3).setCode("123456"));
        assertThrows(StatusFailException.class, () -> manager.addMember(7L, null, "application"));
        verify(members, never()).save(any(GroupMember.class));
    }

    private GroupTrainingProblemManager trainingProblemManager(TrainingProblemEntityService problems) {
        GroupTrainingProblemManager manager = new GroupTrainingProblemManager();
        TrainingEntityService trainings = mock(TrainingEntityService.class);
        inject(manager, "trainingProblemEntityService", problems);
        inject(manager, "trainingEntityService", trainings);
        inject(manager, "groupEntityService", groups);
        inject(manager, "groupValidator", validator);
        when(trainings.getById(10L)).thenReturn(new Training().setId(10L).setGid(7L).setAuthor("operator"));
        when(problems.getById(2L)).thenReturn(new TrainingProblem().setId(2L).setTid(10L).setPid(20L));
        when(problems.updateById(any(TrainingProblem.class))).thenReturn(true);
        return manager;
    }

    @Test void trainingProblemCannotBeMovedToAnAuthorizedTraining() {
        TrainingProblemEntityService problems = mock(TrainingProblemEntityService.class);
        GroupTrainingProblemManager manager = trainingProblemManager(problems);
        when(problems.getById(2L)).thenReturn(new TrainingProblem().setId(2L).setTid(99L).setPid(20L));
        assertThrows(StatusForbiddenException.class,
                () -> manager.updateTrainingProblem(new TrainingProblem().setId(2L).setTid(10L).setRank(1)));
        verify(problems, never()).updateById(any(TrainingProblem.class));
    }

    @Test void trainingProblemCannotBeReplacedWithAnUncheckedProblem() {
        TrainingProblemEntityService problems = mock(TrainingProblemEntityService.class);
        GroupTrainingProblemManager manager = trainingProblemManager(problems);
        assertThrows(StatusForbiddenException.class,
                () -> manager.updateTrainingProblem(new TrainingProblem().setId(2L).setTid(10L).setPid(999L)));
        verify(problems, never()).updateById(any(TrainingProblem.class));
    }

    @Test void trainingProblemDisplayChangesPreserveItsRelationship() throws Exception {
        TrainingProblemEntityService problems = mock(TrainingProblemEntityService.class);
        trainingProblemManager(problems).updateTrainingProblem(new TrainingProblem().setId(2L).setTid(10L)
                .setPid(20L).setRank(4).setDisplayId("A").setGmtCreate(new Date(0)));
        ArgumentCaptor<TrainingProblem> update = ArgumentCaptor.forClass(TrainingProblem.class);
        verify(problems).updateById(update.capture());
        assertEquals(Long.valueOf(2), update.getValue().getId());
        assertEquals(Integer.valueOf(4), update.getValue().getRank());
        assertEquals("A", update.getValue().getDisplayId());
        assertNull(update.getValue().getTid());
        assertNull(update.getValue().getPid());
        assertNull(update.getValue().getGmtCreate());
    }

    private GroupAnnouncementManager groupAnnouncementManager(AnnouncementEntityService announcements) {
        GroupAnnouncementManager manager = new GroupAnnouncementManager();
        inject(manager, "announcementEntityService", announcements);
        inject(manager, "groupEntityService", groups);
        inject(manager, "groupValidator", validator);
        inject(manager, "commonValidator", new CommonValidator());
        when(validator.isGroupRoot("operator", 7L)).thenReturn(true);
        when(validator.isGroupAdmin("operator", 7L)).thenReturn(true);
        when(announcements.updateById(any(Announcement.class))).thenReturn(true);
        when(announcements.save(any(Announcement.class))).thenReturn(true);
        return manager;
    }

    private Announcement announcement() {
        return new Announcement().setId(2L).setGid(7L).setTitle("title").setContent("content").setStatus(0);
    }

    @Test void forgedGroupIdCannotAuthorizeEditingAnotherTeamsAnnouncement() {
        AnnouncementEntityService announcements = mock(AnnouncementEntityService.class);
        GroupAnnouncementManager manager = groupAnnouncementManager(announcements);
        when(announcements.getById(2L)).thenReturn(announcement().setGid(8L).setUid("victim"));
        assertThrows(StatusForbiddenException.class, () -> manager.updateAnnouncement(announcement()));
        verify(announcements, never()).updateById(any(Announcement.class));
    }

    @Test void announcementEditsCannotRewriteAuthorOrCreationDate() throws Exception {
        AnnouncementEntityService announcements = mock(AnnouncementEntityService.class);
        GroupAnnouncementManager manager = groupAnnouncementManager(announcements);
        when(announcements.getById(2L)).thenReturn(announcement().setUid("original-author"));
        manager.updateAnnouncement(announcement().setUid("fake-author").setGmtCreate(new Date(0)));
        ArgumentCaptor<Announcement> update = ArgumentCaptor.forClass(Announcement.class);
        verify(announcements).updateById(update.capture());
        assertEquals("title", update.getValue().getTitle());
        assertEquals("content", update.getValue().getContent());
        assertNull(update.getValue().getUid());
        assertNull(update.getValue().getGid());
        assertNull(update.getValue().getGmtCreate());
    }

    @Test void newGroupAnnouncementUsesTheLoggedInAuthorAndGeneratedId() throws Exception {
        AnnouncementEntityService announcements = mock(AnnouncementEntityService.class);
        groupAnnouncementManager(announcements).addAnnouncement(announcement().setUid("fake-author").setGmtCreate(new Date(0)));
        ArgumentCaptor<Announcement> saved = ArgumentCaptor.forClass(Announcement.class);
        verify(announcements).save(saved.capture());
        assertNull(saved.getValue().getId());
        assertEquals("operator", saved.getValue().getUid());
        assertNull(saved.getValue().getGmtCreate());
    }

    private GroupContestAnnouncementManager contestAnnouncementManager(AnnouncementEntityService announcements,
                                                                         ContestAnnouncementEntityService relations) {
        GroupContestAnnouncementManager manager = new GroupContestAnnouncementManager();
        ContestEntityService contests = mock(ContestEntityService.class);
        inject(manager, "contestEntityService", contests);
        inject(manager, "announcementEntityService", announcements);
        inject(manager, "contestAnnouncementEntityService", relations);
        inject(manager, "groupEntityService", groups);
        inject(manager, "groupValidator", validator);
        inject(manager, "commonValidator", new CommonValidator());
        when(contests.getById(10L)).thenReturn(new Contest().setId(10L).setGid(7L).setUid("operator"));
        when(announcements.getById(2L)).thenReturn(announcement().setUid("original-author"));
        when(announcements.updateById(any(Announcement.class))).thenReturn(true);
        return manager;
    }

    private AnnouncementDTO contestAnnouncement() {
        AnnouncementDTO dto = new AnnouncementDTO();
        dto.setCid(10L);
        dto.setAnnouncement(announcement());
        return dto;
    }

    @Test void contestOwnerCannotEditAnAnnouncementOutsideThatContest() {
        AnnouncementEntityService announcements = mock(AnnouncementEntityService.class);
        GroupContestAnnouncementManager manager = contestAnnouncementManager(announcements, mock(ContestAnnouncementEntityService.class));
        assertThrows(StatusForbiddenException.class, () -> manager.updateContestAnnouncement(contestAnnouncement()));
        verify(announcements, never()).updateById(any(Announcement.class));
    }

    @Test void contestOwnerCannotDeleteAnAnnouncementOutsideThatContest() {
        AnnouncementEntityService announcements = mock(AnnouncementEntityService.class);
        GroupContestAnnouncementManager manager = contestAnnouncementManager(announcements, mock(ContestAnnouncementEntityService.class));
        assertThrows(StatusForbiddenException.class, () -> manager.deleteContestAnnouncement(2L, 10L));
        verify(announcements, never()).removeById(anyLong());
    }

    @Test void validContestAnnouncementEditChecksBothRelationKeysAndPreservesOwnership() throws Exception {
        AnnouncementEntityService announcements = mock(AnnouncementEntityService.class);
        ContestAnnouncementEntityService relations = mock(ContestAnnouncementEntityService.class);
        GroupContestAnnouncementManager manager = contestAnnouncementManager(announcements, relations);
        when(relations.count(any())).thenReturn(1);
        AnnouncementDTO request = contestAnnouncement();
        request.getAnnouncement().setGid(99L).setUid("fake-author");
        manager.updateContestAnnouncement(request);
        ArgumentCaptor<Announcement> update = ArgumentCaptor.forClass(Announcement.class);
        verify(announcements).updateById(update.capture());
        assertNull(update.getValue().getGid());
        assertNull(update.getValue().getUid());
        ArgumentCaptor<AbstractWrapper> relation = ArgumentCaptor.forClass(AbstractWrapper.class);
        verify(relations).count(relation.capture());
        assertTrue(relation.getValue().getSqlSegment().contains("aid"));
        assertTrue(relation.getValue().getSqlSegment().contains("cid"));
        assertTrue(relation.getValue().getParamNameValuePairs().containsValue(2L));
        assertTrue(relation.getValue().getParamNameValuePairs().containsValue(10L));
    }

    @Test void clearingEachMessageTabOnlyTargetsThatCategoryAndRecipient() throws Exception {
        for (String type : new String[]{"Like", "Discuss", "Reply", "Sys", "Mine"}) {
            UserMessageManager manager = new UserMessageManager();
            MsgRemindEntityService reminders = mock(MsgRemindEntityService.class);
            UserSysNoticeEntityService notices = mock(UserSysNoticeEntityService.class);
            inject(manager, "msgRemindEntityService", reminders);
            inject(manager, "userSysNoticeEntityService", notices);
            when(reminders.remove(any())).thenReturn(true);
            when(notices.remove(any())).thenReturn(true);
            manager.cleanMsg(type, null);
            ArgumentCaptor<UpdateWrapper> deletion = ArgumentCaptor.forClass(UpdateWrapper.class);
            if ("Sys".equals(type) || "Mine".equals(type)) {
                verify(notices).remove(deletion.capture());
                verifyNoInteractions(reminders);
            } else {
                verify(reminders).remove(deletion.capture());
                verifyNoInteractions(notices);
            }
            String sql = deletion.getValue().getSqlSegment();
            assertTrue(sql.contains("recipient_id"));
            assertTrue(deletion.getValue().getParamNameValuePairs().containsValue("operator"));
            if ("Like".equals(type)) {
                assertTrue(sql.contains("action IN"));
                assertTrue(deletion.getValue().getParamNameValuePairs().containsValue("Like_Post"));
                assertTrue(deletion.getValue().getParamNameValuePairs().containsValue("Like_Discuss"));
            } else {
                assertTrue(sql.contains("Sys".equals(type) || "Mine".equals(type) ? "type =" : "action ="));
                assertTrue(deletion.getValue().getParamNameValuePairs().containsValue(type));
            }
        }
    }

    @Test void deletingASingleMessageKeepsTheRecipientCategoryAndIdFilters() throws Exception {
        UserMessageManager manager = new UserMessageManager();
        MsgRemindEntityService reminders = mock(MsgRemindEntityService.class);
        inject(manager, "msgRemindEntityService", reminders);
        when(reminders.remove(any())).thenReturn(true);
        manager.cleanMsg("Reply", 88L);
        ArgumentCaptor<UpdateWrapper> deletion = ArgumentCaptor.forClass(UpdateWrapper.class);
        verify(reminders).remove(deletion.capture());
        assertTrue(deletion.getValue().getSqlSegment().contains("id ="));
        assertTrue(deletion.getValue().getParamNameValuePairs().containsValue(88L));
        assertTrue(deletion.getValue().getParamNameValuePairs().containsValue("operator"));
        assertTrue(deletion.getValue().getParamNameValuePairs().containsValue("Reply"));
    }

    @Test void unknownMessageCategoryDoesNotDeleteAnyRecords() {
        UserMessageManager manager = new UserMessageManager();
        MsgRemindEntityService reminders = mock(MsgRemindEntityService.class);
        UserSysNoticeEntityService notices = mock(UserSysNoticeEntityService.class);
        inject(manager, "msgRemindEntityService", reminders);
        inject(manager, "userSysNoticeEntityService", notices);
        assertThrows(StatusFailException.class, () -> manager.cleanMsg(null, null));
        assertThrows(StatusFailException.class, () -> manager.cleanMsg("all", null));
        verifyNoInteractions(reminders, notices);
    }

    @Test void messageListsClampPageAndSizeBeforeQuerying() {
        UserMessageManager manager = new UserMessageManager();
        MsgRemindEntityService reminders = mock(MsgRemindEntityService.class);
        inject(manager, "msgRemindEntityService", reminders);
        when(reminders.getUserMsg(any(), anyString(), anyString())).thenAnswer(invocation -> {
            Page<UserMsgVO> page = invocation.getArgument(0);
            assertEquals(100, page.getSize());
            assertEquals(1000000, page.getCurrent());
            return page;
        });
        manager.getCommentMsg(Integer.MAX_VALUE, Integer.MAX_VALUE);
        manager.getReplyMsg(Integer.MAX_VALUE, Integer.MAX_VALUE);
        manager.getLikeMsg(Integer.MAX_VALUE, Integer.MAX_VALUE);
        verify(reminders, times(3)).getUserMsg(any(), eq("operator"), anyString());
    }

    @Test void noticeListsClampPageAndSizeBeforeQuerying() {
        NoticeManager manager = new NoticeManager();
        UserSysNoticeEntityService notices = mock(UserSysNoticeEntityService.class);
        ApplicationContext context = mock(ApplicationContext.class);
        inject(manager, "userSysNoticeEntityService", notices);
        inject(manager, "applicationContext", context);
        when(context.getBean(NoticeManager.class)).thenReturn(mock(NoticeManager.class));
        when(notices.getSysNotice(anyInt(), anyInt(), anyString())).thenReturn(new Page<SysMsgVO>());
        when(notices.getMineNotice(anyInt(), anyInt(), anyString())).thenReturn(new Page<SysMsgVO>());
        manager.getSysNotice(Integer.MAX_VALUE, Integer.MAX_VALUE);
        manager.getMineNotice(Integer.MAX_VALUE, Integer.MAX_VALUE);
        verify(notices).getSysNotice(100, 1000000, "operator");
        verify(notices).getMineNotice(100, 1000000, "operator");
    }

    private GroupContestManager groupContestManager(ContestEntityService contests) {
        GroupContestManager manager = new GroupContestManager();
        inject(manager, "contestEntityService", contests);
        inject(manager, "groupEntityService", groups);
        inject(manager, "groupValidator", validator);
        inject(manager, "contestValidator", mock(ContestValidator.class));
        when(validator.isGroupAdmin("operator", 7L)).thenReturn(true);
        when(contests.save(any(Contest.class))).thenReturn(true);
        when(contests.saveOrUpdate(any(Contest.class))).thenReturn(true);
        return manager;
    }

    private AdminContestVO contestRequest() {
        AdminContestVO request = new AdminContestVO();
        request.setId(10L);
        request.setUid("forged-uid");
        request.setAuthor("forged-author");
        request.setGid(7L);
        request.setAuth(0);
        return request;
    }

    @Test void groupContestEditsCannotMoveContestOrChangeItsOwner() throws Exception {
        ContestEntityService contests = mock(ContestEntityService.class);
        GroupContestManager manager = groupContestManager(contests);
        when(contests.getById(10L)).thenReturn(new Contest().setId(10L).setUid("operator").setAuthor("operator")
                .setGid(7L).setIsGroup(true).setAuth(0));
        AdminContestVO request = contestRequest();
        request.setGid(999L);
        manager.updateContest(request);
        ArgumentCaptor<Contest> update = ArgumentCaptor.forClass(Contest.class);
        verify(contests).saveOrUpdate(update.capture());
        assertEquals(Long.valueOf(7), update.getValue().getGid());
        assertEquals("operator", update.getValue().getUid());
        assertEquals("operator", update.getValue().getAuthor());
    }

    @Test void newGroupContestUsesTheAuthenticatedOwner() throws Exception {
        ContestEntityService contests = mock(ContestEntityService.class);
        groupContestManager(contests).addContest(contestRequest());
        ArgumentCaptor<Contest> saved = ArgumentCaptor.forClass(Contest.class);
        verify(contests).save(saved.capture());
        assertNull(saved.getValue().getId());
        assertTrue(saved.getValue().getIsGroup());
        assertEquals("operator", saved.getValue().getUid());
        assertEquals("operator", saved.getValue().getAuthor());
    }

    private GroupTrainingManager groupTrainingManager(TrainingEntityService trainings,
                                                       TrainingCategoryEntityService categories) {
        GroupTrainingManager manager = new GroupTrainingManager();
        MappingTrainingCategoryEntityService mappings = mock(MappingTrainingCategoryEntityService.class);
        inject(manager, "trainingEntityService", trainings);
        inject(manager, "trainingCategoryEntityService", categories);
        inject(manager, "mappingTrainingCategoryEntityService", mappings);
        inject(manager, "groupEntityService", groups);
        inject(manager, "groupValidator", validator);
        inject(manager, "trainingValidator", mock(TrainingValidator.class));
        when(validator.isGroupAdmin("operator", 7L)).thenReturn(true);
        when(trainings.getById(10L)).thenReturn(new Training().setId(10L).setAuthor("operator")
                .setGid(7L).setIsGroup(true).setAuth("Public"));
        when(mappings.getOne(any(), eq(false))).thenReturn(new MappingTrainingCategory().setTid(10L).setCid(5L));
        when(mappings.save(any(MappingTrainingCategory.class))).thenReturn(true);
        when(categories.getById(5L)).thenReturn(new TrainingCategory().setId(5L));
        when(trainings.save(any(Training.class))).thenAnswer(invocation -> {
            Training saved = invocation.getArgument(0);
            assertNull(saved.getId());
            saved.setId(10L);
            return true;
        });
        return manager;
    }

    private TrainingDTO trainingRequest() {
        return new TrainingDTO().setTraining(new Training().setId(10L).setGid(7L).setIsGroup(false)
                .setAuthor("forged-author").setTitle("training").setAuth("Public"))
                .setTrainingCategory(new TrainingCategory().setId(5L));
    }

    @Test void groupTrainingEditsPreserveOwnerAndGroupAndAllowPublicCategories() throws Exception {
        TrainingEntityService trainings = mock(TrainingEntityService.class);
        GroupTrainingManager manager = groupTrainingManager(trainings, mock(TrainingCategoryEntityService.class));
        TrainingDTO request = trainingRequest();
        request.getTraining().setGid(999L);
        manager.updateTraining(request);
        ArgumentCaptor<Training> update = ArgumentCaptor.forClass(Training.class);
        verify(trainings).updateById(update.capture());
        assertEquals(Long.valueOf(7), update.getValue().getGid());
        assertTrue(update.getValue().getIsGroup());
        assertEquals("operator", update.getValue().getAuthor());
    }

    @Test void newGroupTrainingUsesTheAuthenticatedAuthor() throws Exception {
        TrainingEntityService trainings = mock(TrainingEntityService.class);
        groupTrainingManager(trainings, mock(TrainingCategoryEntityService.class)).addTraining(trainingRequest());
        ArgumentCaptor<Training> saved = ArgumentCaptor.forClass(Training.class);
        verify(trainings).save(saved.capture());
        assertTrue(saved.getValue().getIsGroup());
        assertEquals("operator", saved.getValue().getAuthor());
    }

    @Test void anotherTeamsCategoryCannotBeSelectedByOmittingItsGroupId() {
        TrainingEntityService trainings = mock(TrainingEntityService.class);
        TrainingCategoryEntityService categories = mock(TrainingCategoryEntityService.class);
        GroupTrainingManager manager = groupTrainingManager(trainings, categories);
        when(categories.getById(5L)).thenReturn(new TrainingCategory().setId(5L).setGid(8L));
        assertThrows(StatusForbiddenException.class, () -> manager.updateTraining(trainingRequest()));
        assertThrows(StatusForbiddenException.class, () -> manager.addTraining(trainingRequest()));
        verify(trainings, never()).updateById(any(Training.class));
        verify(trainings, never()).save(any(Training.class));
    }
}
