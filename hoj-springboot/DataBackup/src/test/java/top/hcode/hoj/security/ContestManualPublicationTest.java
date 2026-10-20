package top.hcode.hoj.security;

import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.dao.group.GroupEntityService;
import top.hcode.hoj.manager.oj.ContestAccessPolicyManager;
import top.hcode.hoj.manager.oj.ContestReleaseManager;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.group.Group;
import top.hcode.hoj.pojo.vo.AdminContestVO;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.validator.GroupValidator;
import java.util.Date;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ContestManualPublicationTest {
    private ContestReleaseManager manager;
    private ContestEntityService contests;
    private ContestAccessPolicyManager policies;
    private GroupEntityService groups;
    private GroupValidator groupValidator;
    private Subject subject;
    private Contest contest;

    @BeforeEach void setup() {
        subject = mock(Subject.class);
        AccountProfile operator = new AccountProfile(); operator.setUid("operator");
        when(subject.getPrincipal()).thenReturn(operator);
        ThreadContext.bind(subject);
        contests = mock(ContestEntityService.class);
        policies = mock(ContestAccessPolicyManager.class);
        groups = mock(GroupEntityService.class);
        groupValidator = mock(GroupValidator.class);
        contest = new Contest().setId(7L).setUid("operator").setEndTime(new Date(0)).setIsGroup(false);
        when(contests.getByIdForUpdate(7L)).thenReturn(contest);
        when(policies.isConfidential(contest)).thenReturn(true);
        manager = new ContestReleaseManager();
        ReflectionTestUtils.setField(manager, "contestEntityService", contests);
        ReflectionTestUtils.setField(manager, "policyManager", policies);
        ReflectionTestUtils.setField(manager, "groupEntityService", groups);
        ReflectionTestUtils.setField(manager, "groupValidator", groupValidator);
    }
    @AfterEach void cleanup() { ThreadContext.unbindSubject(); }

    @Test void ownerCanReleaseEndedContestWithoutSavingOtherContestFields() throws Exception {
        Map<String, Object> result = manager.changeState(7L, true, false);
        ArgumentCaptor<AdminContestVO> input = ArgumentCaptor.forClass(AdminContestVO.class);
        verify(policies).savePolicy(eq(contest), input.capture(), eq(contest));
        assertNotNull(input.getValue().getReleaseAt());
        assertEquals(input.getValue().getReleaseAt(), result.get("releaseAt"));
        verify(contests, never()).saveOrUpdate(any(Contest.class));
    }
    @Test void sealingExplicitlyClearsTheScheduledRelease() throws Exception {
        assertNull(manager.changeState(7L, false, false).get("releaseAt"));
        ArgumentCaptor<AdminContestVO> input = ArgumentCaptor.forClass(AdminContestVO.class);
        verify(policies).savePolicy(eq(contest), input.capture(), eq(contest));
        assertNull(input.getValue().getReleaseAt());
        assertEquals(Boolean.TRUE, input.getValue().getConfidential());
    }
    @Test void ordinaryNonOwnerCannotChangePublication() {
        contest.setUid("someone-else");
        assertThrows(StatusForbiddenException.class, () -> manager.changeState(7L, true, false));
        verifyNoInteractions(policies);
    }
    @Test void rootCanOperateAnotherOwnersContest() throws Exception {
        contest.setUid("someone-else"); when(subject.hasRole("root")).thenReturn(true);
        assertNotNull(manager.changeState(7L, true, false).get("releaseAt"));
    }
    @Test void runningContestCannotBeReleased() throws Exception {
        contest.setEndTime(new Date(System.currentTimeMillis() + 3600000));
        assertThrows(StatusFailException.class, () -> manager.changeState(7L, true, false));
        verify(policies, never()).savePolicy(any(), any(), any());
    }
    @Test void nonConfidentialContestCannotBePublishedThroughThisEndpoint() {
        when(policies.isConfidential(contest)).thenReturn(false);
        assertThrows(StatusFailException.class, () -> manager.changeState(7L, true, false));
    }
    @Test void groupRootCanOperateGroupContestButOrdinaryMemberCannot() throws Exception {
        contest.setGid(9L).setUid("someone-else");
        when(groups.getById(9L)).thenReturn(new Group().setId(9L).setStatus(0));
        assertThrows(StatusForbiddenException.class, () -> manager.changeState(7L, true, true));
        when(groupValidator.isGroupRoot("operator", 9L)).thenReturn(true);
        assertNotNull(manager.changeState(7L, true, true).get("releaseAt"));
    }
    @Test void groupRouteRejectsNonGroupContestAndBannedGroup() {
        assertThrows(StatusForbiddenException.class, () -> manager.changeState(7L, false, true));
        contest.setGid(9L);
        when(groups.getById(9L)).thenReturn(new Group().setId(9L).setStatus(1));
        assertThrows(StatusForbiddenException.class, () -> manager.changeState(7L, false, true));
    }
}
