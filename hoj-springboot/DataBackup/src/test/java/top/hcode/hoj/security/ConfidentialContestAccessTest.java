package top.hcode.hoj.security;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.annotation.Transactional;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.dao.contest.*;
import top.hcode.hoj.manager.admin.contest.AdminContestManager;
import top.hcode.hoj.manager.oj.ContestAccessPolicyManager;
import top.hcode.hoj.manager.oj.ContestManager;
import top.hcode.hoj.manager.oj.ContestScoreboardManager;
import top.hcode.hoj.manager.oj.ContestRankManager;
import top.hcode.hoj.mapper.ContestProtectedResourceMapper;
import top.hcode.hoj.pojo.dto.RegisterContestDTO;
import top.hcode.hoj.pojo.dto.SubmitJudgeDTO;
import top.hcode.hoj.pojo.dto.TestJudgeDTO;
import top.hcode.hoj.pojo.dto.ContestRankDTO;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.contest.ContestAccessPolicy;
import top.hcode.hoj.pojo.entity.contest.ContestRegister;
import top.hcode.hoj.pojo.vo.AccessVO;
import top.hcode.hoj.pojo.vo.AdminContestVO;
import top.hcode.hoj.pojo.vo.ContestVO;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.utils.Constants;
import top.hcode.hoj.validator.AccessValidator;
import top.hcode.hoj.validator.CommonValidator;
import top.hcode.hoj.validator.ContestValidator;
import top.hcode.hoj.validator.GroupValidator;
import top.hcode.hoj.validator.JudgeValidator;

import java.util.Date;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ConfidentialContestAccessTest {
    private ContestAccessPolicyEntityService policies;
    private ContestAccessPolicyManager policyManager;
    private ContestProtectedResourceMapper protectedResources;
    private ContestRegisterEntityService registrations;
    private ContestValidator validator;
    private GroupValidator groupValidator;
    private Subject subject;
    private AccountProfile user;
    private Contest contest;
    private ContestAccessPolicy policy;

    private static void inject(Object target, String field, Object value) {
        ReflectionTestUtils.setField(target, field, value);
    }

    @BeforeEach void setup() {
        policies = mock(ContestAccessPolicyEntityService.class);
        policyManager = new ContestAccessPolicyManager();
        inject(policyManager, "contestAccessPolicyEntityService", policies);
        protectedResources = mock(ContestProtectedResourceMapper.class);
        inject(policyManager, "contestProtectedResourceMapper", protectedResources);
        registrations = mock(ContestRegisterEntityService.class);
        groupValidator = mock(GroupValidator.class);
        validator = new ContestValidator();
        inject(validator, "contestAccessPolicyManager", policyManager);
        inject(validator, "contestRegisterEntityService", registrations);
        inject(validator, "groupValidator", groupValidator);
        inject(validator, "commonValidator", new CommonValidator());
        user = new AccountProfile(); user.setUid("student"); user.setUsername("student");
        subject = mock(Subject.class); when(subject.getPrincipal()).thenReturn(user); ThreadContext.bind(subject);
        long now = System.currentTimeMillis();
        contest = new Contest().setId(7L).setUid("owner").setAuthor("owner").setTitle("contest")
                .setVisible(true).setIsGroup(false).setAuth(1).setPwd("password")
                .setStartTime(new Date(now - 60000)).setEndTime(new Date(now + 60000))
                .setStatus(0).setOpenAccountLimit(true).setAccountLimitRule("invalid legacy account rule")
                .setAllowEndSubmit(false).setOpenRank(false).setSealRank(false);
        policy = new ContestAccessPolicy().setCid(7L);
        when(policies.getById(7L)).thenReturn(policy);
    }

    @AfterEach void cleanup() { ThreadContext.unbindSubject(); }

    private void verified() {
        when(registrations.getOne(any(Wrapper.class), eq(false)))
                .thenReturn(new ContestRegister().setCid(7L).setUid(user.getUid()).setStatus(0));
    }

    private void ended() {
        contest.setEndTime(new Date(System.currentTimeMillis() - 10000));
    }

    @Test void liveConfidentialContestRequiresPasswordForBothReadAndSubmission() {
        assertThrows(StatusForbiddenException.class, () -> validator.validateContestAuth(contest, user, false));
        assertThrows(StatusForbiddenException.class, () -> validator.validateJudgeAuth(contest, user, false));
    }

    @Test void passwordSufficesWithoutAccountRuleOrGroupMembership() throws Exception {
        verified(); contest.setIsGroup(true).setGid(99L);
        validator.validateContestAuth(contest, user, false);
        validator.validateJudgeAuth(contest, user, false);
        verify(groupValidator, never()).isGroupMember(anyString(), anyLong());
    }

    @Test void scheduledContestDeniesAlreadyVerifiedUsersEvenWithStaleRunningStatus() {
        verified(); contest.setStartTime(new Date(System.currentTimeMillis() + 10000));
        assertThrows(StatusForbiddenException.class, () -> validator.validateContestAuth(contest, user, false));
        assertEquals("SCHEDULED", assertDoesNotThrow(() -> validator.getAccess(contest, user, false)).getAccessReason());
    }

    @Test void endTimeImmediatelySealsVerifiedUsersDespiteStaleStatusAndOpenRank() throws Exception {
        verified(); ended(); contest.setOpenRank(true).setAllowEndSubmit(true);
        assertThrows(StatusForbiddenException.class, () -> validator.validateContestAuth(contest, user, false));
        assertThrows(StatusForbiddenException.class, () -> validator.validateJudgeAuth(contest, user, false));
        AccessVO access = validator.getAccess(contest, user, false);
        assertTrue(access.getAccess()); assertFalse(access.getCanViewProblems()); assertFalse(access.getCanSubmit());
        assertEquals("SEALED", access.getAccessReason());
    }

    @Test void futureReleaseKeepsContestSealedUntilExactBoundary() {
        contest.setStartTime(new Date(100)).setEndTime(new Date(200));
        policy.setReleaseAt(new Date(300));
        assertFalse(policyManager.isReleased(contest, policy, new Date(299)));
        assertTrue(policyManager.isReleased(contest, policy, new Date(300)));
        assertEquals(Constants.Contest.STATUS_ENDED.getCode(), policyManager.currentStatus(contest, new Date(200)));
        assertEquals(Constants.Contest.STATUS_RUNNING.getCode(), policyManager.currentStatus(contest, new Date(100)));
    }

    @Test void releaseCannotMakeContestPublicBeforeEnd() {
        policy.setReleaseAt(new Date(0));
        assertFalse(policyManager.isReleased(contest));
        assertThrows(StatusForbiddenException.class, () -> validator.validateContestAuth(contest, null, false));
    }

    @Test void hiddenContestStaysEmbargoedAfterItsScheduledReleaseTime() {
        ended(); policy.setReleaseAt(new Date(0)); contest.setVisible(false);
        assertFalse(policyManager.isReleased(contest));
        assertThrows(StatusFailException.class, () -> validator.validateContestAuth(contest, null, false));
    }

    @Test void publishedContestIsAnonymousReadableAndAnyLoggedInUserMaySubmitWithoutPassword() throws Exception {
        ended(); policy.setReleaseAt(new Date(System.currentTimeMillis() - 5000));
        contest.setIsGroup(true).setGid(99L);
        validator.validateContestAuth(contest, null, false);
        validator.validateJudgeAuth(contest, user, false);
        assertThrows(StatusForbiddenException.class, () -> validator.validateJudgeAuth(contest, null, false));
        AccessVO anonymous = validator.getAccess(contest, null, false);
        assertTrue(anonymous.getReleased()); assertTrue(anonymous.getCanViewProblems()); assertFalse(anonymous.getCanSubmit());
        AccessVO loggedIn = validator.getAccess(contest, user, false);
        assertFalse(loggedIn.getAccess()); assertTrue(loggedIn.getCanSubmit()); assertEquals("RELEASED", loggedIn.getAccessReason());
    }

    @Test void resealingRevokesExistingUsersOnNextRequest() throws Exception {
        verified(); ended(); policy.setReleaseAt(new Date(0));
        validator.validateContestAuth(contest, user, false);
        policy.setReleaseAt(null);
        assertThrows(StatusForbiddenException.class, () -> validator.validateContestAuth(contest, user, false));
    }

    @Test void ownerAndRootCanInspectSealedContest() throws Exception {
        ended();
        validator.validateContestAuth(contest, user, true);
        contest.setUid(user.getUid());
        validator.validateContestAuth(contest, user, false);
        validator.validateJudgeAuth(contest, user, false);
    }

    @Test void unavailablePolicyStoreNeverFallsBackToLegacyAccess() {
        when(policies.getById(7L)).thenThrow(new DataAccessResourceFailureException("offline"));
        contest.setAuth(0);
        assertThrows(DataAccessResourceFailureException.class, () -> validator.validateContestAuth(contest, user, false));
    }

    @Test void existingPublicAndProtectedContestsKeepTheirReadSemantics() throws Exception {
        when(policies.getById(7L)).thenReturn(null);
        contest.setAuth(0).setOpenAccountLimit(false);
        validator.validateContestAuth(contest, null, false);
        contest.setAuth(2);
        validator.validateContestAuth(contest, user, false);
        assertThrows(StatusForbiddenException.class, () -> validator.validateJudgeAuth(contest, user, false));
    }

    @Test void invalidatedRegistrationDoesNotGrantPasswordAccess() {
        when(registrations.getOne(any(Wrapper.class), eq(false))).thenReturn(new ContestRegister().setStatus(1));
        assertThrows(StatusForbiddenException.class, () -> validator.validateContestAuth(contest, user, false));
    }

    @Test void openRankingCannotBypassPasswordOnProblemList() {
        contest.setOpenRank(true);
        ContestEntityService contests = mock(ContestEntityService.class);
        ContestProblemEntityService problems = mock(ContestProblemEntityService.class);
        when(contests.getById(7L)).thenReturn(contest);
        ContestManager manager = new ContestManager();
        inject(manager, "contestEntityService", contests); inject(manager, "contestValidator", validator);
        inject(manager, "contestProblemEntityService", problems);
        assertThrows(StatusForbiddenException.class, () -> manager.getContestProblem(7L, false));
        verifyNoInteractions(problems);
        when(policies.getById(7L)).thenReturn(null); // The old private-list bypass is closed too.
        assertThrows(StatusForbiddenException.class, () -> manager.getContestProblem(7L, false));
        verifyNoInteractions(problems);
    }

    @Test void openOutsideRankingCannotExposeSealedContestProblemMetadata() {
        verified(); ended(); contest.setOpenRank(true);
        ContestEntityService contests = mock(ContestEntityService.class);
        ContestProblemEntityService problems = mock(ContestProblemEntityService.class);
        when(contests.getById(7L)).thenReturn(contest);
        ContestVO info = new ContestVO(); info.setId(7L); info.setOpenRank(true); info.setStatus(1);
        when(contests.getContestInfoById(7L)).thenReturn(info);
        ContestScoreboardManager manager = new ContestScoreboardManager();
        inject(manager, "contestEntityService", contests); inject(manager, "contestValidator", validator);
        inject(manager, "contestProblemEntityService", problems);
        assertThrows(StatusForbiddenException.class, () -> manager.getContestOutsideInfo(7L));
        verifyNoInteractions(problems);
    }

    @Test void legacyAnonymousDetailQueryKeepsItsLoginRequirement() {
        when(subject.getPrincipal()).thenReturn(null);
        when(policies.getById(7L)).thenReturn(null); contest.setAuth(0);
        ContestEntityService contests = mock(ContestEntityService.class);
        ContestProblemEntityService problems = mock(ContestProblemEntityService.class);
        when(contests.getById(7L)).thenReturn(contest);
        ContestManager manager = new ContestManager();
        inject(manager, "contestEntityService", contests); inject(manager, "contestValidator", validator);
        inject(manager, "contestProblemEntityService", problems);
        assertThrows(StatusForbiddenException.class, () -> manager.getContestProblemDetails(7L, "A", false));
        assertThrows(StatusForbiddenException.class, () -> manager.getContestAccess(7L));
        verifyNoInteractions(problems);
    }

    @Test void publishedAnonymousRankingPassesTheLoginGateEvenWithClosedLegacyRankingFlag() throws Exception {
        ended(); policy.setReleaseAt(new Date(0)); contest.setType(0);
        when(subject.getPrincipal()).thenReturn(null);
        ContestEntityService contests = mock(ContestEntityService.class);
        when(contests.getById(7L)).thenReturn(contest);
        ContestRankManager ranks = mock(ContestRankManager.class);
        ContestManager manager = new ContestManager();
        inject(manager, "contestEntityService", contests); inject(manager, "contestValidator", validator);
        inject(manager, "contestRankManager", ranks);
        ContestRankDTO request = new ContestRankDTO(); request.setCid(7L);
        manager.getContestRank(request);
        verify(ranks).getContestACMRankPage(eq(false), eq(false), isNull(), anyList(), isNull(),
                eq(contest), anyInt(), anyInt(), isNull(), eq(false));
    }

    @Test void checkingRevokedLegacyAccountDoesNotDeleteUnrelatedContestRecords() throws Exception {
        verified(); when(policies.getById(7L)).thenReturn(null);
        contest.setAccountLimitRule("<prefix>x</prefix><suffix></suffix><start>1</start><end>1</end><extra></extra>");
        ContestEntityService contests = mock(ContestEntityService.class);
        ContestRecordEntityService records = mock(ContestRecordEntityService.class);
        when(contests.getById(7L)).thenReturn(contest);
        ContestManager manager = new ContestManager();
        inject(manager, "contestEntityService", contests); inject(manager, "contestValidator", validator);
        inject(manager, "contestRecordEntityService", records);
        assertFalse(manager.getContestAccess(7L).getCanViewProblems());
        verifyNoInteractions(records);
        verify(registrations, never()).removeById(anyLong());
    }

    @Test void publicationDecoratesLegacyCompatibleFieldsWithoutChangingContestRow() {
        ended(); policy.setReleaseAt(new Date(0));
        ContestVO info = new ContestVO(); info.setId(7L); info.setAuth(1); info.setStatus(0);
        info.setStartTime(contest.getStartTime()); info.setEndTime(contest.getEndTime());
        info.setOpenRank(false); info.setAllowEndSubmit(false);
        policyManager.decorate(info);
        assertTrue(info.getReleased()); assertTrue(info.getOpenRank()); assertTrue(info.getAllowEndSubmit());
        assertEquals(Integer.valueOf(1), info.getAuth()); assertEquals(Integer.valueOf(1), info.getStatus());
        assertFalse(contest.getOpenRank()); assertFalse(contest.getAllowEndSubmit());
    }

    @Test void oldAdminPageCannotRemovePolicyAndReschedulingResealsIt() throws Exception {
        AdminContestVO omitted = new AdminContestVO();
        Contest old = new Contest().setId(7L).setStartTime(new Date(0)).setEndTime(new Date(1));
        when(policies.update(any(Wrapper.class))).thenReturn(true);
        policyManager.prepare(contest, omitted, old);
        assertEquals(Integer.valueOf(1), contest.getAuth()); assertFalse(contest.getOpenAccountLimit());
        policyManager.savePolicy(contest, omitted, old);
        verify(policies).update(any(Wrapper.class)); verify(policies, never()).removeById(anyLong());
    }

    @Test void earlyReleaseTimeIsRejectedBeforeSaving() {
        AdminContestVO input = new AdminContestVO(); input.setConfidential(true); input.setReleaseAt(new Date(0));
        assertThrows(StatusFailException.class, () -> policyManager.prepare(contest, input, null));
        verify(policies, never()).save(any());
    }

    @Test void contestPageDecoratesAllPoliciesWithOneQuery() {
        ContestVO confidential = new ContestVO(); confidential.setId(7L); confidential.setStatus(0);
        confidential.setStartTime(contest.getStartTime()); confidential.setEndTime(contest.getEndTime());
        ContestVO legacy = new ContestVO(); legacy.setId(8L); legacy.setStatus(0);
        when(policies.list(any(Wrapper.class))).thenReturn(Collections.singletonList(policy));
        policyManager.decorateAll(Arrays.asList(confidential, legacy));
        assertTrue(confidential.getConfidential()); assertFalse(legacy.getConfidential());
        verify(policies, times(1)).list(any(Wrapper.class)); verify(policies, never()).getById(anyLong());
    }

    @Test void deletionRemovesPolicyBeforeCleaningRememberedResourceReferences() throws Exception {
        when(policies.removeById(7L)).thenReturn(true);
        when(protectedResources.countByContestId(7L)).thenReturn(2L);
        when(protectedResources.deleteByContestId(7L)).thenReturn(2);
        policyManager.deletePolicy(7L);
        InOrder order = inOrder(policies, protectedResources);
        order.verify(policies).removeById(7L);
        order.verify(protectedResources).countByContestId(7L);
        order.verify(protectedResources).deleteByContestId(7L);
    }

    @Test void partialResourceCleanupFailsTheContestDeletionTransaction() {
        when(policies.removeById(7L)).thenReturn(true);
        when(protectedResources.countByContestId(7L)).thenReturn(2L);
        when(protectedResources.deleteByContestId(7L)).thenReturn(1);
        assertThrows(StatusFailException.class, () -> policyManager.deletePolicy(7L));
    }

    @Test void registrationLocksAuthoritativePasswordAndIgnoresLegacyWhitelist() throws Exception {
        ContestEntityService contests = mock(ContestEntityService.class);
        ContestManager manager = new ContestManager();
        inject(manager, "contestEntityService", contests); inject(manager, "contestValidator", validator);
        inject(manager, "contestRegisterEntityService", registrations); inject(manager, "contestAccessPolicyManager", policyManager);
        when(contests.getByIdForUpdate(7L)).thenReturn(contest);
        when(registrations.saveOrUpdate(any(ContestRegister.class))).thenReturn(true);
        RegisterContestDTO input = new RegisterContestDTO(); input.setCid(7L); input.setPassword("password");
        manager.toRegisterContest(input);
        InOrder order = inOrder(contests, registrations);
        order.verify(contests).getByIdForUpdate(7L); order.verify(registrations).getOne(any(Wrapper.class), eq(false));
        order.verify(registrations).saveOrUpdate(any(ContestRegister.class));
        assertNotNull(ContestManager.class.getMethod("toRegisterContest", RegisterContestDTO.class).getAnnotation(Transactional.class));
        assertNotNull(AdminContestManager.class.getMethod("updateContest", AdminContestVO.class).getAnnotation(Transactional.class));
    }

    @Test void changingPasswordClearsRegistrationsInsideAdminSave() throws Exception {
        when(subject.hasRole("root")).thenReturn(true);
        ContestEntityService contests = mock(ContestEntityService.class);
        ContestAccessPolicyManager policyMock = mock(ContestAccessPolicyManager.class);
        AdminContestManager manager = new AdminContestManager();
        inject(manager, "contestEntityService", contests); inject(manager, "contestValidator", mock(ContestValidator.class));
        inject(manager, "contestRegisterEntityService", registrations); inject(manager, "contestAccessPolicyManager", policyMock);
        when(contests.getByIdForUpdate(7L)).thenReturn(contest);
        when(contests.saveOrUpdate(any(Contest.class))).thenReturn(true);
        AdminContestVO input = new AdminContestVO(); input.setId(7L); input.setAuth(1); input.setPwd("new-password");
        manager.updateContest(input);
        InOrder order = inOrder(contests, registrations, policyMock);
        order.verify(contests).getByIdForUpdate(7L);
        order.verify(policyMock).prepare(any(Contest.class), eq(input), eq(contest));
        order.verify(contests).saveOrUpdate(any(Contest.class));
        order.verify(registrations).remove(any(Wrapper.class));
        order.verify(policyMock).savePolicy(any(Contest.class), eq(input), eq(contest));
    }

    @Test void shortCodeIsAllowedButWhitespaceAndOversizedCodeRemainRejected() throws Exception {
        JudgeValidator judgeValidator = new JudgeValidator();
        inject(judgeValidator, "accessValidator", mock(AccessValidator.class));
        SubmitJudgeDTO input = new SubmitJudgeDTO().setCode("int main(){}").setLanguage("C++").setIsRemote(false);
        judgeValidator.validateSubmissionInfo(input);
        input.setCode("  \n\t"); assertThrows(StatusFailException.class, () -> judgeValidator.validateSubmissionInfo(input));
        input.setCode(new String(new char[65536]).replace('\0', 'x'));
        assertThrows(StatusFailException.class, () -> judgeValidator.validateSubmissionInfo(input));
        TestJudgeDTO debug = new TestJudgeDTO().setType("public").setPid(1L).setLanguage("C++")
                .setCode("int main(){}").setUserInput("1");
        judgeValidator.validateTestJudgeInfo(debug);
    }
}
