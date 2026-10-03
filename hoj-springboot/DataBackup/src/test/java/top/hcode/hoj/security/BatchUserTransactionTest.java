package top.hcode.hoj.security;

import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.support.*;
import org.springframework.transaction.TransactionDefinition;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.dao.user.*;
import top.hcode.hoj.manager.admin.user.AdminUserManager;
import top.hcode.hoj.manager.admin.contest.AdminContestProblemManager;
import top.hcode.hoj.manager.admin.problem.RemoteProblemManager;
import top.hcode.hoj.service.admin.contest.impl.AdminContestProblemServiceImpl;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.dao.contest.ContestProblemEntityService;
import top.hcode.hoj.dao.problem.ProblemEntityService;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.problem.Problem;
import top.hcode.hoj.crawler.problem.ProblemStrategy;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.manager.msg.AdminNoticeManager;
import top.hcode.hoj.pojo.entity.user.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class BatchUserTransactionTest {
    static class TestTransactions extends AbstractPlatformTransactionManager {
        int commits, rollbacks;
        String stagedName;
        List<String> committedNames = new ArrayList<>();
        @Override protected Object doGetTransaction() { return new Object(); }
        @Override protected void doBegin(Object transaction, TransactionDefinition definition) { stagedName = null; }
        @Override protected void doCommit(DefaultTransactionStatus status) { commits++; committedNames.add(stagedName); }
        @Override protected void doRollback(DefaultTransactionStatus status) { rollbacks++; stagedName = null; }
    }
    @Test void everyImportedUserUsesARealIndependentTransaction() {
        AdminUserManager target = new AdminUserManager(); TestTransactions transactions = new TestTransactions();
        ProxyFactory factory = new ProxyFactory(target);
        factory.addAdvice(new TransactionInterceptor(transactions, new AnnotationTransactionAttributeSource()));
        AdminUserManager proxy = (AdminUserManager) factory.getProxy();
        ApplicationContext context = mock(ApplicationContext.class); when(context.getBean(AdminUserManager.class)).thenReturn(proxy);
        ReflectionTestUtils.setField(target, "applicationContext", context);
        UserInfoEntityService info = mock(UserInfoEntityService.class); UserRoleEntityService roles = mock(UserRoleEntityService.class);
        UserRecordEntityService records = mock(UserRecordEntityService.class);
        ReflectionTestUtils.setField(target, "userInfoEntityService", info); ReflectionTestUtils.setField(target, "userRoleEntityService", roles);
        ReflectionTestUtils.setField(target, "userRecordEntityService", records); ReflectionTestUtils.setField(target, "adminNoticeManager", mock(AdminNoticeManager.class));
        when(info.save(any(UserInfo.class))).thenAnswer(call -> {
            assertTrue(TransactionSynchronizationManager.isActualTransactionActive());
            transactions.stagedName = ((UserInfo) call.getArgument(0)).getUsername(); return true;
        });
        when(roles.save(any(UserRole.class))).thenAnswer(call -> !"failed".equals(transactions.stagedName));
        when(records.save(any(UserRecord.class))).thenReturn(true);
        assertThrows(StatusFailException.class, () -> target.insertBatchUser(Arrays.asList(Arrays.asList("ok", "secret"), Arrays.asList("failed", "secret"))));
        assertEquals(1, transactions.commits); assertEquals(1, transactions.rollbacks);
        assertEquals(Collections.singletonList("ok"), transactions.committedNames);
    }
    @Test void failedRemoteAssociationRollsBackBeforeServiceReturnsError() throws Exception {
        AccountProfile owner = new AccountProfile(); owner.setUid("owner"); owner.setUsername("owner");
        Subject subject = mock(Subject.class); when(subject.getPrincipal()).thenReturn(owner); ThreadContext.bind(subject);
        try {
            AdminContestProblemManager target = new AdminContestProblemManager(); TestTransactions transactions = new TestTransactions();
            ContestEntityService contests = mock(ContestEntityService.class); ContestProblemEntityService assignments = mock(ContestProblemEntityService.class);
            ProblemEntityService problems = mock(ProblemEntityService.class); RemoteProblemManager remote = mock(RemoteProblemManager.class);
            ReflectionTestUtils.setField(target, "contestEntityService", contests); ReflectionTestUtils.setField(target, "contestProblemEntityService", assignments);
            ReflectionTestUtils.setField(target, "problemEntityService", problems); ReflectionTestUtils.setField(target, "remoteProblemManager", remote);
            when(contests.getById(7L)).thenReturn(new Contest().setId(7L).setUid("owner").setIsGroup(false));
            when(remote.getOtherOJProblemInfo("CF", "123A", "owner")).thenReturn(new ProblemStrategy.RemoteProblemInfo());
            when(remote.adminAddOtherOJProblem(any(), eq("CF"))).thenAnswer(call -> {
                assertTrue(TransactionSynchronizationManager.isActualTransactionActive()); transactions.stagedName = "remote-public-problem";
                return new Problem().setId(88L).setAuth(1).setTitle("remote").setProblemId("CF-123A");
            });
            when(problems.saveOrUpdate(any(Problem.class))).thenReturn(true);
            // A DB failure while associating must roll back the newly imported problem too.
            when(assignments.saveOrUpdate(any())).thenReturn(false);
            ProxyFactory factory = new ProxyFactory(target);
            factory.addAdvice(new TransactionInterceptor(transactions, new AnnotationTransactionAttributeSource()));
            AdminContestProblemServiceImpl service = new AdminContestProblemServiceImpl();
            ReflectionTestUtils.setField(service, "adminContestProblemManager", factory.getProxy());
            assertNotEquals(200, service.importContestRemoteOJProblem("CF", "123A", 7L, "A").getStatus());
            assertEquals(0, transactions.commits); assertEquals(1, transactions.rollbacks); assertTrue(transactions.committedNames.isEmpty());
        } finally { ThreadContext.unbindSubject(); }
    }
}
