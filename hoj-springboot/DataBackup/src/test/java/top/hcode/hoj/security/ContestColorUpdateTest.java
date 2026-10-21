package top.hcode.hoj.security;

import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.dao.contest.ContestProblemEntityService;
import top.hcode.hoj.manager.admin.contest.AdminContestProblemManager;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.contest.ContestProblem;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.validator.GroupValidator;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ContestColorUpdateTest {
    private AdminContestProblemManager manager;
    private ContestProblemEntityService problems;
    private Subject subject;
    @BeforeEach void setup() {
        subject = mock(Subject.class);
        AccountProfile operator = new AccountProfile(); operator.setUid("owner"); operator.setUsername("owner");
        when(subject.getPrincipal()).thenReturn(operator); ThreadContext.bind(subject);
        problems = mock(ContestProblemEntityService.class);
        ContestEntityService contests = mock(ContestEntityService.class);
        when(contests.getById(7L)).thenReturn(new Contest().setId(7L).setUid("owner").setIsGroup(false));
        when(problems.getById(11L)).thenReturn(new ContestProblem().setId(11L).setCid(7L).setPid(3L)
                .setDisplayId("A").setDisplayTitle("Original title").setColor("#000000"));
        when(problems.saveOrUpdate(any(ContestProblem.class))).thenReturn(true);
        manager = new AdminContestProblemManager();
        ReflectionTestUtils.setField(manager, "contestProblemEntityService", problems);
        ReflectionTestUtils.setField(manager, "contestEntityService", contests);
        ReflectionTestUtils.setField(manager, "groupValidator", mock(GroupValidator.class));
    }
    @AfterEach void cleanup() { ThreadContext.unbindSubject(); }
    @Test void minimalColorRequestUsesStoredContestAndRetainsRecordDisplayId() throws Exception {
        ContestProblem result = manager.setContestProblem(new ContestProblem().setId(11L).setColor("#123456"));
        assertEquals(Long.valueOf(7L), result.getCid());
        assertEquals(Long.valueOf(3L), result.getPid());
        assertEquals("A", result.getDisplayId());
        assertEquals("Original title", result.getDisplayTitle());
        verify(problems).syncContestRecord(3L, 7L, "A");
        ArgumentCaptor<ContestProblem> update = ArgumentCaptor.forClass(ContestProblem.class);
        verify(problems).saveOrUpdate(update.capture());
        assertNull(update.getValue().getCid());
        assertNull(update.getValue().getPid());
        assertNull(update.getValue().getDisplayTitle());
        assertEquals("#123456", update.getValue().getColor());
    }
    @Test void rootCanUpdateAnExistingContestWithTheLegacyPartialRequest() throws Exception {
        when(subject.hasRole("root")).thenReturn(true);
        AccountProfile root = new AccountProfile(); root.setUid("another-root"); when(subject.getPrincipal()).thenReturn(root);
        assertEquals("#abcdef", manager.setContestProblem(new ContestProblem().setId(11L).setColor("#abcdef")).getColor());
    }
    @Test void forgedContestOrProblemCannotMoveTheAssociation() {
        assertThrows(StatusFailException.class, () -> manager.setContestProblem(new ContestProblem().setId(11L).setCid(8L)));
        assertThrows(StatusFailException.class, () -> manager.setContestProblem(new ContestProblem().setId(11L).setPid(4L)));
        verify(problems, never()).saveOrUpdate(any(ContestProblem.class));
    }
    @Test void unrelatedAdminCannotManageAnotherOwnersContest() {
        AccountProfile other = new AccountProfile(); other.setUid("other"); when(subject.getPrincipal()).thenReturn(other);
        when(subject.hasRole("admin")).thenReturn(true);
        assertThrows(StatusFailException.class, () -> manager.setContestProblem(new ContestProblem().setId(11L).setColor("red")));
        verify(problems, never()).saveOrUpdate(any(ContestProblem.class));
    }
    @Test void missingRecordAndMissingAssociationAreRejected() {
        assertThrows(StatusFailException.class, () -> manager.setContestProblem(new ContestProblem().setId(99L)));
        assertThrows(StatusFailException.class, () -> manager.setContestProblem(new ContestProblem().setCid(7L)));
        assertThrows(StatusFailException.class, () -> manager.setContestProblem(null));
    }
}
