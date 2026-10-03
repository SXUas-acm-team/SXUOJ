package top.hcode.hoj.security;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.dao.RemoteJudgeAccountEntityService;
import top.hcode.hoj.service.impl.RemoteJudgeServiceImpl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RemoteAccountLeaseTest {
    @Test void resultOnlyRechecksCannotReleaseAnotherTasksAccount() {
        RemoteJudgeAccountEntityService accounts = mock(RemoteJudgeAccountEntityService.class);
        RemoteJudgeServiceImpl service = new RemoteJudgeServiceImpl();
        ReflectionTestUtils.setField(service, "remoteJudgeAccountEntityService", accounts);
        service.changeAccountStatus("CF", "account", null);
        verifyNoInteractions(accounts);
    }

    @Test void releaseRequiresTheSameLeaseGenerationAndDoesNotRetryStaleLeases() {
        RemoteJudgeAccountEntityService accounts = mock(RemoteJudgeAccountEntityService.class);
        RemoteJudgeServiceImpl service = new RemoteJudgeServiceImpl();
        ReflectionTestUtils.setField(service, "remoteJudgeAccountEntityService", accounts);
        service.changeAccountStatus("GYM", "account", 12L);
        ArgumentCaptor<UpdateWrapper> update = ArgumentCaptor.forClass(UpdateWrapper.class);
        verify(accounts, times(1)).update(update.capture());
        UpdateWrapper predicate = update.getValue();
        String sql = predicate.getSqlSegment();
        assertTrue(sql.contains("version"));
        assertTrue(sql.contains("status"));
        assertTrue(predicate.getParamNameValuePairs().containsValue(12L));
        assertTrue(predicate.getParamNameValuePairs().containsValue("CF"));
        assertTrue(predicate.getParamNameValuePairs().containsValue(false));
    }
}
