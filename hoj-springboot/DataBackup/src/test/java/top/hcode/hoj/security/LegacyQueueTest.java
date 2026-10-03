package top.hcode.hoj.security;

import cn.hutool.json.JSONUtil;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.dao.judge.JudgeEntityService;
import top.hcode.hoj.judge.Dispatcher;
import top.hcode.hoj.judge.remote.RemoteJudgeReceiver;
import top.hcode.hoj.judge.self.JudgeReceiver;
import top.hcode.hoj.pojo.dto.ToJudgeDTO;
import top.hcode.hoj.pojo.entity.judge.Judge;
import top.hcode.hoj.pojo.vo.ConfigVO;
import top.hcode.hoj.utils.Constants;
import top.hcode.hoj.utils.RedisUtils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class LegacyQueueTest {
    @Test void localAndRemoteConsumersPollLegacyKeys() {
        java.util.List<String> polled = new java.util.ArrayList<>();
        JudgeReceiver local = new JudgeReceiver() {
            @Override public String getTaskByRedis(String queue) { polled.add(queue); return null; }
        };
        RemoteJudgeReceiver remote = new RemoteJudgeReceiver() {
            @Override public String getTaskByRedis(String queue) { polled.add(queue); return null; }
        };
        local.processWaitingTask();
        remote.processWaitingTask();
        assertTrue(polled.contains("Waiting Queue"));
        assertTrue(polled.contains("Remote Waiting Handle Queue"));
    }

    @Test void consumesHistoricalShapeUsingDatabaseSourceAndCurrentToken() {
        JudgeReceiver receiver = new JudgeReceiver() { @Override public void processWaitingTask() { } };
        JudgeEntityService judges = mock(JudgeEntityService.class);
        Dispatcher dispatcher = mock(Dispatcher.class);
        ConfigVO config = new ConfigVO();
        config.setJudgeToken("current-token");
        Judge authoritative = new Judge().setSubmitId(7L).setCid(0L).setCode("database-source")
                .setStatus(Constants.Judge.STATUS_PENDING.getStatus());
        when(judges.getById(7L)).thenReturn(authoritative);
        ReflectionTestUtils.setField(receiver, "judgeEntityService", judges);
        ReflectionTestUtils.setField(receiver, "dispatcher", dispatcher);
        ReflectionTestUtils.setField(receiver, "configVO", config);
        String historical = "{\"judge\":{\"submitId\":7,\"code\":\"historical-source\"},\"token\":\"old-token\"}";
        receiver.handleJudgeMsg(historical, "Waiting Queue");
        ArgumentCaptor<Object> data = ArgumentCaptor.forClass(Object.class);
        verify(dispatcher).dispatch(eq(Constants.TaskType.JUDGE), data.capture());
        ToJudgeDTO task = (ToJudgeDTO) data.getValue();
        assertSame(authoritative, task.getJudge());
        assertEquals("current-token", task.getToken());
    }

    @Test void discardsHistoricalTasksThatAlreadyFinished() {
        JudgeReceiver receiver = new JudgeReceiver() { @Override public void processWaitingTask() { } };
        JudgeEntityService judges = mock(JudgeEntityService.class);
        Dispatcher dispatcher = mock(Dispatcher.class);
        when(judges.getById(7L)).thenReturn(new Judge().setSubmitId(7L).setStatus(Constants.Judge.STATUS_ACCEPTED.getStatus()));
        ReflectionTestUtils.setField(receiver, "judgeEntityService", judges);
        ReflectionTestUtils.setField(receiver, "dispatcher", dispatcher);
        receiver.handleJudgeMsg("{\"judge\":{\"submitId\":7}}", "Waiting Queue");
        verifyNoInteractions(dispatcher);
    }
}
