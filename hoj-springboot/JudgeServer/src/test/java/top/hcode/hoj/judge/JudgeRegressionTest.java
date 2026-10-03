package top.hcode.hoj.judge;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.common.exception.SystemError;
import top.hcode.hoj.dao.JudgeCaseEntityService;
import top.hcode.hoj.judge.entity.JudgeDTO;
import top.hcode.hoj.judge.entity.JudgeGlobalDTO;
import top.hcode.hoj.judge.task.DefaultJudge;
import top.hcode.hoj.pojo.entity.judge.Judge;
import top.hcode.hoj.pojo.entity.problem.Problem;
import top.hcode.hoj.util.Constants;
import top.hcode.hoj.util.ThreadPoolUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class JudgeRegressionTest {

    @Test
    void missingOrEmptyTestCasesFailBeforeAnyProgramRuns() {
        JudgeRun judgeRun = new JudgeRun();
        for (JSONObject info : Arrays.asList(new JSONObject(), new JSONObject().set("testCases", new JSONArray()))) {
            assertThrows(SystemError.class, () -> judgeRun.judgeAllCase(1L, new Problem(), "C++",
                    "unused", info, null, "", false, Constants.JudgeCaseMode.DEFAULT.getMode()));
        }
    }

    @Test
    void emptyResultsCannotBecomeAcceptedOrWriteAnEmptyBatch() {
        JudgeStrategy strategy = new JudgeStrategy();
        JudgeCaseEntityService caseService = mock(JudgeCaseEntityService.class);
        ReflectionTestUtils.setField(strategy, "JudgeCaseEntityService", caseService);

        assertThrows(SystemError.class, () -> strategy.getJudgeInfo(Collections.emptyList(),
                new Problem(), new Judge(), Constants.JudgeCaseMode.DEFAULT.getMode()));
        assertThrows(SystemError.class, () -> strategy.getJudgeInfo(null,
                new Problem(), new Judge(), Constants.JudgeCaseMode.DEFAULT.getMode()));
        verifyZeroInteractions(caseService);
    }

    @Test
    void skippedSubtaskCasesKeepTheirOwnSequenceAndOtherGroupsStillRun() throws Exception {
        JudgeRun judgeRun = new JudgeRun();
        DefaultJudge defaultJudge = mock(DefaultJudge.class);
        ReflectionTestUtils.setField(judgeRun, "defaultJudge", defaultJudge);
        ReflectionTestUtils.setField(judgeRun, "languageConfigLoader", mock(LanguageConfigLoader.class));
        when(defaultJudge.judge(any(JudgeDTO.class), any(JudgeGlobalDTO.class))).thenAnswer(invocation -> {
            JudgeDTO testcase = invocation.getArgument(0);
            return new JSONObject().set("status", testcase.getTestCaseNum() == 1
                    ? Constants.Judge.STATUS_WRONG_ANSWER.getStatus()
                    : Constants.Judge.STATUS_ACCEPTED.getStatus()).set("time", 1).set("memory", 1);
        });

        JSONArray cases = new JSONArray();
        for (int index = 1; index <= 4; index++) {
            cases.add(new JSONObject().set("inputName", index + ".in").set("outputName", index + ".out")
                    .set("caseId", (long) index).set("score", 50).set("groupNum", index == 4 ? 2 : 1));
        }
        Problem problem = new Problem().setId(1L).setType(Constants.Contest.TYPE_OI.getCode())
                .setJudgeMode(Constants.JudgeMode.DEFAULT.getMode()).setTimeLimit(1000)
                .setMemoryLimit(128).setStackLimit(128);
        ExecutorService originalPool = ThreadPoolUtils.getInstance().getThreadPool();
        ExecutorService testPool = Executors.newSingleThreadExecutor();
        ReflectionTestUtils.setField(ThreadPoolUtils.class, "executorService", testPool);
        try {
            List<JSONObject> results = judgeRun.judgeAllCase(1L, problem, "C++", "unused",
                    new JSONObject().set("testCases", cases), null, "", false,
                    Constants.JudgeCaseMode.SUBTASK_LOWEST.getMode());

            assertEquals(Arrays.asList(1, 2, 3, 4), results.stream().map(r -> r.getInt("seq")).collect(Collectors.toList()));
            assertEquals(Arrays.asList(1L, 2L, 3L, 4L), results.stream().map(r -> r.getLong("caseId")).collect(Collectors.toList()));
            assertEquals(Constants.Judge.STATUS_CANCELLED.getStatus(), results.get(1).getInt("status"));
            assertEquals(Constants.Judge.STATUS_CANCELLED.getStatus(), results.get(2).getInt("status"));
            assertEquals(Constants.Judge.STATUS_ACCEPTED.getStatus(), results.get(3).getInt("status"));
            verify(defaultJudge, times(2)).judge(any(JudgeDTO.class), any(JudgeGlobalDTO.class));
        } finally {
            ReflectionTestUtils.setField(ThreadPoolUtils.class, "executorService", originalPool);
            testPool.shutdownNow();
        }
    }

    @Test
    void languageLimitsSupportMillisecondsSecondsAndBareMillisecondValues() {
        LanguageConfigLoader loader = new LanguageConfigLoader();
        assertEquals(Long.valueOf(125), ReflectionTestUtils.<Long>invokeMethod(loader, "parseTimeStr", "125ms"));
        assertEquals(Long.valueOf(2000), ReflectionTestUtils.<Long>invokeMethod(loader, "parseTimeStr", "2s"));
        assertEquals(Long.valueOf(125), ReflectionTestUtils.<Long>invokeMethod(loader, "parseTimeStr", "125"));
        assertEquals(Long.valueOf(125), ReflectionTestUtils.<Long>invokeMethod(loader, "parseTimeStr", "125MS"));
        assertEquals(Long.valueOf(3000), ReflectionTestUtils.<Long>invokeMethod(loader, "parseTimeStr", ""));
    }
}
