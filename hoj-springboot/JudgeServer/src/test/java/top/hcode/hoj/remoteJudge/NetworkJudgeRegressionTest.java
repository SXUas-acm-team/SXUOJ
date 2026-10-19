package top.hcode.hoj.remoteJudge;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import top.hcode.hoj.http.SecureHttp;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.dao.JudgeCaseEntityService;
import top.hcode.hoj.dao.JudgeEntityService;
import top.hcode.hoj.judge.JudgeContext;
import top.hcode.hoj.pojo.entity.judge.Judge;
import top.hcode.hoj.pojo.entity.judge.JudgeCase;
import top.hcode.hoj.pojo.dto.ToJudgeDTO;
import top.hcode.hoj.remoteJudge.entity.RemoteJudgeDTO;
import top.hcode.hoj.remoteJudge.entity.RemoteJudgeRes;
import top.hcode.hoj.remoteJudge.task.RemoteJudgeStrategy;
import top.hcode.hoj.remoteJudge.task.Impl.POJJudge;
import top.hcode.hoj.remoteJudge.task.Impl.HDUJudge;
import top.hcode.hoj.remoteJudge.task.Impl.AtCoderJudge;
import top.hcode.hoj.service.RemoteJudgeService;
import top.hcode.hoj.util.Constants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Scheduler, remote responses, database and account storage are all isolated mocks. */
class NetworkJudgeRegressionTest {
    private JudgeEntityService judges;
    private JudgeCaseEntityService cases;
    private JudgeContext context;
    private RemoteJudgeService accounts;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> future;
    private RemoteJudgeStrategy strategy;
    private RemoteJudgeDTO dto;
    private RemoteJudgeGetResult results;
    private final List<Runnable> tasks = new ArrayList<>();

    @BeforeEach
    void setup() {
        judges = mock(JudgeEntityService.class);
        cases = mock(JudgeCaseEntityService.class);
        context = mock(JudgeContext.class);
        accounts = mock(RemoteJudgeService.class);
        scheduler = mock(ScheduledExecutorService.class);
        future = mock(ScheduledFuture.class);
        strategy = mock(RemoteJudgeStrategy.class);
        dto = RemoteJudgeDTO.builder().oj("HDU").judgeId(42L).submitId(1000L)
                .username("mock-account").accountVersion(7L).cid(0L).pid(2L).uid("local-user").build();
        when(strategy.getRemoteJudgeDTO()).thenReturn(dto);
        when(judges.updateById(any(Judge.class))).thenReturn(true);
        when(cases.saveBatch(anyCollection())).thenReturn(true);
        doAnswer(invocation -> {
            tasks.add(invocation.getArgument(0));
            return future;
        }).when(scheduler).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        results = new RemoteJudgeGetResult(scheduler);
        ReflectionTestUtils.setField(results, "judgeEntityService", judges);
        ReflectionTestUtils.setField(results, "judgeCaseEntityService", cases);
        ReflectionTestUtils.setField(results, "judgeContext", context);
        ReflectionTestUtils.setField(results, "remoteJudgeService", accounts);
    }

    @Test void missingResponseDoesNotKillScheduledRetry() {
        when(strategy.result()).thenReturn(null, response(0));
        results.process(strategy);
        assertDoesNotThrow(() -> tasks.get(0).run());
        verify(accounts, never()).changeAccountStatus(anyString(), anyString(), anyLong());
        tasks.get(0).run();
        verify(judges).updateById(argThat((Judge j) -> j.getSubmitId() == 42L && j.getStatus() == 0));
        verify(accounts).changeAccountStatus("HDU", "mock-account", 7L);
        verify(future).cancel(false);
    }

    @Test void missingOrUnrecognizedStatusCannotBecomeFinalResult() {
        when(strategy.result()).thenReturn(RemoteJudgeRes.builder().build(), response(999), response(5));
        results.process(strategy);
        tasks.get(0).run();
        tasks.get(0).run();
        verifyNoInteractions(judges, cases, accounts, context);
        tasks.get(0).run();
        verify(judges).updateById(argThat((Judge j) -> j.getStatus() == 5));
        verifyNoInteractions(accounts, context);
    }

    @Test void retriesSavingSameCasesBeforeAdvancingRemoteParser() {
        RemoteJudgeRes result = response(0).setJudgeCaseList(Collections.singletonList(new JudgeCase().setSubmitId(42L)));
        when(strategy.result()).thenReturn(result);
        when(cases.saveBatch(anyCollection())).thenThrow(new IllegalStateException("mock DB failure")).thenReturn(true);
        results.process(strategy);
        assertDoesNotThrow(() -> tasks.get(0).run());
        verifyNoInteractions(accounts);
        tasks.get(0).run();
        verify(strategy, times(1)).result();
        verify(cases, times(2)).saveBatch(same(result.getJudgeCaseList()));
        InOrder order = inOrder(cases, judges, context, accounts);
        order.verify(cases, times(2)).saveBatch(anyCollection());
        order.verify(judges).updateById(any(Judge.class));
        order.verify(context).updateOtherTable(eq(42L), eq(0), eq(0L), eq("local-user"), eq(2L), isNull(), isNull(), isNull());
        order.verify(accounts).changeAccountStatus("HDU", "mock-account", 7L);
    }

    @Test void successfulCaseSaveIsNotRepeatedAfterResultWriteFails() {
        when(strategy.result()).thenReturn(response(0).setJudgeCaseList(Collections.singletonList(new JudgeCase())));
        when(judges.updateById(any(Judge.class))).thenReturn(false, true);
        results.process(strategy);
        tasks.get(0).run();
        verifyNoInteractions(accounts);
        tasks.get(0).run();
        verify(strategy, times(1)).result();
        verify(cases, times(1)).saveBatch(anyCollection());
        verify(accounts).changeAccountStatus("HDU", "mock-account", 7L);
    }

    @Test void savedTerminalResultIsRetainedWhenDerivedSynchronizationNeverRecovers() {
        for (int status : new int[]{Constants.Judge.STATUS_ACCEPTED.getStatus(), Constants.Judge.STATUS_WRONG_ANSWER.getStatus()}) {
            setup();
            tasks.clear();
            when(strategy.result()).thenReturn(response(status));
            doThrow(new IllegalStateException("mock derived table unavailable")).when(context)
                    .updateOtherTable(eq(42L), eq(status), eq(0L), eq("local-user"), eq(2L), isNull(), isNull(), isNull());
            results.process(strategy);
            for (int i = 0; i < 61; i++) assertDoesNotThrow(() -> tasks.get(0).run());
            verify(judges, times(1)).updateById(argThat((Judge j) -> j.getStatus() == status));
            verify(judges, never()).update(any(UpdateWrapper.class));
            verify(strategy, times(1)).result();
            verify(context, times(60)).updateOtherTable(eq(42L), eq(status), eq(0L), eq("local-user"), eq(2L),
                    isNull(), isNull(), isNull());
            verify(context, never()).updateOtherTable(anyLong(), eq(Constants.Judge.STATUS_SYSTEM_ERROR.getStatus()),
                    anyLong(), anyString(), anyLong(), isNull(), isNull(), isNull());
            verify(accounts).changeAccountStatus("HDU", "mock-account", 7L);
            verify(future).cancel(false);
        }
    }

    @Test void transientDerivedSynchronizationFailureRetriesWithoutRewritingTerminalVerdict() {
        when(strategy.result()).thenReturn(response(Constants.Judge.STATUS_ACCEPTED.getStatus()));
        doThrow(new IllegalStateException("mock transient derived table failure")).doNothing().when(context)
                .updateOtherTable(eq(42L), eq(0), eq(0L), eq("local-user"), eq(2L), isNull(), isNull(), isNull());
        results.process(strategy);
        tasks.get(0).run();
        verifyNoInteractions(accounts);
        tasks.get(0).run();
        verify(judges, times(1)).updateById(any(Judge.class));
        verify(strategy, times(1)).result();
        verify(context, times(2)).updateOtherTable(eq(42L), eq(0), eq(0L), eq("local-user"), eq(2L),
                isNull(), isNull(), isNull());
        verify(accounts).changeAccountStatus("HDU", "mock-account", 7L);
    }

    @Test void duplicatePollingIsSuppressedUntilCompletion() {
        when(strategy.result()).thenReturn(response(5), response(0));
        results.process(strategy);
        results.process(strategy);
        assertEquals(1, tasks.size());
        tasks.get(0).run();
        tasks.get(0).run();
        results.process(strategy);
        assertEquals(2, tasks.size());
        tasks.get(0).run(); // A late execution of the finished task is harmless.
        verify(strategy, times(2)).result();
        verify(accounts, times(1)).changeAccountStatus("HDU", "mock-account", 7L);
    }

    @Test void timeoutSynchronizesLocalIdAndAlwaysReleasesAccount() {
        when(strategy.result()).thenThrow(new IllegalStateException("mock network failure"));
        doThrow(new IllegalStateException("mock DB unavailable")).when(judges).update(any(UpdateWrapper.class));
        results.process(strategy);
        for (int i = 0; i < 61; i++) assertDoesNotThrow(() -> tasks.get(0).run());
        verify(strategy, times(60)).result();
        verify(context).updateOtherTable(eq(42L), eq(Constants.Judge.STATUS_SYSTEM_ERROR.getStatus()), eq(0L),
                eq("local-user"), eq(2L), isNull(), isNull(), isNull());
        verify(accounts).changeAccountStatus("HDU", "mock-account", 7L);
        verify(future).cancel(false);
        results.process(strategy);
        assertEquals(2, tasks.size());
    }

    @Test void schedulingRejectionDoesNotRetainAnActiveTaskOrAccount() {
        doThrow(new RejectedExecutionException("mock shutdown")).doAnswer(invocation -> {
            tasks.add(invocation.getArgument(0));
            return future;
        }).when(scheduler).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        assertThrows(RejectedExecutionException.class, () -> results.process(strategy));
        verify(accounts).changeAccountStatus("HDU", "mock-account", 7L);
        results.process(strategy);
        assertEquals(1, tasks.size());
    }

    @Test void immediateCompletionStillCancelsAssignedFuture() {
        when(strategy.result()).thenReturn(response(0));
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return future;
        }).when(scheduler).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
        results.process(strategy);
        verify(future).cancel(false);
        results.process(strategy);
        verify(accounts, times(2)).changeAccountStatus("HDU", "mock-account", 7L);
    }

    @Test void failedNewSubmissionCannotReuseEarlierRemoteAcceptedSubmission() {
        RemoteJudgeToSubmit submit = new RemoteJudgeToSubmit();
        ReflectionTestUtils.setField(submit, "judgeEntityService", judges);
        ReflectionTestUtils.setField(submit, "judgeContext", context);
        ReflectionTestUtils.setField(submit, "remoteJudgeService", accounts);
        doThrow(new IllegalStateException("mock login rejected")).when(strategy).submit();
        assertFalse(submit.process(strategy));
        assertNull(dto.getSubmitId());
        verify(judges, never()).updateById(any(Judge.class));
        verify(context).updateOtherTable(eq(42L), eq(Constants.Judge.STATUS_SYSTEM_ERROR.getStatus()), eq(0L),
                eq("local-user"), eq(2L), isNull(), isNull(), isNull());
        verify(accounts).changeAccountStatus("HDU", "mock-account", 7L);
    }

    @Test void nonPositiveRemoteIdIsFailure() {
        RemoteJudgeToSubmit submit = new RemoteJudgeToSubmit();
        ReflectionTestUtils.setField(submit, "judgeEntityService", judges);
        ReflectionTestUtils.setField(submit, "judgeContext", context);
        ReflectionTestUtils.setField(submit, "remoteJudgeService", accounts);
        doAnswer(invocation -> { dto.setSubmitId(0L); return null; }).when(strategy).submit();
        assertFalse(submit.process(strategy));
        verify(judges, never()).updateById(any(Judge.class));
    }

    @Test void remoteSubmissionStorageFailureCannotLeakReservedAccountOrStartPolling() {
        for (boolean throwsError : new boolean[]{false, true}) {
            setup();
            RemoteJudgeToSubmit submit = new RemoteJudgeToSubmit();
            ReflectionTestUtils.setField(submit, "judgeEntityService", judges);
            ReflectionTestUtils.setField(submit, "judgeContext", context);
            ReflectionTestUtils.setField(submit, "remoteJudgeService", accounts);
            doAnswer(invocation -> { dto.setSubmitId(1001L); return null; }).when(strategy).submit();
            if (throwsError) when(judges.updateById(any(Judge.class))).thenThrow(new IllegalStateException("mock unavailable DB"));
            else when(judges.updateById(any(Judge.class))).thenReturn(false);
            // This is the actual Context path: polling only begins when process returns true.
            assertFalse(submit.process(strategy));
            verify(accounts).changeAccountStatus("HDU", "mock-account", 7L);
            verify(context).updateOtherTable(eq(42L), eq(Constants.Judge.STATUS_SYSTEM_ERROR.getStatus()), eq(0L),
                    eq("local-user"), eq(2L), isNull(), isNull(), isNull());
            verifyNoInteractions(scheduler);
        }
    }

    @Test void submissionFailureStillReleasesAccountWhenFailureStatusWriteAlsoThrows() {
        RemoteJudgeToSubmit submit = new RemoteJudgeToSubmit();
        ReflectionTestUtils.setField(submit, "judgeEntityService", judges);
        ReflectionTestUtils.setField(submit, "judgeContext", context);
        ReflectionTestUtils.setField(submit, "remoteJudgeService", accounts);
        doThrow(new IllegalStateException("mock DB unavailable")).when(judges).update(any(UpdateWrapper.class));
        assertFalse(submit.process(strategy));
        verify(accounts).changeAccountStatus("HDU", "mock-account", 7L);
    }

    @Test void preservesHyphenatedAtCoderContestNameBeforeSubmission() {
        RemoteJudgeContext remoteContext = new RemoteJudgeContext();
        RemoteJudgeToSubmit submit = mock(RemoteJudgeToSubmit.class);
        ReflectionTestUtils.setField(remoteContext, "remoteJudgeToSubmit", submit);
        remoteContext.judge(new ToJudgeDTO().setRemoteJudgeProblem("AC-code-festival-2018-quala_a")
                .setJudge(new Judge().setSubmitId(42L).setCid(0L)));
        ArgumentCaptor<RemoteJudgeStrategy> submitted = ArgumentCaptor.forClass(RemoteJudgeStrategy.class);
        verify(submit).process(submitted.capture());
        RemoteJudgeDTO actual = submitted.getValue().getRemoteJudgeDTO();
        assertEquals("code-festival-2018-quala_a", actual.getCompleteProblemId());
        assertEquals("code-festival-2018-quala", actual.getContestId());
        assertEquals("a", actual.getProblemNum());
    }

    @Test void parsesMultipleLetterCodeforcesAndGymIndicesWithoutTruncation() {
        for (String oj : new String[]{"CF", "GYM"}) {
            RemoteJudgeContext remoteContext = new RemoteJudgeContext();
            RemoteJudgeToSubmit submit = mock(RemoteJudgeToSubmit.class);
            ReflectionTestUtils.setField(remoteContext, "remoteJudgeToSubmit", submit);
            remoteContext.judge(new ToJudgeDTO().setRemoteJudgeProblem(oj + "-123AA12")
                    .setJudge(new Judge().setSubmitId(42L).setCid(0L)));
            ArgumentCaptor<RemoteJudgeStrategy> submitted = ArgumentCaptor.forClass(RemoteJudgeStrategy.class);
            verify(submit).process(submitted.capture());
            assertEquals("123", submitted.getValue().getRemoteJudgeDTO().getContestId());
            assertEquals("AA12", submitted.getValue().getRemoteJudgeDTO().getProblemNum());
        }
    }

    @Test void platformsCannotAdoptAnEarlierSubmissionEvenWhenSubmitReturnsSuccess() {
        for (String oj : new String[]{"POJ", "HDU", "AC"}) {
            MockRequests requests = new MockRequests(oj, 200, 11L, 11L);
            RemoteJudgeStrategy platform = platform(oj, requests);
            platform.submit();
            assertEquals(-1L, platform.getRemoteJudgeDTO().getSubmitId(), oj);
            assertFalse(requests.queries.isEmpty());
            for (String url : requests.queries) {
                Map<String, String> parameters = queryParameters(url);
                String usernameKey = oj.equals("POJ") ? "user_id" : oj.equals("AC") ? "f.User" : "user";
                assertEquals("own&user=x", parameters.get(usernameKey), url);
                assertEquals(2, parameters.size(), url);
                assertFalse(parameters.containsKey("source"), url);
                assertFalse(parameters.containsKey("_usercode"), url);
            }
        }
    }

    @Test void platformsAssociateOnlyNewSubmissionAfterBaselineAndNeverPutSourceInGetUrl() {
        for (String oj : new String[]{"POJ", "HDU", "AC"}) {
            MockRequests requests = new MockRequests(oj, 200, 11L, 12L);
            RemoteJudgeStrategy platform = platform(oj, requests);
            platform.submit();
            assertEquals(12L, platform.getRemoteJudgeDTO().getSubmitId(), oj);
            for (String url : requests.queries) {
                assertFalse(url.contains("source="), url);
                assertFalse(url.contains("_usercode="), url);
                assertFalse(url.contains("secret-source-marker"), url);
            }
            verify(requests.submit).execute();
        }
    }

    @Test void unavailableSubmissionListCannotBeTreatedAsEmptyBaseline() {
        for (String oj : new String[]{"POJ", "HDU", "AC"}) {
            MockRequests requests = new MockRequests(oj, 503, 11L, 12L);
            assertThrows(IllegalStateException.class, () -> platform(oj, requests).submit());
            verify(requests.submit, never()).execute();
        }
    }

    @Test void interruptedRemoteSubmissionWaitPreservesCancellation() {
        POJJudge platform = new POJJudge();
        Thread.currentThread().interrupt();
        try {
            assertThrows(IllegalStateException.class, () -> ReflectionTestUtils.invokeMethod(platform, "pauseBeforeRetry", 1));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    private RemoteJudgeStrategy platform(String oj, MockRequests requests) {
        RemoteJudgeStrategy platform;
        if (oj.equals("POJ")) {
            platform = new POJJudge() {
                @Override public void login() { }
                @Override protected HttpRequest getRequest(String url) { return requests.get(url); }
                @Override protected HttpRequest postRequest(String url) { return requests.post(url); }
                @Override protected void pauseBeforeRetry(int seconds) { }
            };
        } else if (oj.equals("HDU")) {
            platform = new HDUJudge() {
                @Override public void login() { }
                @Override protected HttpRequest getRequest(String url) { return requests.get(url); }
                @Override protected HttpRequest postRequest(String url) { return requests.post(url); }
                @Override protected void pauseBeforeRetry(int seconds) { }
            };
        } else {
            platform = new AtCoderJudge() {
                @Override public void login() { }
                @Override protected HttpRequest getRequest(String url) { return requests.get(url); }
                @Override protected HttpRequest postRequest(String url) { return requests.post(url); }
                @Override protected void pauseBeforeRetry(int seconds) { }
            };
        }
        platform.setRemoteJudgeDTO(RemoteJudgeDTO.builder().oj(oj).judgeId(42L).username("own&user=x")
                .cookies(Collections.emptyList()).contestId("abc110").completeProblemId(oj.equals("AC") ? "abc110_a" : "1000")
                .language(oj.equals("AC") ? "C++ 17 (gcc 12.2)" : "G++").loginStatus(302)
                .userCode("secret-source-marker").build());
        return platform;
    }

    private static final class MockRequests {
        private final HttpRequest query = spy(SecureHttp.get("https://example.invalid/status"));
        private final HttpRequest submit = spy(SecureHttp.post("https://example.invalid/submit"));
        private final List<String> queries = new ArrayList<>();
        private int queryCount;

        private MockRequests(String oj, int queryStatus, long previous, long current) {
            HttpResponse submitted = mock(HttpResponse.class);
            when(submitted.getStatus()).thenReturn(302);
            doReturn(submitted).when(submit).execute();
            doAnswer(invocation -> {
                // Execute Hutool's real GET query construction, then stop before opening
                // a socket. This detects retained POST forms/source, not just mock calls.
                ReflectionTestUtils.invokeMethod(query, "urlWithParamIfGet");
                queries.add(query.getUrl());
                long id = queryCount++ == 0 ? previous : current;
                String html;
                if (oj.equals("POJ")) html = "<tr align=center><td>" + id + "</td></tr>";
                else if (oj.equals("HDU")) html = "<td height=22px>" + id + "</td>";
                else html = "<a href=\"/contests/abc110/submissions/" + id + "\">Detail</a>";
                HttpResponse response = mock(HttpResponse.class);
                when(response.getStatus()).thenReturn(queryStatus);
                when(response.body()).thenReturn(html);
                return response;
            }).when(query).execute();
        }

        private HttpRequest get(String url) { return query.setUrl(url); }
        private HttpRequest post(String url) { return submit.setUrl(url); }
    }

    private RemoteJudgeRes response(int status) {
        return RemoteJudgeRes.builder().status(status).time(8).memory(16).build();
    }

    private Map<String, String> queryParameters(String url) {
        Map<String, String> parameters = new LinkedHashMap<>();
        for (String pair : java.net.URI.create(url).getRawQuery().split("&")) {
            String[] parts = pair.split("=", 2);
            try {
                String key = java.net.URLDecoder.decode(parts[0], "UTF-8");
                String value = java.net.URLDecoder.decode(parts[1], "UTF-8");
                assertNull(parameters.put(key, value), "Duplicate query parameter: " + url);
            } catch (java.io.UnsupportedEncodingException impossible) {
                throw new AssertionError(impossible);
            }
        }
        return parameters;
    }
}
