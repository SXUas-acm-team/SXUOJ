package top.hcode.hoj.security;

import org.junit.jupiter.api.Test;
import top.hcode.hoj.remoteJudge.entity.RemoteJudgeDTO;
import top.hcode.hoj.remoteJudge.task.Impl.CodeForcesJudge;
import top.hcode.hoj.util.JudgeUtils;
import top.hcode.hoj.util.ThreadPoolUtils;

import static org.junit.jupiter.api.Assertions.*;

class JudgeBoundariesTest {
    @Test void excludesCredentialsAndStudentCodeFromLogs() {
        String value = RemoteJudgeDTO.builder().oj("CF").password("password-marker")
                .csrfToken("csrf-marker").userCode("source-marker").build().toString();
        assertFalse(value.contains("password-marker"));
        assertFalse(value.contains("csrf-marker"));
        assertFalse(value.contains("source-marker"));
    }

    @Test void rejectsStaleAndUnrelatedCodeforcesSubmissionRows() {
        String html = "<table>" + row(90, "42", "A") + row(102, "99", "B") + row(103, "42", "A") + "</table>";
        assertEquals(103, CodeForcesJudge.selectSubmissionId(html, "42", "A", 100L));
        assertEquals(-1, CodeForcesJudge.selectSubmissionId(html, "42", "A", 103L));
        assertEquals(-1, CodeForcesJudge.selectSubmissionId(html, "42", "C", 100L));
        assertEquals(103, CodeForcesJudge.selectSubmissionId(html, "42", "A", null));
    }

    @Test void checkerPercentagesAreFiniteAndInsideTheScoreRange() {
        assertEquals(0.5, JudgeUtils.parsePercentage("50"));
        assertEquals(1.0, JudgeUtils.parsePercentage("100"));
        for (String invalid : new String[]{"101", "-1", "NaN", "Infinity", "1e309", "not a score"}) {
            assertNull(JudgeUtils.parsePercentage(invalid));
        }
    }

    @Test void comparisonHandlesLongWhitespaceWithoutRegexBacktracking() {
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < 1024 * 1024; i++) line.append(' ');
        line.append('x');
        assertTimeoutPreemptively(java.time.Duration.ofSeconds(2), () -> assertEquals(line.toString(), JudgeUtils.trimLineEnds(line.toString())));
        assertEquals("x\ny", JudgeUtils.trimLineEnds("x \t\n y ").replace("\n ", "\n"));
    }

    @Test void overloadRejectsNewWorkWithoutDiscardingAnExistingFuture() throws Exception {
        java.util.concurrent.ThreadPoolExecutor configured = (java.util.concurrent.ThreadPoolExecutor) ThreadPoolUtils.getInstance().getThreadPool();
        java.util.concurrent.ThreadPoolExecutor pool = new java.util.concurrent.ThreadPoolExecutor(1, 1, 1,
                java.util.concurrent.TimeUnit.SECONDS, new java.util.concurrent.ArrayBlockingQueue<>(1), configured.getRejectedExecutionHandler());
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch started = new java.util.concurrent.CountDownLatch(1);
        try {
            pool.submit(() -> { started.countDown(); release.await(); return 1; });
            assertTrue(started.await(2, java.util.concurrent.TimeUnit.SECONDS));
            java.util.concurrent.Future<Integer> queued = pool.submit(() -> 2);
            assertThrows(java.util.concurrent.RejectedExecutionException.class, () -> pool.submit(() -> 3));
            release.countDown();
            assertEquals(2, queued.get(2, java.util.concurrent.TimeUnit.SECONDS));
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    private String row(long id, String contest, String problem) {
        return "<tr data-submission-id=\"" + id + "\"><td><a href=\"/contest/" + contest + "/problem/" + problem + "\">problem</a></td></tr>";
    }
}
