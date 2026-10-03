package top.hcode.hoj.judge;

import cn.hutool.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import top.hcode.hoj.judge.entity.JudgeDTO;
import top.hcode.hoj.judge.entity.JudgeGlobalDTO;
import top.hcode.hoj.judge.entity.SandBoxRes;
import top.hcode.hoj.judge.task.DefaultJudge;
import top.hcode.hoj.pojo.entity.problem.ProblemCase;
import top.hcode.hoj.util.Constants;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class TestCaseNormalizationTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void identicalOutputWithUnicodeLineEndWhitespaceIsAccepted() throws Exception {
        String expectedOutput = "answer\u2003\nnext\n";
        Files.write(temporaryDirectory.resolve("1.out"), expectedOutput.getBytes(StandardCharsets.UTF_8));
        ProblemCase testcase = new ProblemCase().setId(1L).setInput("1.in").setOutput("1.out").setScore(100);
        JSONObject info = new ProblemTestCaseUtils().initLocalTestCase(Constants.JudgeMode.DEFAULT.getMode(),
                Constants.JudgeCaseMode.DEFAULT.getMode(), "test", temporaryDirectory.toString(), Collections.singletonList(testcase));

        SandBoxRes runResult = SandBoxRes.builder().status(Constants.Judge.STATUS_ACCEPTED.getStatus())
                .time(1L).memory(1L).exitCode(0L).stdout(expectedOutput).build();
        JudgeGlobalDTO global = JudgeGlobalDTO.builder().maxTime(1000L).maxMemory(128L)
                .testCaseInfo(info).removeEOLBlank(true).needUserOutputFile(false).build();
        JSONObject verdict = new DefaultJudge().checkResult(runResult, JudgeDTO.builder().testCaseNum(1).build(), global);

        assertEquals(Constants.Judge.STATUS_ACCEPTED.getStatus(), verdict.getInt("status"));
    }

    @Test
    void testDataNormalizationPreservesLeadingAndInternalWhitespace() {
        assertEquals("  a b\n\n c", ProblemTestCaseUtils.rtrim("  a b \t\n \t\n c \t\n"));
        assertEquals("", ProblemTestCaseUtils.rtrim(" \t\r\n"));
        assertNull(ProblemTestCaseUtils.rtrim(null));
    }

    @Test
    void longWhitespaceBeforeContentDoesNotCauseRegexBacktracking() {
        char[] spaces = new char[1024 * 1024];
        Arrays.fill(spaces, ' ');
        String content = new String(spaces) + "x";
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> assertEquals(content, ProblemTestCaseUtils.rtrim(content)));
    }
}
