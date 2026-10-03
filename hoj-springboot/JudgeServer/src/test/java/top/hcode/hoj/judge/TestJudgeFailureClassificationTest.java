package top.hcode.hoj.judge;

import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import top.hcode.hoj.judge.entity.LanguageConfig;
import top.hcode.hoj.pojo.dto.TestJudgeReq;
import top.hcode.hoj.pojo.dto.TestJudgeRes;
import top.hcode.hoj.util.Constants;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;

class TestJudgeFailureClassificationTest {

    @Test
    void sandboxFailureIsReportedAsSystemError() {
        RestTemplate client = SandboxRun.getRestTemplate();
        ClientHttpRequestFactory originalFactory = client.getRequestFactory();
        MockRestServiceServer sandbox = MockRestServiceServer.bindTo(client).build();
        try {
            sandbox.expect(requestTo(SandboxRun.getSandboxBaseUrl() + "/run")).andRespond(withServerError());
            TestJudgeRes response = strategy().testJudge(new TestJudgeReq().setLanguage("C++").setCode("int main() {}"));
            assertEquals(Constants.Judge.STATUS_SYSTEM_ERROR.getStatus(), response.getStatus());
            sandbox.verify();
        } finally {
            client.setRequestFactory(originalFactory);
        }
    }

    @Test
    void compilerDiagnosticsRemainCompileErrors() {
        RestTemplate client = SandboxRun.getRestTemplate();
        ClientHttpRequestFactory originalFactory = client.getRequestFactory();
        MockRestServiceServer sandbox = MockRestServiceServer.bindTo(client).build();
        try {
            sandbox.expect(requestTo(SandboxRun.getSandboxBaseUrl() + "/run"))
                    .andRespond(withSuccess("[{\"status\":\"Nonzero Exit Status\",\"files\":{\"stderr\":\"syntax error\"}}]", MediaType.APPLICATION_JSON));
            TestJudgeRes response = strategy().testJudge(new TestJudgeReq().setLanguage("C++").setCode("broken code"));
            assertEquals(Constants.Judge.STATUS_COMPILE_ERROR.getStatus(), response.getStatus());
            assertTrue(response.getStderr().contains("syntax error"));
            sandbox.verify();
        } finally {
            client.setRequestFactory(originalFactory);
        }
    }

    @Test
    void unexpectedInternalFailureIsNotReportedAsBadStudentCode() {
        JudgeStrategy strategy = new JudgeStrategy();
        LanguageConfigLoader loader = mock(LanguageConfigLoader.class);
        when(loader.getLanguageConfigByName("C++")).thenThrow(new IllegalStateException("configuration unavailable"));
        ReflectionTestUtils.setField(strategy, "languageConfigLoader", loader);
        TestJudgeRes response = strategy.testJudge(new TestJudgeReq().setLanguage("C++"));
        assertEquals(Constants.Judge.STATUS_SYSTEM_ERROR.getStatus(), response.getStatus());
    }

    private JudgeStrategy strategy() {
        LanguageConfig language = new LanguageConfig();
        language.setLanguage("C++");
        language.setCompileCommand("g++ main.cpp -o main");
        language.setSrcName("main.cpp");
        language.setExeName("main");
        language.setCompileEnvs(Collections.emptyList());
        language.setMaxCpuTime(1000L);
        language.setMaxRealTime(1000L);
        language.setMaxMemory(128 * 1024 * 1024L);
        LanguageConfigLoader loader = mock(LanguageConfigLoader.class);
        when(loader.getLanguageConfigByName("C++")).thenReturn(language);
        JudgeStrategy strategy = new JudgeStrategy();
        ReflectionTestUtils.setField(strategy, "languageConfigLoader", loader);
        return strategy;
    }
}
