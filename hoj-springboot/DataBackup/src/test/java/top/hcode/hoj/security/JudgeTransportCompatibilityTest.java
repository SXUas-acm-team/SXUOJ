package top.hcode.hoj.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import top.hcode.hoj.common.result.CommonResult;
import top.hcode.hoj.dao.judge.JudgeEntityService;
import top.hcode.hoj.judge.Dispatcher;
import top.hcode.hoj.pojo.entity.judge.Judge;
import java.util.Collections;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;

class JudgeTransportCompatibilityTest {
    @Test void judgeSuccessResponseIsReadableByAnUncustomizedRestTemplate() {
        RestTemplate client = new RestTemplate();
        MockRestServiceServer server = MockRestServiceServer.createServer(client);
        server.expect(requestTo("http://judge.invalid/judge")).andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess("{\"status\":200,\"data\":null,\"msg\":\"finished\"}", MediaType.APPLICATION_JSON));
        CommonResult response = client.postForObject("http://judge.invalid/judge", Collections.emptyMap(), CommonResult.class);
        assertEquals(Integer.valueOf(200), response.getStatus());
        assertEquals("finished", response.getMsg());
        server.verify();
    }

    @Test void immutableResponseSupportsDataAndErrorPayloadsWithoutCompilerParameterNames() throws Exception {
        CommonResult response = new ObjectMapper().readValue(
                "{\"status\":403,\"data\":{\"reason\":\"credentials\"},\"msg\":\"denied\"}", CommonResult.class);
        assertEquals(Integer.valueOf(403), response.getStatus());
        assertEquals("credentials", ((Map) response.getData()).get("reason"));
        assertEquals("denied", response.getMsg());
        assertEquals(200, new ObjectMapper().readTree(new ObjectMapper().writeValueAsString(
                CommonResult.successResponse())).get("status").asInt());
    }

    @Test void aValidSuccessResponseDoesNotOverwriteTheJudgersVerdict() {
        Dispatcher dispatcher = new Dispatcher();
        JudgeEntityService judges = mock(JudgeEntityService.class);
        ReflectionTestUtils.setField(dispatcher, "judgeEntityService", judges);
        ReflectionTestUtils.invokeMethod(dispatcher, "checkResult", CommonResult.successResponse(), 7001L);
        verifyNoInteractions(judges);
    }

    @Test void aRejectedRequestUpdatesTheCorrectSubmissionInsteadOfLosingItsId() {
        Dispatcher dispatcher = new Dispatcher();
        JudgeEntityService judges = mock(JudgeEntityService.class);
        ReflectionTestUtils.setField(dispatcher, "judgeEntityService", judges);
        ReflectionTestUtils.invokeMethod(dispatcher, "checkResult", CommonResult.errorResponse("denied", 403), 7002L);
        ArgumentCaptor<Judge> record = ArgumentCaptor.forClass(Judge.class);
        verify(judges).updateById(record.capture());
        assertEquals(Long.valueOf(7002L), record.getValue().getSubmitId());
        assertEquals(Integer.valueOf(4), record.getValue().getStatus());
        assertEquals("denied", record.getValue().getErrorMessage());
    }
}
