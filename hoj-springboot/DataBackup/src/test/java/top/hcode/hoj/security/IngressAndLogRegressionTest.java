package top.hcode.hoj.security;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockServletContext;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.boot.autoconfigure.web.servlet.error.BasicErrorController;
import org.springframework.boot.autoconfigure.web.ErrorProperties;
import org.springframework.boot.web.servlet.error.DefaultErrorAttributes;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;
import top.hcode.hoj.config.StartupRunner;
import top.hcode.hoj.config.WebMvcConfig;
import top.hcode.hoj.shiro.JwtFilter;
import top.hcode.hoj.utils.IpUtils;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

public class IngressAndLogRegressionTest {
    private static class LookupFilter extends JwtFilter {
        boolean denied(MockHttpServletRequest req, MockHttpServletResponse res) throws Exception {return onAccessDenied(req, res);}
        boolean allowed(MockHttpServletRequest req, MockHttpServletResponse res) {return isAccessAllowed(req, res, null);}
    }
    private static class Filter extends LookupFilter {
        @Override protected org.springframework.web.servlet.HandlerExecutionChain getRequestHandler(javax.servlet.http.HttpServletRequest req) {
            throw new IllegalStateException("mapping failure");
        }
    }

    @Configuration
    @EnableWebMvc
    public static class ResourceMappings implements WebMvcConfigurer {
        @Override public void addResourceHandlers(ResourceHandlerRegistry registry) {
            new WebMvcConfig().addResourceHandlers(registry);
        }
        @Bean public ProtectedController protectedController() { return new ProtectedController(); }
        @Bean public BasicErrorController errorController() {
            return new BasicErrorController(new DefaultErrorAttributes(), new ErrorProperties());
        }
    }

    @RestController
    public static class ProtectedController {
        @PostMapping("/api/file/upload-testcase-zip") public String upload() { throw new AssertionError("Unauthenticated upload invoked"); }
        @GetMapping("/api/public/file/protected-controller") public String protectedDownload() { throw new AssertionError("Protected controller invoked"); }
    }

    private AnnotationConfigWebApplicationContext resourceContext() {
        AnnotationConfigWebApplicationContext context = new AnnotationConfigWebApplicationContext();
        context.setServletContext(new MockServletContext());
        context.register(ResourceMappings.class);
        context.refresh();
        return context;
    }

    private MockHttpServletRequest request(AnnotationConfigWebApplicationContext context, String method, String path) {
        MockHttpServletRequest request = new MockHttpServletRequest(context.getServletContext(), method, path);
        request.setAttribute(DispatcherServlet.WEB_APPLICATION_CONTEXT_ATTRIBUTE, context);
        return request;
    }

    @Test public void configuredPublicResourcesResolveThroughRealSpringMappings() {
        try (AnnotationConfigWebApplicationContext context = resourceContext()) {
            LookupFilter filter = new LookupFilter();
            for (String path : new String[]{"/api/public/img/avatar.png", "/api/public/file/problem.pdf"}) {
                for (String method : new String[]{"GET", "HEAD"}) {
                    assertTrue(filter.allowed(request(context, method, path), new MockHttpServletResponse()));
                }
                assertFalse(filter.allowed(request(context, "POST", path), new MockHttpServletResponse()));
            }
        }
    }

    @Test public void protectedControllersWinBeforeOverlappingResourceMapping() throws Exception {
        try (AnnotationConfigWebApplicationContext context = resourceContext()) {
            LookupFilter filter = new LookupFilter();
            MockHttpServletRequest request = request(context, "GET", "/api/public/file/protected-controller");
            MockHttpServletResponse response = new MockHttpServletResponse();
            assertFalse(filter.allowed(request, response));
            assertFalse(filter.denied(request, response));
            assertEquals(401, response.getStatus());
        }
    }

    @Test public void unknownAndMultipartRequestsRemainDeniedWithRealMappings() throws Exception {
        try (AnnotationConfigWebApplicationContext context = resourceContext()) {
            LookupFilter filter = new LookupFilter();
            for (String path : new String[]{"/api/missing-handler", "/api/file/upload-testcase-zip"}) {
                MockHttpServletRequest request = request(context, "POST", path);
                request.setContentType("multipart/form-data; boundary=x");
                MockHttpServletResponse response = new MockHttpServletResponse();
                assertFalse(filter.allowed(request, response));
                assertFalse(filter.denied(request, response));
                assertEquals(401, response.getStatus());
            }
        }
    }

    @Test public void missingPublicResourceErrorDispatchKeepsIts404Response() {
        try (AnnotationConfigWebApplicationContext context = resourceContext()) {
            LookupFilter filter = new LookupFilter();
            for (String path : new String[]{"/api/public/img/missing.png", "/api/public/file/missing.pdf"}) {
                for (String method : new String[]{"GET", "HEAD"}) {
                    MockHttpServletRequest request = request(context, method, "/error");
                    request.setDispatcherType(DispatcherType.ERROR);
                    request.setAttribute(RequestDispatcher.ERROR_REQUEST_URI, path);
                    request.setAttribute(RequestDispatcher.ERROR_STATUS_CODE, 404);
                    assertTrue(filter.allowed(request, new MockHttpServletResponse()));
                }
            }
        }
    }

    @Test public void directErrorCallsAndProtectedControllerErrorsRemainDenied() {
        try (AnnotationConfigWebApplicationContext context = resourceContext()) {
            LookupFilter filter = new LookupFilter();
            MockHttpServletRequest direct = request(context, "GET", "/error");
            assertFalse(filter.allowed(direct, new MockHttpServletResponse()));
            direct.setAttribute(RequestDispatcher.ERROR_REQUEST_URI, "/api/public/img/missing.png");
            direct.setAttribute(RequestDispatcher.ERROR_STATUS_CODE, 404);
            assertFalse(filter.allowed(direct, new MockHttpServletResponse()));
            for (String path : new String[]{"/api/public/file/protected-controller", "/api/private/problem"}) {
                MockHttpServletRequest request = request(context, "GET", "/error");
                request.setDispatcherType(DispatcherType.ERROR);
                request.setAttribute(RequestDispatcher.ERROR_REQUEST_URI, path);
                request.setAttribute(RequestDispatcher.ERROR_STATUS_CODE, 404);
                assertFalse(filter.allowed(request, new MockHttpServletResponse()));
            }
            direct.setDispatcherType(DispatcherType.ERROR);
            direct.setAttribute(RequestDispatcher.ERROR_STATUS_CODE, 500);
            assertFalse(filter.allowed(direct, new MockHttpServletResponse()));
            direct.setAttribute(RequestDispatcher.ERROR_STATUS_CODE, 404);
            direct.setMethod("POST");
            assertFalse(filter.allowed(direct, new MockHttpServletResponse()));
        }
    }
    @Test public void handlerLookupFailureCannotSkipAuthentication() {
        assertFalse(new Filter().allowed(new MockHttpServletRequest("POST", "/api/file/upload-testcase-zip"), new MockHttpServletResponse()));
    }
    @Test public void uploadWithoutTokenIsRejectedBeforeController() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/file/upload-testcase-zip");
        request.setContentType("multipart/form-data; boundary=x");
        MockHttpServletResponse response = new MockHttpServletResponse();
        assertFalse(new Filter().denied(request, response));
        assertEquals(401, response.getStatus());
    }
    @Test public void directClientCannotSpoofSourceIp() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("192.0.2.1");
        request.addHeader("X-Forwarded-For", "192.0.2.99");
        assertEquals("192.0.2.1", IpUtils.getUserIpAddr(request));
    }
    @Test public void proxyAppendedClientWinsOverSpoofedFirstHop() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("127.0.0.1");
        request.addHeader("X-Forwarded-For", "192.0.2.99, 192.0.2.1");
        assertEquals("192.0.2.1", IpUtils.getUserIpAddr(request));
    }
    @Test public void malformedRemoteAccountConfigDoesNotLogPasswords() {
        Logger logger = (Logger) LoggerFactory.getLogger("hoj");
        ListAppender<ILoggingEvent> capture = new ListAppender<>();
        capture.start(); logger.addAppender(capture);
        try {
            ReflectionTestUtils.invokeMethod(new StartupRunner(), "addRemoteJudgeAccountToMySQL", "HDU",
                    Arrays.asList("user"), Arrays.asList("SENTINEL_PASSWORD_A", "SENTINEL_PASSWORD_B"));
            assertFalse(capture.list.isEmpty());
            for (ILoggingEvent event : capture.list) assertFalse(event.getFormattedMessage().contains("SENTINEL_PASSWORD"));
        } finally {logger.detachAppender(capture); capture.stop();}
    }
}
