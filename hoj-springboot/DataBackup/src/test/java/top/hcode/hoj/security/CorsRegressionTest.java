package top.hcode.hoj.security;

import org.apache.shiro.authc.AuthenticationException;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.DefaultCorsProcessor;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import top.hcode.hoj.config.WebMvcConfig;
import top.hcode.hoj.shiro.JwtFilter;
import top.hcode.hoj.utils.JwtUtils;
import top.hcode.hoj.utils.RedisUtils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CorsRegressionTest {
    private static class Registry extends CorsRegistry {
        Map<String, CorsConfiguration> configurations() { return getCorsConfigurations(); }
    }
    private static class Filter extends JwtFilter {
        boolean before(MockHttpServletRequest request, MockHttpServletResponse response) throws Exception { return preHandle(request, response); }
        boolean fail(MockHttpServletRequest request, MockHttpServletResponse response) {
            return onLoginFailure(null, new AuthenticationException("unauthenticated"), request, response);
        }
    }

    @Test void explicitAuthorizationPreflightIsAllowedWithoutAmbientCredentials() throws Exception {
        Registry registry = new Registry();
        new WebMvcConfig().addCorsMappings(registry);
        CorsConfiguration config = registry.configurations().get("/**");
        MockHttpServletRequest request = preflight();
        MockHttpServletResponse response = new MockHttpServletResponse();
        assertTrue(new DefaultCorsProcessor().processRequest(config, request, response));
        assertEquals("*", response.getHeader("Access-Control-Allow-Origin"));
        assertNull(response.getHeader("Access-Control-Allow-Credentials"));
        assertTrue(response.getHeader("Access-Control-Allow-Headers").toLowerCase().contains("authorization"));
    }

    @Test void shiroPreflightDoesNotReflectUntrustedOrNullOrigins() throws Exception {
        for (String origin : new String[]{"https://untrusted.invalid", "null"}) {
            MockHttpServletRequest request = preflight(); request.removeHeader("Origin"); request.addHeader("Origin", origin);
            MockHttpServletResponse response = new MockHttpServletResponse();
            assertFalse(new Filter().before(request, response));
            assertEquals(200, response.getStatus());
            assertEquals("*", response.getHeader("Access-Control-Allow-Origin"));
            assertNull(response.getHeader("Access-Control-Allow-Credentials"));
            assertTrue(response.getHeader("Access-Control-Allow-Headers").contains("Authorization"));
        }
    }

    @Test void authenticationFailureDoesNotReenableCredentialedCors() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/private");
        request.addHeader("Origin", "https://untrusted.invalid");
        MockHttpServletResponse response = new MockHttpServletResponse();
        assertFalse(new Filter().fail(request, response));
        assertEquals(401, response.getStatus());
        assertNull(response.getHeader("Access-Control-Allow-Credentials"));
    }

    @Test void tokenRefreshRemainsVisibleWithoutCredentialedCors() {
        Filter filter = new Filter();
        RedisUtils redis = new RedisUtils();
        RedisTemplate template = mock(RedisTemplate.class);
        ValueOperations values = mock(ValueOperations.class);
        when(template.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(anyString(), anyString(), eq(20L), eq(TimeUnit.SECONDS))).thenReturn(true);
        redis.setRedisTemplate(template);
        JwtUtils tokens = mock(JwtUtils.class);
        ReflectionTestUtils.setField(filter, "redisUtils", redis);
        ReflectionTestUtils.setField(filter, "jwtUtils", tokens);
        when(tokens.refreshToken("member", "old-token")).thenReturn("new-token");
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/private");
        request.addHeader("Authorization", "old-token");
        MockHttpServletResponse response = new MockHttpServletResponse();
        ReflectionTestUtils.invokeMethod(filter, "refreshToken", request, response, "member");
        assertEquals("new-token", response.getHeader("Authorization"));
        assertEquals("true", response.getHeader("Refresh-Token"));
        assertTrue(response.getHeader("Access-Control-Expose-Headers").contains("Authorization"));
        assertNull(response.getHeader("Access-Control-Allow-Credentials"));
        verify(template).execute(any(RedisScript.class), anyList(), anyString());
    }

    private static MockHttpServletRequest preflight() {
        MockHttpServletRequest request = new MockHttpServletRequest("OPTIONS", "/api/private");
        request.addHeader("Origin", "https://untrusted.invalid");
        request.addHeader("Access-Control-Request-Method", "POST");
        request.addHeader("Access-Control-Request-Headers", "Authorization,Content-Type");
        return request;
    }
}
