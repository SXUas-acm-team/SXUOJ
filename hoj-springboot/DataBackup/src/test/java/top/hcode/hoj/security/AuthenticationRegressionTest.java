package top.hcode.hoj.security;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.utils.JwtUtils;
import top.hcode.hoj.utils.RedisUtils;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

public class AuthenticationRegressionTest {
    private JwtUtils jwt;
    private String secret;
    private Map<String, Object> cache;
    private Runnable onSessionWrite;
    @BeforeEach @SuppressWarnings("unchecked") public void setup() {
        cache = new HashMap<>();
        onSessionWrite = null;
        RedisTemplate<String, Object> template = mock(RedisTemplate.class);
        ValueOperations<String, Object> values = mock(ValueOperations.class);
        when(template.opsForValue()).thenReturn(values);
        when(template.hasKey(anyString())).thenAnswer(i -> cache.containsKey(i.getArgument(0)));
        when(values.get(anyString())).thenAnswer(i -> cache.get(i.getArgument(0)));
        doAnswer(i -> {
            String key = i.getArgument(0);
            cache.put(key, i.getArgument(1));
            if (key.startsWith(top.hcode.hoj.shiro.ShiroConstant.SHIRO_TOKEN_KEY + "session:") && onSessionWrite != null) {
                Runnable pending = onSessionWrite;
                onSessionWrite = null;
                pending.run();
            }
            return null;
        })
                .when(values).set(anyString(), any(), anyLong(), any(TimeUnit.class));
        when(values.setIfAbsent(anyString(), any(), anyLong(), any(TimeUnit.class))).thenAnswer(i -> {
            String key = i.getArgument(0);
            if (cache.containsKey(key)) return false;
            cache.put(key, i.getArgument(1)); return true;
        });
        when(template.expire(anyString(), anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(template.delete(anyCollection())).thenAnswer(i -> {
            for (Object key : (Collection<?>) i.getArgument(0)) cache.remove(key);
            return 1L;
        });
        RedisUtils redis = new RedisUtils();
        redis.setRedisTemplate(template);
        jwt = new JwtUtils();
        secret = SecretPolicy.randomSecret();
        jwt.setSecret(secret);
        jwt.setExpire(3600);
        jwt.setCheckRefreshExpire(1800);
        ReflectionTestUtils.setField(jwt, "redisUtils", redis);
    }
    @Test public void validSignatureWithoutIssuedSessionIsRejected() {
        jwt.generateToken("1");
        String forged = Jwts.builder().setSubject("1").setExpiration(new Date(System.currentTimeMillis() + 3600000))
                .signWith(SignatureAlgorithm.HS512, secret).compact();
        assertNotNull(jwt.getClaimByToken(forged));
        assertFalse(jwt.hasValidSession("1", forged));
    }
    @Test public void separateIssuedSessionsRemainValid() {
        String first = jwt.generateToken("1");
        String second = jwt.generateToken("1");
        assertTrue(jwt.hasValidSession("1", first));
        assertTrue(jwt.hasValidSession("1", second));
        assertFalse(jwt.hasValidSession("another-user", first));
    }
    @Test public void logoutAndLoginCannotReviveOldToken() {
        String old = jwt.generateToken("1");
        jwt.cleanToken("1");
        assertFalse(jwt.hasValidSession("1", old));
        String current = jwt.generateToken("1");
        assertTrue(jwt.hasValidSession("1", current));
        assertFalse(jwt.hasValidSession("1", old));
    }
    @Test public void refreshDeadlineIsIndependentForEachBrowserSession() {
        String first = jwt.generateToken("1");
        String second = jwt.generateToken("1");
        cache.remove(top.hcode.hoj.shiro.ShiroConstant.SHIRO_TOKEN_REFRESH + "1:" + jwt.getClaimByToken(first).getId());
        assertTrue(jwt.needsRefresh("1", first));
        assertFalse(jwt.needsRefresh("1", second));
    }
    @Test public void defaultSigningKeyCannotBeBound() {
        assertThrows(IllegalArgumentException.class, () -> jwt.setSecret("hoj-secret-init"));
    }
    @Test public void refreshPreservesTheAuthenticatedEpoch() {
        String original = jwt.generateToken("1");
        String refreshed = jwt.refreshToken("1", original);
        assertTrue(jwt.hasValidSession("1", refreshed));
        assertEquals(jwt.getClaimByToken(original).get("sessionEpoch"), jwt.getClaimByToken(refreshed).get("sessionEpoch"));
    }
    @Test public void logoutDuringRefreshCannotCreateANewAuthenticatedEpoch() {
        String original = jwt.generateToken("1");
        onSessionWrite = () -> jwt.cleanToken("1");
        assertThrows(IllegalStateException.class, () -> jwt.refreshToken("1", original));
        assertFalse(jwt.hasToken("1"));
        assertFalse(jwt.hasValidSession("1", original));
    }
    @Test public void newLoginDuringRefreshDoesNotMakeOldSessionValid() {
        String original = jwt.generateToken("1");
        String[] newLogin = new String[1];
        onSessionWrite = () -> { jwt.cleanToken("1"); newLogin[0] = jwt.generateToken("1"); };
        assertThrows(IllegalStateException.class, () -> jwt.refreshToken("1", original));
        assertTrue(jwt.hasValidSession("1", newLogin[0]));
        assertFalse(jwt.hasValidSession("1", original));
    }
}
