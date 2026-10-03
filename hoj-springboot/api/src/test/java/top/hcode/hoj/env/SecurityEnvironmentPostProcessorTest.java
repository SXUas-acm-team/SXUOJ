package top.hcode.hoj.env;

import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.MapPropertySource;
import top.hcode.hoj.security.SecretPolicy;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;

public class SecurityEnvironmentPostProcessorTest {
    private StandardEnvironment environment(String profile, String jwt, String token) {
        StandardEnvironment env = new StandardEnvironment();
        env.setActiveProfiles(profile);
        Map<String, Object> values = new HashMap<>();
        values.put("spring.application.name", "hoj-data-backup");
        values.put("jwt-token-secret", jwt);
        values.put("hoj.jwt.secret", "hoj-secret-init");
        values.put("judge-token", token);
        values.put("spring.cloud.nacos.config.enabled", false);
        values.put("spring.cloud.nacos.discovery.enabled", false);
        env.getPropertySources().addFirst(new MapPropertySource("test", values));
        return env;
    }
    @Test public void localSeedCannotBecomeSigningKey() {
        StandardEnvironment env = environment("local", "default", "no_judge_token");
        new SecurityEnvironmentPostProcessor().postProcessEnvironment(env, new SpringApplication());
        assertTrue(SecretPolicy.isStrongJwt(env.getProperty("hoj.jwt.secret")));
        assertEquals(env.getProperty("hoj.jwt.secret"), env.getProperty("jwt-token-secret"));
        assertTrue(SecretPolicy.isStrongServiceToken(env.getProperty("hoj.judge.token")));
    }
    @Test(expected = IllegalStateException.class) public void productionRejectsPublicSeed() {
        new SecurityEnvironmentPostProcessor().postProcessEnvironment(environment("prod", "default", "default"), new SpringApplication());
    }
    @Test public void explicitSecretIsUsedByBothConsumers() {
        String key = SecretPolicy.randomSecret();
        StandardEnvironment env = environment("prod", key, SecretPolicy.randomSecret());
        Map<String, Object> override = new HashMap<>();
        override.put("JWT_TOKEN_SECRET", key);
        env.getPropertySources().addFirst(new MapPropertySource("operator", override));
        new SecurityEnvironmentPostProcessor().postProcessEnvironment(env, new SpringApplication());
        assertEquals(key, env.getProperty("hoj.jwt.secret"));
    }
    @Test public void knownJudgeFallbackAlwaysRejected() {
        assertFalse(SecretPolicy.matchesServiceToken("no_judge_token", "no_judge_token"));
        assertFalse(SecretPolicy.matchesServiceToken(null, null));
        String token = SecretPolicy.randomSecret();
        assertTrue(SecretPolicy.matchesServiceToken(token, token));
        assertFalse(SecretPolicy.matchesServiceToken("other", token));
    }
}
