package top.hcode.hoj.security;

import org.junit.Test;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.config.StartupRunner;
import top.hcode.hoj.manager.admin.system.ConfigManager;
import top.hcode.hoj.pojo.vo.ConfigVO;
import top.hcode.hoj.utils.JwtUtils;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class BackendStartupSecretsCompatibilityTest {
    @Test public void defaultEnvironmentPreservesExistingJudgeTokenAndPublishesActualSigningKey() {
        ConfigVO config = new ConfigVO();
        config.setJudgeToken("existing-legacy-judge-token");
        String signingKey = SecretPolicy.randomSecret();
        initialize(config, "default", signingKey);
        assertEquals("existing-legacy-judge-token", config.getJudgeToken());
        assertEquals(signingKey, config.getTokenSecret());
    }

    @Test public void explicitLegacyClientTokenStillOverridesRemoteWithoutLengthRestriction() {
        ConfigVO config = new ConfigVO();
        config.setJudgeToken("remote-existing-token");
        initialize(config, "short", SecretPolicy.randomSecret());
        assertEquals("short", config.getJudgeToken());
    }

    @Test public void missingTokensGetStrongGeneratedValueBeforePublishing() {
        ConfigVO config = new ConfigVO();
        config.setJudgeToken("no_judge_token");
        initialize(config, "default", SecretPolicy.randomSecret());
        assertTrue(SecretPolicy.isStrongServiceToken(config.getJudgeToken()));
    }

    @Test public void lateRemoteWeakKeyCannotOverrideStrongOperatorKeyInEitherConsumer() {
        String signingKey = SecretPolicy.randomSecret();
        Map<String, Object> values = new HashMap<>();
        values.put("JWT_TOKEN_SECRET", signingKey);
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("operator", values));
        JwtUtils jwt = new JwtUtils();
        ReflectionTestUtils.setField(jwt, "environment", environment);
        jwt.setSecret("old-remote-weak-key");
        ConfigVO config = new ConfigVO();
        config.bindTokenSecret("old-remote-weak-key", environment);
        assertEquals(signingKey, jwt.getSecret());
        assertEquals(jwt.getSecret(), config.getTokenSecret());
    }

    private void initialize(ConfigVO config, String legacyJudgeToken, String signingKey) {
        StartupRunner runner = new StartupRunner();
        JwtUtils jwt = mock(JwtUtils.class);
        when(jwt.getSecret()).thenReturn(signingKey);
        ConfigManager manager = mock(ConfigManager.class);
        ReflectionTestUtils.setField(runner, "configVo", config);
        ReflectionTestUtils.setField(runner, "configManager", manager);
        ReflectionTestUtils.setField(runner, "jwtUtils", jwt);
        ReflectionTestUtils.setField(runner, "judgeToken", legacyJudgeToken);
        ReflectionTestUtils.invokeMethod(runner, "initDefaultConfig");
        verify(manager).sendNewConfigToNacos();
    }
}
