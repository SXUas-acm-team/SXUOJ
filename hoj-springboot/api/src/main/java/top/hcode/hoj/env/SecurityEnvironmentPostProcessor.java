package top.hcode.hoj.env;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.config.ConfigFileApplicationListener;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import top.hcode.hoj.security.SecretPolicy;
import top.hcode.hoj.security.BackendRuntimeSecrets;

import java.util.LinkedHashMap;
import java.util.Map;

/** Runs after profile files, before any authentication or datasource bean is built. */
public class SecurityEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {
    @Override
    public int getOrder() { return ConfigFileApplicationListener.DEFAULT_ORDER + 1; }

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment env, SpringApplication app) {
        String name = env.getProperty("spring.application.name", "");
        boolean backend = "hoj-data-backup".equals(name) && env.containsProperty("jwt-token-secret");
        boolean judge = "hoj-judgeserver".equals(name);
        if (!backend && !judge) return;
        boolean nacosEnabled = env.getProperty("spring.cloud.nacos.config.enabled", Boolean.class, true)
                || env.getProperty("spring.cloud.nacos.discovery.enabled", Boolean.class, true);
        if (nacosEnabled) {
            String password = first(env, "NACOS_PASSWORD", "nacos-password", "spring.cloud.nacos.config.password");
            if (backend ? password == null || password.trim().isEmpty()
                    : SecretPolicy.isPlaceholder(password) || "nacos".equals(password) || password.length() < 12) {
                throw new IllegalStateException(backend ? "Nacos requires a configured nonempty NACOS_PASSWORD."
                        : "Nacos requires an explicitly configured strong NACOS_PASSWORD.");
            }
        }
        Map<String, Object> values = new LinkedHashMap<>();
        if (backend) {
            String jwt = first(env, "HOJ_JWT_SECRET", "JWT_TOKEN_SECRET", "hoj.jwt.secret", "jwt-token-secret");
            // Nacos arrives after this phase. Final consumers resolve a weak remote/local
            // value to the same persistent key, while preserving explicit strong env keys.
            values.put("hoj.jwt.secret", jwt == null ? "default" : jwt);
            if (SecretPolicy.isStrongJwt(jwt)) values.put("jwt-token-secret", jwt);
        }
        String token = first(env, "HOJ_JUDGE_TOKEN", "JUDGE_TOKEN", "hoj.judge.token", "judge-token");
        if (!backend && !SecretPolicy.isStrongServiceToken(token)) {
            throw new IllegalStateException("Judge service token is missing or weak; configure the same strong JUDGE_TOKEN for backend and JudgeServer.");
        }
        // A backend placeholder must not shadow the token already held by Nacos.
        if (!backend || !BackendRuntimeSecrets.isJudgePlaceholder(token)) {
            if (!backend) values.put("hoj.judge.token", token);
            values.put("judge-token", token);
        }
        env.getPropertySources().addFirst(new MapPropertySource("validatedServiceSecrets", values));
    }

    private String first(ConfigurableEnvironment env, String... keys) {
        String placeholder = null;
        for (String key : keys) {
            String value = env.getProperty(key);
            if (value != null && !value.trim().isEmpty()) {
                if (!SecretPolicy.isPlaceholder(value)) return value;
                if (placeholder == null) placeholder = value;
            }
        }
        return placeholder;
    }
}
