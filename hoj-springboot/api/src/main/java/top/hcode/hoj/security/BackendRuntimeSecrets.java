package top.hcode.hoj.security;

import org.springframework.core.env.Environment;
import java.nio.file.Paths;

/** Resolves backend secrets again after Nacos has added its remote property sources. */
public final class BackendRuntimeSecrets {
    public static final String JWT_KEY_PATH = "/hoj/log/backend/.runtime-secrets/jwt.key";
    private BackendRuntimeSecrets() { }

    public static String resolveJwt(Environment environment, String candidate) {
        if (environment != null) {
            for (String name : new String[]{"HOJ_JWT_SECRET", "JWT_TOKEN_SECRET"}) {
                String explicit = environment.getProperty(name);
                if (SecretPolicy.isStrongJwt(explicit)) return explicit;
            }
        }
        if (SecretPolicy.isStrongJwt(candidate)) return candidate;
        if (environment != null) {
            String legacy = environment.getProperty("jwt-token-secret");
            if (SecretPolicy.isStrongJwt(legacy)) return legacy;
        }
        String path = environment == null ? JWT_KEY_PATH
                : environment.getProperty("hoj.runtime-secrets.jwt-path", JWT_KEY_PATH);
        return PersistentJwtSecret.loadOrCreate(Paths.get(path));
    }

    public static String resolveJudgeToken(String explicit, String configured) {
        if (!isJudgePlaceholder(explicit)) return explicit;
        if (!isJudgePlaceholder(configured)) return configured;
        return SecretPolicy.randomSecret();
    }

    public static boolean isJudgePlaceholder(String value) {
        return value == null || value.trim().isEmpty() || "default".equals(value)
                || "no_judge_token".equals(value);
    }

    public static String configuredJudgeToken(Environment environment, String legacy) {
        if (environment != null) {
            for (String name : new String[]{"HOJ_JUDGE_TOKEN", "JUDGE_TOKEN"}) {
                String explicit = environment.getProperty(name);
                if (!isJudgePlaceholder(explicit)) return explicit;
            }
        }
        return legacy;
    }
}
