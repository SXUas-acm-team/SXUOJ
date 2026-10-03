package top.hcode.hoj.security;

import java.security.MessageDigest;
import java.security.SecureRandom;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** Validates service secrets without including their values in diagnostics. */
public final class SecretPolicy {
    private SecretPolicy() { }

    public static boolean isPlaceholder(String value) {
        return value == null || value.trim().isEmpty() || "default".equals(value)
                || "hoj-secret-init".equals(value) || "no_judge_token".equals(value)
                || "hoj-judge-init".equals(value) || "hoj123456".equals(value);
    }

    public static boolean isStrongJwt(String value) {
        if (isPlaceholder(value)) return false;
        try { return Base64.getDecoder().decode(value).length >= 64; }
        catch (IllegalArgumentException invalid) { return false; }
    }

    public static boolean isStrongServiceToken(String value) {
        return !isPlaceholder(value) && value.length() >= 32;
    }

    public static String randomSecret() {
        byte[] bytes = new byte[64];
        new SecureRandom().nextBytes(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static boolean matchesServiceToken(String supplied, String configured) {
        return isStrongServiceToken(configured) && supplied != null
                && MessageDigest.isEqual(supplied.getBytes(StandardCharsets.UTF_8),
                configured.getBytes(StandardCharsets.UTF_8));
    }
}
