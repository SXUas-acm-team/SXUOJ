package top.hcode.hoj.manager.oj;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

/** Salted, deliberately expensive invitation hashes. No plaintext invitation is persisted. */
public final class ContestInvitationPassword {
    private ContestInvitationPassword() {}
    private static byte[] derive(String password, byte[] salt) throws Exception {
        PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, 120000, 256);
        try { return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded(); }
        finally { spec.clearPassword(); }
    }
    public static String hash(String password) {
        try {
            byte[] salt = new byte[16]; new SecureRandom().nextBytes(salt);
            return Base64.getEncoder().encodeToString(salt) + ":" + Base64.getEncoder().encodeToString(derive(password, salt));
        } catch (Exception e) { throw new IllegalStateException("无法保存豁免邀请码", e); }
    }
    public static boolean matches(String password, String encoded) {
        try {
            String[] parts = encoded.split(":", -1);
            return parts.length == 2 && MessageDigest.isEqual(Base64.getDecoder().decode(parts[1]),
                    derive(password, Base64.getDecoder().decode(parts[0])));
        } catch (Exception e) { return false; }
    }
}
