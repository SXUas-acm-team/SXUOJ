package top.hcode.hoj.manager.oj;

import org.springframework.stereotype.Component;
import top.hcode.hoj.security.PersistentJwtSecret;
import top.hcode.hoj.security.BackendRuntimeSecrets;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

/** Separate persistent encryption key on the existing backend log volume. */
@Component
public class IdentitySecretStore {
    protected byte[] key() throws Exception {
        String secret = PersistentJwtSecret.loadOrCreate(Paths.get(BackendRuntimeSecrets.JWT_KEY_PATH)
                .resolveSibling("identity.key"));
        return Arrays.copyOf(MessageDigest.getInstance("SHA-256").digest(secret.getBytes(StandardCharsets.UTF_8)), 16);
    }
    public String encrypt(Long cid, String url, String value) {
        try {
            byte[] iv = new byte[12]; new SecureRandom().nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key(), "AES"), new GCMParameterSpec(128, iv));
            cipher.updateAAD((cid + "\n" + url).getBytes(StandardCharsets.UTF_8));
            byte[] encrypted = cipher.doFinal(value.getBytes(StandardCharsets.UTF_8));
            byte[] output = Arrays.copyOf(iv, iv.length + encrypted.length);
            System.arraycopy(encrypted, 0, output, iv.length, encrypted.length);
            return Base64.getEncoder().encodeToString(output);
        } catch (Exception e) { throw new IllegalStateException("无法安全保存核验服务密钥，请检查后端日志挂载目录权限。"); }
    }
    public String decrypt(Long cid, String url, String value) {
        try {
            byte[] data = Base64.getDecoder().decode(value);
            if (data.length < 29) throw new IllegalArgumentException();
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key(), "AES"), new GCMParameterSpec(128, Arrays.copyOf(data, 12)));
            cipher.updateAAD((cid + "\n" + url).getBytes(StandardCharsets.UTF_8));
            return new String(cipher.doFinal(data, 12, data.length - 12), StandardCharsets.UTF_8);
        } catch (Exception e) { throw new IllegalStateException("无法读取核验服务密钥，请管理员重新保存密钥并检查加密密钥挂载。"); }
    }
}
