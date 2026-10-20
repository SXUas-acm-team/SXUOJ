import cn.hutool.crypto.digest.BCrypt;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/** Offline deployment helper. Reads a password only from stdin, never process arguments. */
public final class NacosPasswordHash {
    public static void main(String[] args) throws Exception {
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String password = input.readLine();
        if (password == null || password.length() < 12 || password.getBytes(StandardCharsets.UTF_8).length > 72) {
            System.err.println("Nacos password must have at least 12 characters and at most 72 UTF-8 bytes.");
            System.exit(64);
        }
        if (args.length == 0) {
            System.out.println(BCrypt.hashpw(password, BCrypt.gensalt(10)));
        } else if (args.length == 1 && "--verify".equals(args[0])) {
            String encoded = input.readLine();
            boolean valid = encoded != null && BCrypt.checkpw(password, encoded);
            System.out.println(valid ? "true" : "false");
            if (!valid) System.exit(1);
        } else {
            System.err.println("Usage: NacosPasswordHash [--verify]; password comes from stdin.");
            System.exit(64);
        }
    }
}
