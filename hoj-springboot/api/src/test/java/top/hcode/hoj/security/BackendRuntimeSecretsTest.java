package top.hcode.hoj.security;

import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.Assert.*;

public class BackendRuntimeSecretsTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private void expectInitializationFailure(Runnable action) {
        try {
            action.run();
            fail("Expected persistent key initialization to fail");
        } catch (IllegalStateException expected) {
            assertNotNull(expected.getMessage());
        }
    }

    private Path keyPath() throws Exception {
        Path logs = temporary.newFolder().toPath();
        Assume.assumeTrue(Files.getFileStore(logs).supportsFileAttributeView("posix"));
        return logs.resolve(".runtime-secrets/jwt.key");
    }

    private StandardEnvironment environment(Map<String, Object> values) {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        environment.getPropertySources().addFirst(new MapPropertySource("fixture", values));
        return environment;
    }

    @Test public void generatedKeySurvivesReloadAndConcurrentConsumersWithPrivatePermissions() throws Exception {
        Path file = keyPath();
        String key = PersistentJwtSecret.loadOrCreate(file);
        assertTrue(SecretPolicy.isStrongJwt(key));
        assertEquals(PosixFilePermissions.fromString("rwx------"), Files.getPosixFilePermissions(file.getParent()));
        assertEquals(PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(file));
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            List<Future<String>> results = new ArrayList<>();
            for (int index = 0; index < 8; index++) results.add(pool.submit(() -> PersistentJwtSecret.loadOrCreate(file)));
            for (Future<String> result : results) assertEquals(key, result.get(5, TimeUnit.SECONDS));
        } finally { pool.shutdownNow(); }
    }

    @Test public void corruptExistingKeyIsRejectedWithoutReplacement() throws Exception {
        Path file = keyPath();
        PersistentJwtSecret.loadOrCreate(file);
        byte[] corrupt = "public-seed\n".getBytes(StandardCharsets.US_ASCII);
        Files.write(file, corrupt);
        expectInitializationFailure(() -> PersistentJwtSecret.loadOrCreate(file));
        assertArrayEquals(corrupt, Files.readAllBytes(file));
    }

    @Test public void keyAndLockSymlinksAreRejectedWithoutFollowingThem() throws Exception {
        for (String name : new String[]{"jwt.key", "jwt.key.lock"}) {
            Path file = keyPath();
            PersistentJwtSecret.loadOrCreate(file);
            Path victim = temporary.newFile().toPath();
            byte[] original = "untouched".getBytes(StandardCharsets.US_ASCII);
            Files.write(victim, original);
            Path linked = file.getParent().resolve(name);
            Files.delete(linked);
            Files.createSymbolicLink(linked, victim);
            expectInitializationFailure(() -> PersistentJwtSecret.loadOrCreate(file));
            assertArrayEquals(original, Files.readAllBytes(victim));
        }
    }

    @Test public void unsafePermissionsAndMissingLogVolumeFailClearly() throws Exception {
        Path file = keyPath();
        PersistentJwtSecret.loadOrCreate(file);
        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-r--r--"));
        expectInitializationFailure(() -> PersistentJwtSecret.loadOrCreate(file));
        Path missing = temporary.getRoot().toPath().resolve("missing/.runtime-secrets/jwt.key");
        expectInitializationFailure(() -> PersistentJwtSecret.loadOrCreate(missing));
    }

    @Test public void finalWeakRemoteBindingsResolveToTheSamePersistentKey() throws Exception {
        Path file = keyPath();
        Map<String, Object> values = new HashMap<>();
        values.put("hoj.runtime-secrets.jwt-path", file.toString());
        values.put("jwt-token-secret", "default");
        StandardEnvironment environment = environment(values);
        String first = BackendRuntimeSecrets.resolveJwt(environment, "old-short-remote-key");
        assertTrue(SecretPolicy.isStrongJwt(first));
        assertEquals(first, BackendRuntimeSecrets.resolveJwt(environment, "hoj-secret-init"));
    }

    @Test public void strongOperatorOrRemoteKeyDoesNotRequireCreatingAFallbackFile() {
        String operator = SecretPolicy.randomSecret();
        String remote = SecretPolicy.randomSecret();
        Map<String, Object> values = new HashMap<>();
        values.put("JWT_TOKEN_SECRET", operator);
        values.put("hoj.runtime-secrets.jwt-path", temporary.getRoot().toPath().resolve("missing/key").toString());
        assertEquals(operator, BackendRuntimeSecrets.resolveJwt(environment(values), remote));
        values.remove("JWT_TOKEN_SECRET");
        assertEquals(remote, BackendRuntimeSecrets.resolveJwt(environment(values), remote));
    }

    @Test public void existingJudgeCredentialsArePreservedAndOnlyMissingCredentialsAreGenerated() {
        assertEquals("short-existing", BackendRuntimeSecrets.resolveJudgeToken("short-existing", "remote-token"));
        assertEquals("hoj123456", BackendRuntimeSecrets.resolveJudgeToken("default", "hoj123456"));
        assertEquals("hoj-judge-init", BackendRuntimeSecrets.resolveJudgeToken("default", "hoj-judge-init"));
        assertTrue(SecretPolicy.isStrongServiceToken(BackendRuntimeSecrets.resolveJudgeToken("default", "no_judge_token")));
    }
}
