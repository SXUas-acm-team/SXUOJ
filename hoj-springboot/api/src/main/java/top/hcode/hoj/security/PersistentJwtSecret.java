package top.hcode.hoj.security;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.Set;
import java.util.Arrays;
import java.util.HashSet;

/** An installation-local signing key; never a public seed or a per-start random value. */
public final class PersistentJwtSecret {
    private static final Set<PosixFilePermission> DIRECTORY_MODE = PosixFilePermissions.fromString("rwx------");
    private static final Set<PosixFilePermission> FILE_MODE = PosixFilePermissions.fromString("rw-------");

    private PersistentJwtSecret() { }

    public static synchronized String loadOrCreate(Path requestedFile) {
        Path file = requestedFile.toAbsolutePath().normalize();
        Path directory = file.getParent();
        try {
            if (directory == null || directory.getParent() == null) throw new IOException("Invalid key location");
            Path logDirectory = directory.getParent();
            rejectSymlinkParents(logDirectory);
            if (!Files.isDirectory(logDirectory, LinkOption.NOFOLLOW_LINKS)
                    || !Files.getFileStore(logDirectory).supportsFileAttributeView("posix")) {
                throw new IOException("An existing log directory with POSIX permissions is required");
            }
            try { Files.createDirectory(directory, PosixFilePermissions.asFileAttribute(DIRECTORY_MODE)); }
            catch (FileAlreadyExistsException existing) { /* Validate the existing private directory below. */ }
            validatePrivatePath(directory, true);
            Path lockFile = directory.resolve("jwt.key.lock");
            if (Files.exists(lockFile, LinkOption.NOFOLLOW_LINKS)) validatePrivatePath(lockFile, false);
            try (FileChannel channel = FileChannel.open(lockFile,
                    new HashSet<OpenOption>(Arrays.asList(StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                            LinkOption.NOFOLLOW_LINKS)),
                    PosixFilePermissions.asFileAttribute(FILE_MODE))) {
                validatePrivatePath(lockFile, false);
                try (FileLock ignored = channel.lock()) {
                    if (Files.exists(file, LinkOption.NOFOLLOW_LINKS)) return read(file);
                    Path temporary = Files.createTempFile(directory, ".jwt-", ".tmp",
                            PosixFilePermissions.asFileAttribute(FILE_MODE));
                    try {
                        byte[] encoded = (SecretPolicy.randomSecret() + "\n").getBytes(StandardCharsets.US_ASCII);
                        try (FileChannel output = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                            ByteBuffer buffer = ByteBuffer.wrap(encoded);
                            while (buffer.hasRemaining()) output.write(buffer);
                            output.force(true);
                        }
                        // The private lock serializes processes; publish only a fully written key.
                        Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE);
                        try (FileChannel directoryChannel = FileChannel.open(directory, StandardOpenOption.READ)) {
                            directoryChannel.force(true);
                        }
                        return read(file);
                    } finally { Files.deleteIfExists(temporary); }
                }
            }
        } catch (IOException | UnsupportedOperationException | SecurityException failure) {
            throw new IllegalStateException("Cannot initialize persistent JWT key at " + file
                    + ". Ensure the backend log volume is writable; symlinks, unsafe permissions and corrupt keys are rejected.", failure);
        }
    }

    private static void rejectSymlinkParents(Path path) throws IOException {
        for (Path current = path; current != null; current = current.getParent()) {
            if (Files.isSymbolicLink(current)) throw new IOException("Symbolic links are not allowed for the JWT key path");
        }
    }

    private static void validatePrivatePath(Path path, boolean directory) throws IOException {
        if (Files.isSymbolicLink(path) || (directory ? !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)
                : !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))) {
            throw new IOException("Invalid private JWT key path");
        }
        Set<PosixFilePermission> expected = directory ? DIRECTORY_MODE : FILE_MODE;
        UserPrincipal owner = path.getFileSystem().getUserPrincipalLookupService()
                .lookupPrincipalByName(System.getProperty("user.name"));
        if (!Files.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS).equals(expected)
                || !Files.getOwner(path, LinkOption.NOFOLLOW_LINKS).equals(owner)) {
            throw new IOException("Private JWT key path must be owned by this process user with mode "
                    + (directory ? "0700" : "0600"));
        }
    }

    private static String read(Path file) throws IOException {
        validatePrivatePath(file, false);
        ByteBuffer content = ByteBuffer.allocate(4097);
        try (FileChannel input = FileChannel.open(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            while (content.hasRemaining() && input.read(content) != -1) { /* bounded read */ }
        }
        if (content.position() > 4096) throw new IOException("Persistent JWT key is corrupt");
        String stored = new String(content.array(), 0, content.position(), StandardCharsets.US_ASCII);
        String value = stored.endsWith("\n") ? stored.substring(0, stored.length() - 1) : stored;
        if (!value.matches("[A-Za-z0-9+/]+={0,2}") || !SecretPolicy.isStrongJwt(value)) {
            throw new IOException("Persistent JWT key is corrupt");
        }
        return value;
    }
}
