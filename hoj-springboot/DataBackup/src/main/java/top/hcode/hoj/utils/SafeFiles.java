package top.hcode.hoj.utils;

import java.io.*;
import java.nio.file.*;
import java.util.Comparator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/** File boundaries for untrusted problem packages and testcase metadata. */
public final class SafeFiles {
    private static final long MAX_ENTRY_BYTES = 64L * 1024 * 1024;
    private static final long MAX_ARCHIVE_BYTES = 512L * 1024 * 1024;
    private static final int MAX_ENTRIES = 10000;

    private SafeFiles() { }

    public static String basename(String name) {
        if (name == null || name.trim().isEmpty() || name.length() > 255
                || name.equals(".") || name.equals("..") || name.indexOf('/') >= 0
                || name.indexOf('\\') >= 0 || name.indexOf(':') >= 0 || name.indexOf('\0') >= 0
                || name.endsWith(".") || name.endsWith(" ") || name.matches("(?i)(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\\..*)?")) {
            throw new IllegalArgumentException("Invalid file name");
        }
        return name;
    }

    public static File child(String root, String name) {
        basename(name);
        try {
            Path base = new File(root).getCanonicalFile().toPath();
            Path target = base.resolve(name).toFile().getCanonicalFile().toPath();
            if (!target.getParent().equals(base) || Files.isSymbolicLink(base.resolve(name))) {
                throw new IllegalArgumentException("File must remain inside its directory");
            }
            return target.toFile();
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid file path", e);
        }
    }

    public static String uploadDirectory(String directory) {
        if (directory == null || directory.trim().isEmpty()) throw new IllegalArgumentException("Missing upload directory");
        try {
            Path target = new File(directory).getCanonicalFile().toPath();
            Path primary = new File(Constants.File.TESTCASE_TMP_FOLDER.getPath()).getCanonicalFile().toPath();
            Path fallback = new File(System.getProperty("user.home"), "hoj/file/zip").getCanonicalFile().toPath();
            if ((!target.startsWith(primary) || target.equals(primary))
                    && (!target.startsWith(fallback) || target.equals(fallback))) {
                throw new IllegalArgumentException("Testcase upload directory is outside temporary storage");
            }
            if (!Files.isDirectory(target) || Files.isSymbolicLink(Paths.get(directory))) {
                throw new IllegalArgumentException("Invalid testcase upload directory");
            }
            return target.toString();
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid testcase upload directory", e);
        }
    }

    public static void unzip(String archive, String destination) {
        Path root = Paths.get(destination).toAbsolutePath().normalize();
        try {
            if (Files.isSymbolicLink(root)) throw new IOException("Archive destination is a symlink");
            Files.createDirectories(root);
            root = root.toRealPath();
            long archiveBytes = Files.size(Paths.get(archive));
            long total = 0;
            int count = 0;
            try (ZipInputStream input = new ZipInputStream(Files.newInputStream(Paths.get(archive)))) {
                ZipEntry entry;
                byte[] buffer = new byte[8192];
                while ((entry = input.getNextEntry()) != null) {
                    if (++count > MAX_ENTRIES) throw new IOException("Too many archive entries");
                    String name = entry.getName();
                    if (name == null || name.indexOf('\\') >= 0 || name.indexOf(':') >= 0
                            || name.indexOf('\0') >= 0 || name.startsWith("/")) {
                        throw new IOException("Unsafe archive entry");
                    }
                    for (String segment : name.split("/")) basename(segment);
                    Path target = root.resolve(name).normalize();
                    if (!target.startsWith(root) || target.equals(root)) throw new IOException("Unsafe archive entry");
                    Path parent = target.getParent();
                    Files.createDirectories(parent);
                    if (!parent.toRealPath().startsWith(root) || Files.isSymbolicLink(target)) {
                        throw new IOException("Archive symlink is forbidden");
                    }
                    if (entry.isDirectory()) {
                        Files.createDirectories(target);
                        continue;
                    }
                    long expanded = 0;
                    try (OutputStream output = Files.newOutputStream(target, StandardOpenOption.CREATE_NEW)) {
                        int size;
                        while ((size = input.read(buffer)) != -1) {
                            expanded += size;
                            total += size;
                            if (expanded > MAX_ENTRY_BYTES || total > MAX_ARCHIVE_BYTES
                                    || total > Math.max(1024L * 1024, archiveBytes * 200)) {
                                throw new IOException("Archive expansion limit exceeded");
                            }
                            output.write(buffer, 0, size);
                        }
                    }
                }
            }
        } catch (IOException | RuntimeException e) {
            deleteTree(root);
            throw new IllegalArgumentException("Invalid or oversized problem archive", e);
        }
    }

    public static byte[] readLimited(InputStream input, int maxBytes) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int count;
        while ((count = input.read(buffer)) != -1) {
            if (result.size() > maxBytes - count) throw new IOException("File exceeds parser size limit");
            result.write(buffer, 0, count);
        }
        return result.toByteArray();
    }

    public static void deleteTree(Path root) {
        if (root == null || !Files.exists(root, LinkOption.NOFOLLOW_LINKS)) return;
        try (java.util.stream.Stream<Path> files = Files.walk(root)) {
            files.sorted(Comparator.reverseOrder()).forEach(path -> {
                try { Files.deleteIfExists(path); } catch (IOException ignored) { }
            });
        } catch (IOException ignored) { }
    }
}
