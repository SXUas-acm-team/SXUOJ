package top.hcode.hoj.security;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;
import top.hcode.hoj.manager.file.ImportFpsProblemManager;
import top.hcode.hoj.manager.file.ImportHydroProblemManager;
import top.hcode.hoj.utils.CodeForcesUtils;
import top.hcode.hoj.utils.SafeFiles;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.zip.*;

import static org.junit.jupiter.api.Assertions.*;

class UntrustedFilesTest {
    @TempDir Path temporary;

    @Test void rejectsTraversalWithoutWritingOutsideDestination() throws Exception {
        Path archive = archive("../escaped.txt", "secret".getBytes(StandardCharsets.UTF_8));
        Path output = temporary.resolve("extracted");
        assertThrows(IllegalArgumentException.class, () -> SafeFiles.unzip(archive.toString(), output.toString()));
        assertFalse(Files.exists(temporary.resolve("escaped.txt")));
        assertFalse(Files.exists(output));
        assertThrows(IllegalArgumentException.class, () -> SafeFiles.child(temporary.toString(), "../config.yml"));
        assertThrows(IllegalArgumentException.class, () -> SafeFiles.child(temporary.toString(), "C:\\config.yml"));
        assertThrows(IllegalArgumentException.class, () -> SafeFiles.uploadDirectory(temporary.toString()));
    }

    @Test void limitsExpansionAndRemovesPartialExtraction() throws Exception {
        Path archive = archive("large.txt", new byte[2 * 1024 * 1024]);
        Path output = temporary.resolve("large");
        assertThrows(IllegalArgumentException.class, () -> SafeFiles.unzip(archive.toString(), output.toString()));
        assertFalse(Files.exists(output));
    }

    @Test void acceptsAnOrdinaryTestcaseArchive() throws Exception {
        Path archive = archive("case/1.in", "1 2\n".getBytes(StandardCharsets.UTF_8));
        Path output = temporary.resolve("valid");
        SafeFiles.unzip(archive.toString(), output.toString());
        assertEquals("1 2\n", new String(Files.readAllBytes(output.resolve("case/1.in")), StandardCharsets.UTF_8));
    }

    @Test void rejectsYamlObjectTagsAndAliasExpansion() {
        assertThrows(RuntimeException.class, () -> ImportHydroProblemManager.safeYaml().load("!!javax.script.ScriptEngineManager []"));
        StringBuilder yaml = new StringBuilder("a: &list [1, 2]\nb: [");
        for (int i = 0; i < 25; i++) yaml.append(i == 0 ? "*list" : ", *list");
        yaml.append("]");
        assertThrows(RuntimeException.class, () -> ImportHydroProblemManager.safeYaml().load(yaml.toString()));
        assertNotNull(ImportHydroProblemManager.safeYaml().load("type: default\ntime: 1s\n"));
    }

    @Test void rejectsXmlDoctypeBeforeAccessingProblemServices() {
        String xml = "<!DOCTYPE fps [<!ENTITY secret SYSTEM 'file:///untrusted-test'>]><fps><title>&secret;</title></fps>";
        ImportFpsProblemManager manager = new ImportFpsProblemManager();
        assertThrows(RuntimeException.class, () -> ReflectionTestUtils.invokeMethod(manager, "parseFps",
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), "test-user", new ArrayList<String>()));
    }

    @Test void boundsPdfBodiesAndRejectsHtmlDisguisedAsPdf() throws Exception {
        assertThrows(IOException.class, () -> CodeForcesUtils.copyPDF(new ByteArrayInputStream("<html>".getBytes()), new ByteArrayOutputStream()));
        byte[] pdf = "%PDF-1.7\nminimal".getBytes(StandardCharsets.US_ASCII);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        CodeForcesUtils.copyPDF(new ByteArrayInputStream(pdf), output);
        assertArrayEquals(pdf, output.toByteArray());
        InputStream endless = new InputStream() {
            int header;
            public int read() { return header < 5 ? "%PDF-".charAt(header++) : 0; }
            public int read(byte[] buffer, int offset, int length) { java.util.Arrays.fill(buffer, offset, offset + length, (byte) 0); return length; }
        };
        assertThrows(IOException.class, () -> CodeForcesUtils.copyPDF(endless, new OutputStream() { public void write(int value) { } public void write(byte[] data, int offset, int length) { } }));
    }

    private Path archive(String name, byte[] data) throws IOException {
        Path archive = Files.createTempFile(temporary, "package-", ".zip");
        try (ZipOutputStream output = new ZipOutputStream(Files.newOutputStream(archive))) {
            output.putNextEntry(new ZipEntry(name));
            output.write(data);
            output.closeEntry();
        }
        return archive;
    }
}
