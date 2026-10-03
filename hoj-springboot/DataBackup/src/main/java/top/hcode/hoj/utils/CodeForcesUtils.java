package top.hcode.hoj.utils;

import top.hcode.hoj.http.SecureHttp;

import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.http.HttpRequest;
import lombok.extern.slf4j.Slf4j;

import javax.script.*;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

@Slf4j(topic = "hoj")
public class CodeForcesUtils {
    private static String RCPC;

    public static String getRCPC() {
        if (RCPC == null){
            HttpRequest request = SecureHttp.get("https://codeforces.com")
                    .timeout(20000);
            String html = request.execute().body();
            List<String> list = ReUtil.findAll("[a-z0-9]+[a-z0-9]{31}", html, 0, new ArrayList<>());
            updateRCPC(list);
        }
        return RCPC;
    }

    public static void updateRCPC(List<String> list) {

        ScriptEngine se = new ScriptEngineManager().getEngineByName("javascript");
        Bindings bindings = se.createBindings();
        bindings.put("string", 4);
        se.setBindings(bindings, ScriptContext.ENGINE_SCOPE);

        String file = ResourceUtil.readUtf8Str("CodeForcesAES.js");
        try {
            se.eval(file);
            // 是否可调用
            if (se instanceof Invocable) {
                Invocable in = (Invocable) se;
                RCPC = (String) in.invokeFunction("getRCPC", list.get(0), list.get(1), list.get(2));
            }
        } catch (ScriptException e) {
            log.error("CodeForcesUtils.updateRCPC throw ScriptException ->", e);
        } catch (NoSuchMethodException e) {
            log.error("CodeForcesUtils.updateRCPC throw NoSuchMethodException ->", e);
        }
    }

    public static void downloadPDF(String urlStr, String savePath) {
        java.nio.file.Path target = java.nio.file.Paths.get(savePath).toAbsolutePath().normalize();
        java.nio.file.Path partial = null;
        java.net.HttpURLConnection connection = null;
        try {
            URL url = new URL(urlStr);
            for (int redirects = 0; ; redirects++) {
                if (!"https".equalsIgnoreCase(url.getProtocol())
                        || !("codeforces.com".equalsIgnoreCase(url.getHost())
                        || "www.codeforces.com".equalsIgnoreCase(url.getHost()))
                        || url.getUserInfo() != null || (url.getPort() != -1 && url.getPort() != 443)) {
                    throw new IOException("PDF origin is not approved");
                }
                connection = (java.net.HttpURLConnection) url.openConnection();
                connection.setInstanceFollowRedirects(false);
                connection.setConnectTimeout(10000);
                connection.setReadTimeout(5000);
                connection.setRequestProperty("User-Agent", "SXUOJ problem importer");
                connection.setRequestProperty("Accept-Encoding", "identity");
                connection.setRequestProperty("Cookie", "RCPC=" + getRCPC());
                int status = connection.getResponseCode();
                if (status >= 300 && status < 400) {
                    if (redirects >= 3) throw new IOException("Too many PDF redirects");
                    String location = connection.getHeaderField("Location");
                    if (location == null) throw new IOException("Invalid PDF redirect");
                    URL next = new URL(url, location);
                    connection.disconnect();
                    url = next;
                    continue;
                }
                if (status != 200 || connection.getContentLengthLong() > 32L * 1024 * 1024) {
                    throw new IOException("PDF response rejected");
                }
                break;
            }
            java.nio.file.Files.createDirectories(target.getParent());
            partial = java.nio.file.Files.createTempFile(target.getParent(), "gym-", ".pdf.part");
            try (InputStream input = connection.getInputStream();
                 java.io.OutputStream output = java.nio.file.Files.newOutputStream(partial)) {
                copyPDF(input, output);
            }
            java.nio.file.Files.move(partial, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to download a bounded PDF", e);
        } finally {
            if (connection != null) connection.disconnect();
            if (partial != null) try { java.nio.file.Files.deleteIfExists(partial); } catch (IOException ignored) { }
        }
    }

    public static void copyPDF(InputStream input, java.io.OutputStream output) throws IOException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(60);
        byte[] header = new byte[5];
        for (int i = 0; i < header.length; i++) {
            int value = input.read();
            if (value < 0) throw new IOException("Incomplete PDF");
            header[i] = (byte) value;
        }
        if (!java.util.Arrays.equals(header, "%PDF-".getBytes(java.nio.charset.StandardCharsets.US_ASCII))) {
            throw new IOException("Response is not a PDF");
        }
        output.write(header);
        long bytes = header.length;
        byte[] buffer = new byte[8192];
        int read;
        while ((read = input.read(buffer)) != -1) {
            bytes += read;
            if (bytes > 32L * 1024 * 1024 || System.nanoTime() > deadline) {
                throw new IOException("PDF transfer limit exceeded");
            }
            output.write(buffer, 0, read);
        }
    }
}