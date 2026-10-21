package top.hcode.hoj.manager.oj;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.stereotype.Component;
import top.hcode.hoj.common.exception.StatusFailException;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class ContestIdentityClient {
    public static String validateUrl(String value) throws StatusFailException {
        if ("https://sf-zt.gwy.fun".equals(value) || "https://sf-zt.gwy.fun/".equals(value)
                || "https://sf-zt.gwy.fun/cloudxxsaacloud".equals(value) || "https://sf-zt.gwy.fun/cloudxxsaacloud/".equals(value))
            value = ContestIdentityManager.DEFAULT_URL;
        try {
            URI uri = new URI(value);
            if (!"https".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null || uri.getUserInfo() != null
                    || uri.getRawQuery() != null || uri.getRawFragment() != null || value.length() > 1024
                    || !uri.getPath().endsWith("/v1/identities/verify-name")) throw new IllegalArgumentException();
            return uri.toASCIIString();
        } catch (Exception e) { throw new StatusFailException("请输入 HTTPS 姓名核验接口完整地址，路径以 /v1/identities/verify-name 结尾。"); }
    }
    static InetAddress[] resolvePublic(String host) throws UnknownHostException {
        InetAddress[] addresses = InetAddress.getAllByName(host);
        for (InetAddress address : addresses) {
            byte[] bytes = address.getAddress();
            int first = bytes[0] & 255, second = bytes[1] & 255;
            if (address.isAnyLocalAddress() || address.isLoopbackAddress() || address.isLinkLocalAddress()
                    || address.isSiteLocalAddress() || address.isMulticastAddress()
                    || (bytes.length == 4 && (first == 0 || first >= 224 || (first == 100 && second >= 64 && second <= 127)
                    || (first == 198 && (second == 18 || second == 19))))
                    || (bytes.length == 16 && (first & 0xfe) == 0xfc)) {
                throw new UnknownHostException("Identity endpoint must resolve to public addresses");
            }
        }
        return addresses;
    }
    protected CloseableHttpClient newClient() {
        RequestConfig timeouts = RequestConfig.custom().setConnectTimeout(5000).setConnectionRequestTimeout(5000)
                .setSocketTimeout(22000).build();
        // Resolver supplies the validated addresses to the actual socket connection (no second DNS lookup).
        return HttpClients.custom().setDnsResolver(ContestIdentityClient::resolvePublic)
                .disableRedirectHandling().disableAutomaticRetries().setDefaultRequestConfig(timeouts).build();
    }
    public boolean verify(String url, String key, String studentNumber, String name) throws StatusFailException {
        url = validateUrl(url);
        try (CloseableHttpClient client = newClient()) {
            HttpPost request = new HttpPost(url);
            request.setHeader("X-API-Key", key);
            request.setHeader("Content-Type", "application/json; charset=utf-8");
            Map<String, String> body = new LinkedHashMap<>(); body.put("xgh", studentNumber); body.put("name", name);
            request.setEntity(new StringEntity(new ObjectMapper().writeValueAsString(body), StandardCharsets.UTF_8));
            try (CloseableHttpResponse response = client.execute(request)) {
                int status = response.getStatusLine().getStatusCode();
                if (status == 404) return false;
                if (status == 429) throw new StatusFailException("核验服务繁忙，请稍后重试。");
                if (status == 401 || status == 403) throw new StatusFailException("核验服务配置不可用，请联系比赛管理员。");
                if (status != 200 || response.getEntity() == null) throw new StatusFailException("核验服务暂时不可用，请稍后重试。");
                try (InputStream input = response.getEntity().getContent(); ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                    byte[] buffer = new byte[4096]; int count;
                    while ((count = input.read(buffer)) != -1) {
                        if (output.size() + count > 65536) throw new StatusFailException("核验服务响应异常，请联系管理员。");
                        output.write(buffer, 0, count);
                    }
                    return matched(new ObjectMapper().readTree(output.toByteArray()));
                }
            }
        } catch (StatusFailException e) { throw e; }
        catch (UnknownHostException e) { throw new StatusFailException("核验服务域名解析失败或解析到非公网地址，请管理员检查服务器 DNS。"); }
        catch (javax.net.ssl.SSLException e) { throw new StatusFailException("核验服务 HTTPS 证书校验或连接失败，请管理员检查服务证书与服务器时间。"); }
        catch (java.net.SocketTimeoutException | org.apache.http.conn.ConnectTimeoutException e) { throw new StatusFailException("核验服务连接超时，请稍后重试或联系管理员检查后端网络。"); }
        catch (Exception e) { throw new StatusFailException("核验服务连接失败或响应异常，请稍后重试。"); }
    }
    static boolean matched(JsonNode result) throws StatusFailException {
        if (result == null || !result.path("matched").isBoolean()
                || (result.has("mode") && !"live".equals(result.path("mode").asText()))) {
            throw new StatusFailException("核验服务响应异常，未完成核验。");
        }
        return result.get("matched").booleanValue();
    }
}
