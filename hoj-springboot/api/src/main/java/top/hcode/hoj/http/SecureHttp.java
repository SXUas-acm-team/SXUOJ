package top.hcode.hoj.http;

import cn.hutool.http.HttpRequest;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.net.URI;
import java.security.GeneralSecurityException;

/** Remote OJ requests must authenticate the server and never downgrade via redirects. */
public final class SecureHttp {
    private static final SSLSocketFactory SOCKET_FACTORY = defaultTrustStore();

    private SecureHttp() { }

    private static SSLSocketFactory defaultTrustStore() {
        try {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, null, null);
            return context.getSocketFactory();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Cannot initialize remote OJ TLS", e);
        }
    }

    public static HttpRequest create(String url) {
        URI uri = URI.create(url);
        if (!"https".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null || uri.getUserInfo() != null) {
            throw new IllegalArgumentException("Remote OJ endpoint requires HTTPS");
        }
        return new HttpRequest(url).setSSLSocketFactory(SOCKET_FACTORY)
                .setHostnameVerifier(HttpsURLConnection.getDefaultHostnameVerifier())
                .setFollowRedirects(false).timeout(20000);
    }

    public static HttpRequest get(String url) { return create(url).method(cn.hutool.http.Method.GET); }
    public static HttpRequest post(String url) { return create(url).method(cn.hutool.http.Method.POST); }
    public static String getBody(String url) { return get(url).execute().body(); }
}
