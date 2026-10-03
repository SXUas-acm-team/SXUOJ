package top.hcode.hoj.http;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.ssl.DefaultSSLInfo;
import org.junit.Test;

import javax.net.ssl.HttpsURLConnection;
import java.lang.reflect.Field;

import static org.junit.Assert.*;

public class HttpTransportSecurityTest {
    @Test public void rejectsPlainHttpAndCredentialsInUrls() {
        for (String url : new String[]{"http://poj.org/login", "https://user:password@example.com/login"}) {
            try { SecureHttp.post(url); fail("Unsafe endpoint accepted"); }
            catch (IllegalArgumentException expected) { }
        }
    }

    @Test public void overridesHutoolInsecureDefaultsAndDisablesRedirects() throws Exception {
        HttpRequest request = SecureHttp.get("https://codeforces.com");
        Object config = field(request, "config");
        assertSame(HttpsURLConnection.getDefaultHostnameVerifier(), field(config, "hostnameVerifier"));
        assertNotSame(DefaultSSLInfo.DEFAULT_SSF, field(config, "ssf"));
        assertEquals(0, field(config, "maxRedirectCount"));
    }

    private static Object field(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
