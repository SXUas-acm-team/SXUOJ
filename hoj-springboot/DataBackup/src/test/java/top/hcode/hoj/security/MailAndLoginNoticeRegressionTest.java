package top.hcode.hoj.security;

import org.junit.jupiter.api.Test;
import top.hcode.hoj.manager.email.EmailManager;
import top.hcode.hoj.dao.user.impl.SessionEntityServiceImpl;
import java.util.Date;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.*;

public class MailAndLoginNoticeRegressionTest {
    @Test public void disablingImplicitSslRequiresStartTls() {
        Properties p = EmailManager.secureMailProperties(false);
        assertEquals("false", p.getProperty("mail.smtp.ssl.enable"));
        assertEquals("true", p.getProperty("mail.smtp.starttls.enable"));
        assertEquals("true", p.getProperty("mail.smtp.starttls.required"));
        assertEquals("true", p.getProperty("mail.smtp.ssl.checkserveridentity"));
    }
    @Test public void implicitTlsStillValidatesServerIdentity() {
        assertEquals("true", EmailManager.secureMailProperties(true).getProperty("mail.smtp.ssl.enable"));
        assertEquals("true", EmailManager.secureMailProperties(true).getProperty("mail.smtp.ssl.checkserveridentity"));
    }
    private static class NoticeService extends SessionEntityServiceImpl {
        private final String response;
        NoticeService(String response) {this.response = response;}
        @Override protected String lookupAddress(String ip) {
            if (response == null) throw new IllegalStateException("provider unavailable");
            return response;
        }
        String notice() {return getRemoteLoginContent("192.0.2.1", "192.0.2.2", new Date(0));}
    }
    @Test public void providerFailureStillProducesSecurityNotice() {
        assertTrue(new NoticeService(null).notice().contains("192.0.2.2"));
    }
    @Test public void missingCityCodeStillProducesSecurityNotice() {
        assertTrue(new NoticeService("{\"addr\":\"same city\"}").notice().contains("192.0.2.2"));
    }
}
