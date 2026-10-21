package top.hcode.hoj.security;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerExecutionChain;
import top.hcode.hoj.controller.admin.AdminAccountController;
import top.hcode.hoj.pojo.dto.LoginDTO;
import top.hcode.hoj.shiro.JwtFilter;
import javax.servlet.http.HttpServletRequest;
import static org.junit.jupiter.api.Assertions.*;

class AdminLoginPublicAccessTest {
    @Test void loginEndpointCanReachItsCredentialCheckWithoutAnExistingToken() throws Exception {
        HandlerExecutionChain handler = new HandlerExecutionChain(new HandlerMethod(new AdminAccountController(),
                AdminAccountController.class.getMethod("login", LoginDTO.class)));
        class Filter extends JwtFilter {
            @Override protected HandlerExecutionChain getRequestHandler(HttpServletRequest request) { return handler; }
            boolean allowed() { return isAccessAllowed(new MockHttpServletRequest("POST", "/api/admin/login"), new MockHttpServletResponse(), null); }
        }
        assertTrue(new Filter().allowed());
    }
}
