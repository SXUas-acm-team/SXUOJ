package top.hcode.hoj.interceptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.manager.file.ProtectedProblemResourceManager;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Component
public class ProtectedProblemResourceInterceptor implements HandlerInterceptor {
    @Autowired private ProtectedProblemResourceManager protectedProblemResourceManager;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        response.setHeader("Cache-Control", "private, no-store, max-age=0");
        response.setHeader("Referrer-Policy", "no-referrer");
        try {
            protectedProblemResourceManager.validateResourceRequest(request);
            return true;
        } catch (StatusForbiddenException e) {
            response.setStatus(403);
            return false;
        } catch (IllegalArgumentException e) {
            response.setStatus(400);
            return false;
        }
    }
}
