package top.hcode.hoj.shiro;


import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import io.jsonwebtoken.Claims;
import lombok.extern.slf4j.Slf4j;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.web.filter.authc.AuthenticatingFilter;
import org.apache.shiro.web.util.WebUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.boot.autoconfigure.web.servlet.error.BasicErrorController;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerExecutionChain;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.support.RequestContextUtils;
import top.hcode.hoj.annotation.AnonApi;
import top.hcode.hoj.common.result.CommonResult;
import top.hcode.hoj.common.result.ResultStatus;
import top.hcode.hoj.utils.JwtUtils;
import top.hcode.hoj.utils.RedisUtils;
import top.hcode.hoj.utils.Constants;
import top.hcode.hoj.utils.ServiceContextUtils;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.DispatcherType;
import javax.servlet.RequestDispatcher;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Himit_ZH
 * @Date: 2020/7/19 23:16
 * @Description:
 */
@Component
@Slf4j(topic = "hoj")
public class JwtFilter extends AuthenticatingFilter {

    @Autowired
    private JwtUtils jwtUtils;

    @Autowired
    private RedisUtils redisUtils;

    @Override
    protected boolean isAccessAllowed(ServletRequest request, ServletResponse response, Object mappedValue) {
        HttpServletRequest httpRequest = WebUtils.toHttp(request);
        try {
            HandlerExecutionChain handler = getRequestHandler(httpRequest);
            if (handler == null) return false;
            if (handler.getHandler() instanceof org.springframework.web.servlet.resource.ResourceHttpRequestHandler) {
                return "GET".equals(httpRequest.getMethod()) || "HEAD".equals(httpRequest.getMethod());
            }
            if (!(handler.getHandler() instanceof HandlerMethod)) return false;
            HandlerMethod handlerClazz = (HandlerMethod) handler.getHandler();
            if (isMissingPublicResourceError(httpRequest, handlerClazz)) return true;
            // 判断请求是否访问的是公共接口，如果拥有@AnonApi注解则不再走登录认证，直接访问controller对应的方法
            AnonApi anonApi = ServiceContextUtils.getAnnotation(handlerClazz.getMethod(),
                    handlerClazz.getBeanType(),
                    AnonApi.class);
            if (anonApi != null) {
                // 即使api标记了不用登录，但如果请求头携带了token，可以尝试着进行登录验证。
                String jwt = httpRequest.getHeader("Authorization");
                if (StrUtil.isNotBlank(jwt)) {
                    try {
                        Claims claim = jwtUtils.getClaimByToken(jwt);
                        if (claim == null || jwtUtils.isTokenExpired(claim.getExpiration())) {
                            // 如果已经过期，则不进行登录尝试
                            return true;
                        }
                        String userId = claim.getSubject();
                        boolean hasToken = jwtUtils.hasValidSession(userId, jwt);
                        // 缓存中不存在，说明了token失效，则不进行登录尝试
                        if (!hasToken) {
                            return true;
                        }
                        AccountProfile userRolesVo = (AccountProfile) SecurityUtils.getSubject().getPrincipal();
                        if (userRolesVo == null) {
                            // 尝试手动登录
                            JwtToken jwtToken = new JwtToken(jwt);
                            SecurityUtils.getSubject().login(jwtToken);
                        }
                    } catch (Exception ignored) {
                        // 即使出错，也不影响正常访问无鉴权接口
                    }
                }
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            log.debug("Request handler lookup failed; authentication is required");
            return false;
        }
    }

    private boolean isMissingPublicResourceError(HttpServletRequest request, HandlerMethod handler) throws Exception {
        if (request.getDispatcherType() != DispatcherType.ERROR
                || !Integer.valueOf(404).equals(request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE))
                || (!"GET".equals(request.getMethod()) && !"HEAD".equals(request.getMethod()))
                || !BasicErrorController.class.isAssignableFrom(handler.getBeanType())) return false;
        Object original = request.getAttribute(RequestDispatcher.ERROR_REQUEST_URI);
        if (!(original instanceof String)) return false;
        String originalUri = (String) original;
        String contextPath = request.getContextPath();
        if (!originalUri.startsWith(contextPath)) return false;
        String lookupPath = originalUri.substring(contextPath.length());
        if (!lookupPath.startsWith(Constants.File.IMG_API.getPath())
                && !lookupPath.startsWith(Constants.File.FILE_API.getPath())) return false;
        HttpServletRequest originalRequest = new HttpServletRequestWrapper(request) {
            @Override public String getRequestURI() { return originalUri; }
            @Override public String getServletPath() { return lookupPath; }
            @Override public String getPathInfo() { return null; }
        };
        HandlerExecutionChain originalHandler = getRequestHandler(originalRequest);
        return originalHandler != null && originalHandler.getHandler()
                instanceof org.springframework.web.servlet.resource.ResourceHttpRequestHandler;
    }

    protected HandlerExecutionChain getRequestHandler(HttpServletRequest request) throws Exception {
        WebApplicationContext ctx = RequestContextUtils.findWebApplicationContext(request);
        if (ctx == null) throw new IllegalStateException("Request application context is unavailable");
        List<HandlerMapping> mappings = new ArrayList<>(BeanFactoryUtils
                .beansOfTypeIncludingAncestors(ctx, HandlerMapping.class, true, false).values());
        AnnotationAwareOrderComparator.sort(mappings);
        for (HandlerMapping mapping : mappings) {
            HandlerExecutionChain handler = mapping.getHandler(request);
            if (handler != null) return handler;
        }
        return null;
    }

    @Override
    protected AuthenticationToken createToken(ServletRequest servletRequest, ServletResponse servletResponse) throws Exception {
        // 获取 token
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        String jwt = request.getHeader("Authorization");
        if (StrUtil.isBlank(jwt)) {
            return null;
        }
        return new JwtToken(jwt);
    }


    @Override
    protected boolean onAccessDenied(ServletRequest servletRequest, ServletResponse servletResponse) throws Exception {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        String token = request.getHeader("Authorization");
        if (StrUtil.isBlank(token)) {
            return this.onLoginFailure(null, new AuthenticationException("请先登录！"), servletRequest, servletResponse);
        } else {
            // 判断是否已过期
            Claims claim = jwtUtils.getClaimByToken(token);
            if (claim == null || jwtUtils.isTokenExpired(claim.getExpiration())) {
                return this.onLoginFailure(null,
                        new AuthenticationException("登录状态已失效，请重新登录！"), servletRequest, servletResponse);
            }
            String userId = claim.getSubject();

            // Each issued session has its own token hash; logout revokes the user's epoch.
            boolean hasToken = jwtUtils.hasValidSession(userId, token);
            if (!hasToken) {
                return this.onLoginFailure(null,
                        new AuthenticationException("登录状态已失效，请重新登录！"), servletRequest, servletResponse);
            }
            if (jwtUtils.needsRefresh(userId, token)) {
                //过了需更新token时间，但是还未过期，则进行token刷新
                HttpServletResponse httpResponse = (HttpServletResponse) servletResponse;
                HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
                try {
                    this.refreshToken(httpRequest, httpResponse, userId);
                } catch (IllegalStateException revoked) {
                    return this.onLoginFailure(null, new AuthenticationException("登录状态已失效，请重新登录！"),
                            servletRequest, servletResponse);
                }
            }
        }
        // 执行自动登录
        return executeLogin(servletRequest, servletResponse);
    }

    /**
     * 刷新Token，并更新token到前端
     *
     * @param request
     * @param userId
     * @param response
     * @return
     */
    private void refreshToken(HttpServletRequest request, HttpServletResponse response, String userId) throws IOException {
        String requestId = UUID.randomUUID().toString();
        boolean locked = redisUtils.getLock(ShiroConstant.SHIRO_TOKEN_LOCK + userId, 20, requestId);// 获取锁20s
        if (locked) {
            try {
                String newToken = jwtUtils.refreshToken(userId, request.getHeader("Authorization"));
                response.setHeader("Access-Control-Allow-Credentials", "true");
                response.setHeader("Authorization", newToken); //放到信息头部
                response.setHeader("Access-Control-Expose-Headers", "Refresh-Token,Authorization,Url-Type"); //让前端可用访问
                if (request.getHeader("Url-Type") != null) response.setHeader("Url-Type", request.getHeader("Url-Type"));
                response.setHeader("Refresh-Token", "true"); //告知前端需要刷新token
            } finally {
                redisUtils.releaseLock(ShiroConstant.SHIRO_TOKEN_LOCK + userId, requestId);
            }
        }
    }


    @Override
    protected boolean onLoginFailure(AuthenticationToken token, AuthenticationException e, ServletRequest request, ServletResponse response) {
        returnErrorResponse(request, response, e, ResultStatus.ACCESS_DENIED);
        return false;
    }

    private void returnErrorResponse(ServletRequest request, ServletResponse response, Exception e, ResultStatus resultStatus) {
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        try {
            //处理登录失败的异常
            Throwable throwable = e.getCause() == null ? e : e.getCause();
            CommonResult<Void> result = CommonResult.errorResponse(throwable.getMessage(), resultStatus);
            String json = JSONUtil.toJsonStr(result);
            httpResponse.setContentType("application/json;charset=utf-8");
            httpResponse.setHeader("Access-Control-Expose-Headers", "Refresh-Token,Authorization,Url-Type"); //让前端可用访问
            httpResponse.setHeader("Access-Control-Allow-Credentials", "true");
            if (httpRequest.getHeader("Url-Type") != null) httpResponse.setHeader("Url-Type", httpRequest.getHeader("Url-Type"));
            httpResponse.setStatus(resultStatus.getStatus());
            httpResponse.getWriter().print(json);
        } catch (IOException e1) {
        }
    }

    /**
     * 对跨域提供支持
     */
    @Override
    protected boolean preHandle(ServletRequest request, ServletResponse response) throws Exception {
        HttpServletRequest httpServletRequest = WebUtils.toHttp(request);
        HttpServletResponse httpServletResponse = WebUtils.toHttp(response);
        httpServletResponse.setHeader("Access-control-Allow-Origin", httpServletRequest.getHeader("Origin"));
        httpServletResponse.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS,PUT,DELETE");
        httpServletResponse.setHeader("Access-Control-Allow-Headers", httpServletRequest.getHeader("Access-Control-Request-Headers"));
        httpServletResponse.setHeader("Access-Control-Expose-Headers",
                "Refresh-Token,Authorization,Url-Type,Content-disposition,Content-Type"); //让前端可用访问
        // 跨域时会首先发送一个OPTIONS请求，这里我们给OPTIONS请求直接返回正常状态
        if (httpServletRequest.getMethod().equals(RequestMethod.OPTIONS.name())) {
            httpServletResponse.setStatus(org.springframework.http.HttpStatus.OK.value());
            return false;
        }
        return super.preHandle(request, response);
    }
}
