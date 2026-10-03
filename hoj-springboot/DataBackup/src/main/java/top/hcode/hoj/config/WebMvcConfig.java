package top.hcode.hoj.config;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import top.hcode.hoj.interceptor.AccessInterceptor;
import top.hcode.hoj.utils.Constants;

import java.io.File;

/**
 * 解决跨域问题以及增加注解拦截类
 */
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    private static final String[] EXCLUDE_PATH_PATTERNS = new String[]{
            "/api/admin/**", "/api/file/**", "/api/msg/**", "/api/public/**"
    };

    @Autowired
    private AccessInterceptor accessInterceptor;

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOrigins("*")
                .allowedMethods("GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS")
                .allowCredentials(true)
                .maxAge(3600)
                .allowedHeaders("*");
    }

    // 前端直接通过/public/img/图片名称即可拿到
    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // /api/public/img/** /api/public/file/**
        registry.addResourceHandler(Constants.File.IMG_API.getPath() + "**", Constants.File.FILE_API.getPath() + "**")
                .addResourceLocations("file:" + Constants.File.USER_AVATAR_FOLDER.getPath() + File.separator,
                        "file:" + Constants.File.GROUP_AVATAR_FOLDER.getPath() + File.separator,
                        "file:" + Constants.File.MARKDOWN_FILE_FOLDER.getPath() + File.separator,
                        "file:" + Constants.File.HOME_CAROUSEL_FOLDER.getPath() + File.separator,
                        "file:" + Constants.File.PROBLEM_FILE_FOLDER.getPath() + File.separator);
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new org.springframework.web.servlet.HandlerInterceptor() {
            @Override
            public boolean preHandle(javax.servlet.http.HttpServletRequest request,
                                     javax.servlet.http.HttpServletResponse response, Object handler) {
                response.setHeader("X-Content-Type-Options", "nosniff");
                response.setHeader("Content-Security-Policy", "sandbox; default-src 'none'");
                if (request.getRequestURI().startsWith(Constants.File.FILE_API.getPath())) {
                    response.setHeader("Content-Disposition", "attachment");
                } else {
                    String uri = request.getRequestURI().toLowerCase(java.util.Locale.ROOT);
                    if (!uri.matches(".*\\.(png|jpe?g|gif|webp)$")) {
                        response.setHeader("Content-Disposition", "attachment");
                    }
                }
                return true;
            }
        }).addPathPatterns("/api/public/img/**", "/api/public/file/**");
        registry.addInterceptor(accessInterceptor)
                .addPathPatterns("/api/**")
                .excludePathPatterns(EXCLUDE_PATH_PATTERNS);
    }
}
