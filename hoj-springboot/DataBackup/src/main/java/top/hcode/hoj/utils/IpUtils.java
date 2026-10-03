package top.hcode.hoj.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @Author: Himit_ZH
 * @Date: 2020/10/30 11:12
 * @Description:
 */
@Slf4j(topic = "hoj")
public class IpUtils {

    public static String getUserIpAddr(HttpServletRequest request) {
        String peer = request.getRemoteAddr();
        if (!isTrustedProxy(peer)) return peer == null ? "" : peer;
        String forwarded = request.getHeader("X-Forwarded-For");
        if (forwarded == null || forwarded.length() > 1024) return peer;
        String[] hops = forwarded.split(",");
        for (int i = hops.length - 1; i >= 0; i--) {
            String candidate = hops[i].trim();
            if (!candidate.matches("[0-9a-fA-F:.]+")) return peer;
            if (!isTrustedProxy(candidate)) return candidate;
        }
        return peer;
    }

    private static boolean isTrustedProxy(String ip) {
        if (ip == null) return false;
        if ("127.0.0.1".equals(ip) || "::1".equals(ip) || "0:0:0:0:0:0:0:1".equals(ip)) return true;
        String configured = System.getProperty("hoj.trusted-proxies", System.getenv("HOJ_TRUSTED_PROXIES"));
        if (configured != null) {
            for (String proxy : configured.split(",")) {
                if (ip.equals(proxy.trim())) return true;
            }
        }
        return false;
    }

    public static String getServiceIp() {
        InetAddress address = null;
        try {
            address = InetAddress.getLocalHost();
            return address.getHostAddress(); //返回IP地址
        } catch (UnknownHostException e) {
            log.error("本地ip获取异常---------->{}", e.getMessage());
        }
        return null;
    }
}