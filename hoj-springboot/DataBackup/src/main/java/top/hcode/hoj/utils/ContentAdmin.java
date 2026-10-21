package top.hcode.hoj.utils;

import org.apache.shiro.SecurityUtils;

/** Global content authority; never used for accounts, roles or system settings. */
public final class ContentAdmin {
    private ContentAdmin() { }
    public static boolean allowed() {
        return SecurityUtils.getSubject().hasRole("root") || SecurityUtils.getSubject().hasRole("admin");
    }
}
