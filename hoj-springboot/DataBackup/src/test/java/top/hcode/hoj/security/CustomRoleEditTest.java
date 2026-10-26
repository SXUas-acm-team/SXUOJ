package top.hcode.hoj.security;

import org.junit.jupiter.api.Test;
import top.hcode.hoj.dao.user.impl.UserRoleEntityServiceImpl;
import static org.junit.jupiter.api.Assertions.*;

class CustomRoleEditTest {
    @Test void customRoleCanBeChangedToAdministratorWithoutIndexingByDatabaseId() {
        String content = new UserRoleEntityServiceImpl().getAuthChangeContent(584510778, 1001);
        assertTrue(content.contains("自定义角色"));
        assertTrue(content.contains("【管理员】"));
        assertFalse(content.contains("普通管理员"));
    }
}
