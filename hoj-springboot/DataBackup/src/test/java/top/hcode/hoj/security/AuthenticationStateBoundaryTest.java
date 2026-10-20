package top.hcode.hoj.security;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import io.jsonwebtoken.Claims;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.LockedAccountException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import top.hcode.hoj.dao.user.UserInfoEntityService;
import top.hcode.hoj.dao.user.UserRoleEntityService;
import top.hcode.hoj.manager.admin.user.AdminUserManager;
import top.hcode.hoj.manager.file.ProtectedProblemResourceManager;
import top.hcode.hoj.mapper.RoleAuthMapper;
import top.hcode.hoj.pojo.dto.AdminEditUserDTO;
import top.hcode.hoj.pojo.entity.user.Auth;
import top.hcode.hoj.pojo.entity.user.Role;
import top.hcode.hoj.pojo.entity.user.UserInfo;
import top.hcode.hoj.pojo.entity.user.UserRole;
import top.hcode.hoj.pojo.vo.RoleAuthsVO;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.shiro.AccountRealm;
import top.hcode.hoj.shiro.JwtToken;
import top.hcode.hoj.utils.JwtUtils;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Status changes are an authorization boundary, including for previously issued sessions. */
class AuthenticationStateBoundaryTest {
    private static class ExposedRealm extends AccountRealm {
        AuthenticationInfo authenticate() { return doGetAuthenticationInfo(new JwtToken("issued-token")); }
        AuthorizationInfo authorize(String uid) {
            AccountProfile profile = new AccountProfile(); profile.setUid(uid);
            return doGetAuthorizationInfo(new SimplePrincipalCollection(profile, getName()));
        }
    }

    private ExposedRealm realm;
    private UserInfoEntityService users;
    private UserRoleEntityService roles;
    private RoleAuthMapper grants;

    @BeforeEach void setup() {
        realm = new ExposedRealm(); users = mock(UserInfoEntityService.class);
        roles = mock(UserRoleEntityService.class); grants = mock(RoleAuthMapper.class);
        inject(realm, "userInfoEntityService", users); inject(realm, "userRoleEntityService", roles);
        inject(realm, "roleAuthMapper", grants);
        JwtUtils jwt = mock(JwtUtils.class); Claims claims = mock(Claims.class);
        when(jwt.getClaimByToken("issued-token")).thenReturn(claims); when(claims.getSubject()).thenReturn("student");
        inject(realm, "jwtUtils", jwt);
    }

    @AfterEach void cleanup() { RequestContextHolder.resetRequestAttributes(); }
    private static void inject(Object target, String field, Object value) { ReflectionTestUtils.setField(target, field, value); }
    private Role role(long id, String name, Integer status) { return new Role().setId(id).setRole(name).setStatus(status); }
    private Auth permission(String name, Integer status) { return new Auth().setPermission(name).setStatus(status); }
    private RoleAuthsVO grants(Auth... auths) {
        RoleAuthsVO result = new RoleAuthsVO(); result.setAuths(Arrays.asList(auths)); return result;
    }

    @Test void issuedJwtIsRejectedForEveryNonzeroAccountStatus() {
        for (int status : new int[]{-1, 1, 2}) {
            when(users.getOne(any(Wrapper.class), eq(false))).thenReturn(new UserInfo().setUuid("student").setStatus(status));
            assertThrows(LockedAccountException.class, () -> realm.authenticate());
        }
    }

    @Test void activeAccountStillAuthenticatesAndLongLivedIdentityCachesAreDisabled() {
        when(users.getOne(any(Wrapper.class), eq(false))).thenReturn(new UserInfo().setUuid("student").setStatus(0));
        assertNotNull(realm.authenticate());
        assertFalse(realm.isAuthenticationCachingEnabled()); assertFalse(realm.isAuthorizationCachingEnabled());
    }

    @Test void disabledOrUndefinedRoleCannotGrantItsNameOrPermissions() {
        when(roles.getRolesByUid("student")).thenReturn(Arrays.asList(
                role(1L, "root", 1), role(2L, "admin", -1), role(3L, "problem_admin", null)));
        AuthorizationInfo info = realm.authorize("student");
        assertTrue(info.getRoles().isEmpty()); assertTrue(info.getStringPermissions().isEmpty());
        verifyNoInteractions(grants);
    }

    @Test void activeRoleOnlyGrantsActiveNonblankPermissions() {
        when(roles.getRolesByUid("student")).thenReturn(Collections.singletonList(role(1L, "user", 0)));
        when(grants.getRoleAuths(1L)).thenReturn(grants(permission("submit", 0), permission("user_admin", 1),
                permission("discussion_add", -1), permission("reply", null), permission(null, 0), permission(" ", 0)));
        AuthorizationInfo info = realm.authorize("student");
        assertEquals(Collections.singleton("user"), info.getRoles());
        assertEquals(Collections.singleton("submit"), info.getStringPermissions());
    }

    @Test void removedRoleBetweenQueriesDoesNotProduceAnInternalError() {
        when(roles.getRolesByUid("student")).thenReturn(Collections.singletonList(role(1L, "user", 0)));
        when(grants.getRoleAuths(1L)).thenReturn(null);
        assertTrue(realm.authorize("student").getStringPermissions().isEmpty());
    }

    @Test void permissionSnapshotIsSharedWithinOneRequestAndRefreshedOnNextRequest() {
        Role active = role(1L, "user", 0); Auth permission = permission("submit", 0);
        when(roles.getRolesByUid("student")).thenReturn(Collections.singletonList(active));
        when(grants.getRoleAuths(1L)).thenReturn(grants(permission));
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(new MockHttpServletRequest()));
        AuthorizationInfo first = realm.authorize("student");
        permission.setStatus(1);
        assertSame(first, realm.authorize("student"));
        verify(roles, times(1)).getRolesByUid("student"); verify(grants, times(1)).getRoleAuths(1L);
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(new MockHttpServletRequest()));
        assertTrue(realm.authorize("student").getStringPermissions().isEmpty());
        verify(roles, times(2)).getRolesByUid("student"); verify(grants, times(2)).getRoleAuths(1L);
    }

    @Test void requestMemoNeverReusesAnotherAccountsRoles() {
        when(roles.getRolesByUid("root-user")).thenReturn(Collections.singletonList(role(1L, "root", 0)));
        when(roles.getRolesByUid("ordinary-user")).thenReturn(Collections.singletonList(role(2L, "user", 0)));
        when(grants.getRoleAuths(anyLong())).thenReturn(grants());
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(new MockHttpServletRequest()));
        assertEquals(Collections.singleton("root"), realm.authorize("root-user").getRoles());
        assertEquals(Collections.singleton("user"), realm.authorize("ordinary-user").getRoles());
    }

    private AdminUserManager adminManager() {
        AdminUserManager manager = new AdminUserManager(); inject(manager, "userInfoEntityService", users);
        inject(manager, "userRoleEntityService", roles);
        when(users.update(any(Wrapper.class))).thenReturn(true);
        when(roles.getOne(any(Wrapper.class), eq(false))).thenReturn(new UserRole().setUid("student").setRoleId(1002L));
        return manager;
    }

    private AdminEditUserDTO edit(int status) {
        AdminEditUserDTO input = new AdminEditUserDTO(); input.setUid("student"); input.setUsername("student");
        input.setType(1002); input.setStatus(status); input.setSetNewPwd(false); input.setPassword("");
        return input;
    }

    @Test void banningUserRevokesSessionsEvenWithoutAChangeOfPasswordOrRole() throws Exception {
        adminManager().editUser(edit(1)); verify(roles).deleteCache("student", true);
    }

    @Test void legacyNegativeBanStatusAlsoRevokesSessions() throws Exception {
        adminManager().editUser(edit(-1)); verify(roles).deleteCache("student", true);
    }

    @Test void unbanningOrOrdinaryProfileEditDoesNotCreateAnAuthenticatedSession() throws Exception {
        adminManager().editUser(edit(0)); verify(roles, never()).deleteCache(anyString(), anyBoolean());
    }

    @Test void previouslyIssuedRootResourceCapabilityCannotTreatDisabledRoleAsRoot() {
        ProtectedProblemResourceManager manager = new ProtectedProblemResourceManager();
        inject(manager, "userRoleEntityService", roles);
        for (Integer status : Arrays.asList(-1, 1, null)) {
            when(roles.getRolesByUid("student")).thenReturn(Collections.singletonList(role(1L, "root", status)));
            assertEquals(Boolean.FALSE, ReflectionTestUtils.invokeMethod(manager, "isRoot", "student"));
        }
        when(roles.getRolesByUid("student")).thenReturn(Collections.singletonList(role(1L, "root", 0)));
        assertEquals(Boolean.TRUE, ReflectionTestUtils.invokeMethod(manager, "isRoot", "student"));
    }
}
