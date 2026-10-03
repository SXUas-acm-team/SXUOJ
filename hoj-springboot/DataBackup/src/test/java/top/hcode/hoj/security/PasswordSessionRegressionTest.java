package top.hcode.hoj.security;

import cn.hutool.crypto.SecureUtil;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusSystemErrorException;
import top.hcode.hoj.dao.user.UserInfoEntityService;
import top.hcode.hoj.manager.oj.AccountManager;
import top.hcode.hoj.manager.oj.PassportManager;
import top.hcode.hoj.pojo.dto.ChangePasswordDTO;
import top.hcode.hoj.pojo.dto.ResetPasswordDTO;
import top.hcode.hoj.pojo.entity.user.UserInfo;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.utils.Constants;
import top.hcode.hoj.utils.JwtUtils;
import top.hcode.hoj.utils.RedisUtils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PasswordSessionRegressionTest {
    private UserInfoEntityService users;
    private RedisUtils redis;
    private RedisTemplate<String, Object> template;
    private ValueOperations<String, Object> values;
    private JwtUtils jwt;
    private AccountManager account;
    private PassportManager passport;

    @BeforeEach @SuppressWarnings("unchecked") void setup() {
        users = mock(UserInfoEntityService.class); jwt = mock(JwtUtils.class);
        template = mock(RedisTemplate.class); values = mock(ValueOperations.class);
        when(template.opsForValue()).thenReturn(values);
        redis = new RedisUtils(); redis.setRedisTemplate(template);
        account = new AccountManager(); passport = new PassportManager();
        for (Object manager : new Object[]{account, passport}) {
            ReflectionTestUtils.setField(manager, "userInfoEntityService", users);
            ReflectionTestUtils.setField(manager, "redisUtils", redis);
            ReflectionTestUtils.setField(manager, "jwtUtils", jwt);
        }
        AccountProfile profile = new AccountProfile(); profile.setUid("owner");
        Subject subject = mock(Subject.class); when(subject.getPrincipal()).thenReturn(profile); ThreadContext.bind(subject);
        when(users.getOne(any(Wrapper.class), eq(false))).thenReturn(new UserInfo().setUuid("owner").setPassword(SecureUtil.md5("old-password")));
        when(users.update(any(Wrapper.class))).thenReturn(true);
    }
    @AfterEach void cleanup() { ThreadContext.unbindSubject(); }

    private ChangePasswordDTO change(String oldPassword) {
        ChangePasswordDTO dto = new ChangePasswordDTO(); dto.setOldPassword(oldPassword); dto.setNewPassword("new-password"); return dto;
    }
    private ResetPasswordDTO reset() {
        ResetPasswordDTO dto = new ResetPasswordDTO(); dto.setUsername("alice"); dto.setPassword("new-password"); dto.setCode("test-reset-code"); return dto;
    }
    private String codeKey() { return Constants.Email.RESET_PASSWORD_KEY_PREFIX.getValue() + "alice"; }

    @Test void successfulPasswordChangeRevokesExistingSessions() throws Exception {
        assertEquals(200, account.changePassword(change("old-password")).getCode());
        verify(jwt).cleanToken("owner");
    }
    @Test void incorrectPasswordDoesNotRevokeSessionsOrWritePassword() throws Exception {
        assertEquals(400, account.changePassword(change("wrong-password")).getCode());
        verifyNoInteractions(jwt); verify(users, never()).update(any(Wrapper.class));
    }
    @Test void failedPasswordWriteDoesNotReportSuccessOrRevokeSessions() {
        when(users.update(any(Wrapper.class))).thenReturn(false);
        assertThrows(StatusSystemErrorException.class, () -> account.changePassword(change("old-password")));
        verifyNoInteractions(jwt);
    }
    @Test void emailPasswordResetRevokesSessionsForThePersistedUser() throws Exception {
        when(values.get(codeKey())).thenReturn("test-reset-code");
        passport.resetPassword(reset());
        verify(jwt).cleanToken("owner"); verify(template).delete(codeKey());
    }
    @Test void expiredResetCodeReturnsBusinessErrorBeforeUserOrSessionChanges() {
        when(template.hasKey(codeKey())).thenReturn(true);
        when(values.get(codeKey())).thenReturn(null);
        assertThrows(StatusFailException.class, () -> passport.resetPassword(reset()));
        verifyNoInteractions(jwt, users);
    }
    @Test void failedResetWriteLeavesCodeAndSessionsUntouched() {
        when(values.get(codeKey())).thenReturn("test-reset-code");
        when(users.update(any(Wrapper.class))).thenReturn(false);
        assertThrows(StatusFailException.class, () -> passport.resetPassword(reset()));
        verifyNoInteractions(jwt); verify(template, never()).delete(codeKey());
    }
}
