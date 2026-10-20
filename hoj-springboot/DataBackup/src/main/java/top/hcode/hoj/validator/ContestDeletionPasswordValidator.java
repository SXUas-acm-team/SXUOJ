package top.hcode.hoj.validator;

import cn.hutool.crypto.SecureUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.dao.user.UserInfoEntityService;
import top.hcode.hoj.pojo.entity.user.UserInfo;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.utils.RedisUtils;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Call only after establishing permission to delete the authoritative contest. */
@Component
public class ContestDeletionPasswordValidator {
    private static final int MAX_ATTEMPTS = 5;
    private static final long WINDOW_SECONDS = 1800;
    private static final String ATTEMPT_KEY = "CONTEST_DELETE_PASSWORD_ATTEMPTS:";

    @Resource
    private UserInfoEntityService userInfoEntityService;

    @Resource
    private RedisUtils redisUtils;

    public void validate(AccountProfile operator, String password) throws StatusFailException, StatusForbiddenException {
        if (operator == null || !StringUtils.hasText(operator.getUid())) throw new StatusForbiddenException("请先登录！");
        if (!StringUtils.hasText(password)) throw new StatusFailException("删除比赛前请输入当前登录账号的密码！");
        if (password.length() > 128) throw new StatusFailException("账号密码错误，无法删除比赛！");

        String key = ATTEMPT_KEY + operator.getUid();
        // Reserve each attempt atomically; successful verification clears the failure window.
        long attempts = redisUtils.incr(key, 1);
        if (attempts == 1 && !redisUtils.expire(key, WINDOW_SECONDS)) {
            throw new StatusFailException("暂时无法验证账号密码，请稍后重试！");
        }
        if (attempts > MAX_ATTEMPTS) throw new StatusFailException("账号密码验证失败次数过多，请在30分钟后重试！");

        UserInfo stored = userInfoEntityService.getOne(new QueryWrapper<UserInfo>()
                .select("uuid", "password", "status").eq("uuid", operator.getUid()).eq("status", 0), false);
        if (stored == null || stored.getStatus() != 0) throw new StatusForbiddenException("当前账号不可用，无法删除比赛！");
        String expected = stored.getPassword();
        if (expected == null || !MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8),
                SecureUtil.md5(password).getBytes(StandardCharsets.UTF_8))) {
            throw new StatusFailException("账号密码错误，无法删除比赛！");
        }
        redisUtils.del(key);
    }
}
