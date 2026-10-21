package top.hcode.hoj.manager.oj;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.apache.shiro.SecurityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.dao.contest.*;
import top.hcode.hoj.mapper.*;
import top.hcode.hoj.pojo.dto.*;
import top.hcode.hoj.pojo.entity.contest.*;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.utils.ContentAdmin;
import top.hcode.hoj.utils.RedisUtils;
import top.hcode.hoj.utils.RequestLimits;
import top.hcode.hoj.validator.GroupValidator;
import java.text.Normalizer;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class ContestIdentityManager {
    public static final String DEFAULT_URL = "https://sf-zt.gwy.fun/cloudxxsaacloud/v1/identities/verify-name";
    @Autowired private ContestIdentityConfigMapper configs;
    @Autowired private ContestIdentityBindingMapper bindings;
    @Autowired private ContestEntityService contests;
    @Autowired private ContestRegisterEntityService registrations;
    @Autowired private ContestAccessPolicyManager policies;
    @Autowired private GroupValidator groups;
    @Autowired private ContestIdentityClient client;
    @Autowired private ContestIdentityBindingWriter writer;
    @Autowired private IdentitySecretStore secrets;
    @Autowired private RedisUtils redis;
    @Autowired private top.hcode.hoj.dao.user.UserInfoEntityService users;

    public boolean enabled(Long cid) {
        ContestIdentityConfig config = configs.selectById(cid);
        return config != null && Boolean.TRUE.equals(config.getEnabled());
    }
    public ContestIdentityBinding binding(Long cid, String uid) {
        return uid == null ? null : bindings.selectOne(new QueryWrapper<ContestIdentityBinding>().eq("cid", cid).eq("uid", uid));
    }
    public boolean required(Contest contest, AccountProfile user) {
        return enabled(contest.getId()) && !policies.isReleased(contest)
                && binding(contest.getId(), user == null ? null : user.getUid()) == null;
    }
    public void requireVerified(Contest contest, AccountProfile user) throws StatusForbiddenException {
        if (required(contest, user)) throw new StatusForbiddenException("请先完成本场比赛的学号姓名核验。");
    }
    public Contest authorizeAdmin(Long cid) throws StatusFailException, StatusForbiddenException {
        if (cid == null || cid <= 0) throw new StatusFailException("比赛ID无效。");
        Contest contest = contests.getById(cid);
        if (contest == null) throw new StatusFailException("比赛不存在。");
        AccountProfile user = (AccountProfile) SecurityUtils.getSubject().getPrincipal();
        if (user == null || (!ContentAdmin.allowed() && !Objects.equals(user.getUid(), contest.getUid())
                && !(Boolean.TRUE.equals(contest.getIsGroup()) && groups.isGroupRoot(user.getUid(), contest.getGid())))) {
            throw new StatusForbiddenException("无权管理该比赛。");
        }
        return contest;
    }
    public Map<String, Object> adminConfig(Long cid) throws StatusFailException, StatusForbiddenException {
        authorizeAdmin(cid);
        ContestIdentityConfig config = configs.selectById(cid);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("enabled", config != null && Boolean.TRUE.equals(config.getEnabled()));
        result.put("replaceRealname", config != null && Boolean.TRUE.equals(config.getReplaceRealname()));
        result.put("starPrefixes", config == null ? "" : config.getStarPrefixes());
        result.put("apiUrl", config == null ? DEFAULT_URL : config.getApiUrl());
        result.put("hasApiKey", config != null && StringUtils.hasText(config.getApiKey())
                || StringUtils.hasText(System.getenv("IDENTITY_API_KEY")));
        return result;
    }
    @Transactional(rollbackFor = Exception.class)
    public void saveConfig(Long cid, ContestIdentityConfigDTO input) throws StatusFailException, StatusForbiddenException {
        authorizeAdmin(cid);
        if (contests.getByIdForUpdate(cid) == null) throw new StatusFailException("比赛不存在。");
        if (input == null || input.getEnabled() == null || input.getReplaceRealname() == null) throw new StatusFailException("核验设置不完整。");
        ContestIdentityConfig old = configs.selectById(cid);
        String url = ContestIdentityClient.validateUrl(StringUtils.hasText(input.getApiUrl()) ? input.getApiUrl().trim() : DEFAULT_URL);
        String prefixes = normalizePrefixes(input.getStarPrefixes());
        String key = old == null ? null : old.getApiKey();
        if (StringUtils.hasText(input.getApiKey())) {
            if (input.getApiKey().length() > 1024 || input.getApiKey().chars().anyMatch(c -> c < 33 || c > 126)) {
                throw new StatusFailException("核验服务密钥格式无效。");
            }
            key = secrets.encrypt(cid, url, input.getApiKey());
        } else if (old != null && !Objects.equals(old.getApiUrl(), url) && StringUtils.hasText(key)) {
            throw new StatusFailException("修改接口地址时请重新输入密钥。");
        }
        if (input.getEnabled() && !StringUtils.hasText(key)) {
            String fallback = System.getenv("IDENTITY_API_KEY");
            if (!StringUtils.hasText(fallback) || !DEFAULT_URL.equals(url)) throw new StatusFailException("启用入场核验前请先配置核验服务密钥。");
            key = secrets.encrypt(cid, url, fallback);
        }
        ContestIdentityConfig config = new ContestIdentityConfig(); config.setCid(cid);
        config.setEnabled(input.getEnabled()); config.setReplaceRealname(input.getReplaceRealname());
        config.setStarPrefixes(prefixes); config.setApiUrl(url); config.setApiKey(key);
        if (old == null) configs.insert(config); else configs.updateById(config);
    }
    public static String normalizePrefixes(String value) throws StatusFailException {
        if (!StringUtils.hasText(value)) return "";
        if (value.length() > 1536) throw new StatusFailException("打星前缀过长。");
        LinkedHashSet<String> values = new LinkedHashSet<>();
        for (String prefix : value.trim().split("[,，\\s]+")) {
            if (!prefix.matches("[0-9]{1,64}")) throw new StatusFailException("学号前缀只能包含数字，多个前缀用逗号分隔。");
            values.add(prefix);
        }
        if (values.size() > 20) throw new StatusFailException("最多设置20个学号前缀。");
        return String.join(",", values);
    }
    private void reserve(String key, long limit) throws StatusFailException {
        long attempts = redis.incr(key, 1);
        if (attempts == 1 && !redis.expire(key, 60)) throw new StatusFailException("核验限流服务不可用，请稍后重试。");
        if (attempts > limit) throw new StatusFailException("核验请求过于频繁，请一分钟后重试。");
    }
    public void verify(ContestIdentityVerifyDTO input) throws StatusFailException, StatusForbiddenException {
        AccountProfile user = (AccountProfile) SecurityUtils.getSubject().getPrincipal();
        if (user == null) throw new StatusForbiddenException("请先登录。");
        if (input == null || input.getCid() == null || input.getStudentNumber() == null || input.getName() == null
                || input.getStudentNumber().length() > 64 || input.getName().length() > 256) throw new StatusFailException("请填写有效的学号和姓名。");
        String number = input.getStudentNumber().trim();
        String name = Normalizer.normalize(input.getName(), Normalizer.Form.NFKC).trim();
        if (!number.matches("[A-Za-z0-9_.@-]{1,64}") || name.isEmpty() || name.length() > 128
                || name.chars().anyMatch(Character::isISOControl)) throw new StatusFailException("请填写有效的学号和姓名。");
        Contest contest = contests.getById(input.getCid());
        if (contest == null || !Boolean.TRUE.equals(contest.getVisible())) throw new StatusFailException("比赛不存在。");
        ContestIdentityConfig config = configs.selectById(input.getCid());
        if (config == null || !Boolean.TRUE.equals(config.getEnabled())) throw new StatusFailException("本场比赛未开启入场核验。");
        if (policies.isConfidential(contest) && policies.currentStatus(contest, new Date()) == 1) throw new StatusFailException("比赛已结束，无需入场核验。");
        if (Boolean.TRUE.equals(contest.getIsGroup()) && !policies.isConfidential(contest)
                && !ContentAdmin.allowed() && !groups.isGroupMember(user.getUid(), contest.getGid())) throw new StatusForbiddenException("您不是比赛所属团队成员。");
        if (Integer.valueOf(1).equals(contest.getAuth())) {
            ContestRegister registration = registrations.getOne(new QueryWrapper<ContestRegister>()
                    .eq("cid", contest.getId()).eq("uid", user.getUid()), false);
            if (registration == null || Integer.valueOf(1).equals(registration.getStatus())) throw new StatusForbiddenException("请先输入比赛密码。");
        }
        ContestIdentityBinding old = binding(contest.getId(), user.getUid());
        if (old != null) {
            if (number.equals(old.getStudentNumber()) && name.equals(old.getVerifiedName())) return;
            throw new StatusFailException("本场比赛已绑定身份，不能更换学号或姓名。");
        }
        reserve("CONTEST_IDENTITY_USER:" + user.getUid(), 5);
        reserve("CONTEST_IDENTITY_SERVICE", 100);
        if (!StringUtils.hasText(config.getApiKey())) throw new StatusFailException("核验服务未配置，请联系管理员。");
        String key = secrets.decrypt(contest.getId(), config.getApiUrl(), config.getApiKey());
        if (!client.verify(config.getApiUrl(), key, number, name)) throw new StatusFailException("学号与姓名不匹配，请检查后重试。");
        writer.bind(config, user.getUid(), number, name);
    }
    public <T> void decorateRanks(Long cid, List<T> rows, Function<T, String> uid, Function<T, String> username,
                                  BiConsumer<T, String> realname, Map<String, Boolean> stars) {
        ContestIdentityConfig config = configs.selectById(cid);
        if (config == null || rows.isEmpty()) return;
        Map<String, ContestIdentityBinding> byUid = bindings.selectList(new QueryWrapper<ContestIdentityBinding>().eq("cid", cid))
                .stream().collect(Collectors.toMap(ContestIdentityBinding::getUid, b -> b));
        List<String> prefixes = StringUtils.hasText(config.getStarPrefixes()) ? Arrays.asList(config.getStarPrefixes().split(",")) : Collections.emptyList();
        for (T row : rows) {
            ContestIdentityBinding binding = byUid.get(uid.apply(row));
            if (Boolean.TRUE.equals(config.getReplaceRealname())) realname.accept(row, binding == null ? "" : binding.getVerifiedName());
            if (binding != null && prefixes.stream().anyMatch(binding.getStudentNumber()::startsWith)) {
                stars.put(RequestLimits.normalizeAccount(username.apply(row)), true);
            }
        }
    }
    public void delete(Long cid) {
        bindings.delete(new QueryWrapper<ContestIdentityBinding>().eq("cid", cid));
        configs.deleteById(cid);
    }
    public List<String> automaticStarAccounts(Long cid) {
        ContestIdentityConfig config = configs.selectById(cid);
        if (config == null || !StringUtils.hasText(config.getStarPrefixes())) return Collections.emptyList();
        List<String> prefixes = Arrays.asList(config.getStarPrefixes().split(","));
        List<String> uids = bindings.selectList(new QueryWrapper<ContestIdentityBinding>().eq("cid", cid)).stream()
                .filter(b -> prefixes.stream().anyMatch(b.getStudentNumber()::startsWith))
                .map(ContestIdentityBinding::getUid).collect(Collectors.toList());
        List<String> result = new ArrayList<>();
        for (int i = 0; i < uids.size(); i += 500) {
            result.addAll(users.list(new QueryWrapper<top.hcode.hoj.pojo.entity.user.UserInfo>()
                    .select("username").in("uuid", uids.subList(i, Math.min(i + 500, uids.size())))).stream()
                    .map(top.hcode.hoj.pojo.entity.user.UserInfo::getUsername).collect(Collectors.toList()));
        }
        return result;
    }
}
