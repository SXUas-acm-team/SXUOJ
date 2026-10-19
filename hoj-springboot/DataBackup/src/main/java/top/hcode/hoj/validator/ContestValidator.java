package top.hcode.hoj.validator;

import cn.hutool.core.util.ReUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.dao.contest.ContestRegisterEntityService;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.contest.ContestRegister;
import top.hcode.hoj.pojo.entity.contest.ContestAccessPolicy;
import top.hcode.hoj.manager.oj.ContestAccessPolicyManager;
import top.hcode.hoj.pojo.vo.AccessVO;
import top.hcode.hoj.pojo.vo.AdminContestVO;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.utils.Constants;

import javax.annotation.Resource;
import java.util.Date;
import java.util.Objects;
import java.util.List;
import top.hcode.hoj.utils.RequestLimits;

/**
 * @Author: Himit_ZH
 * @Date: 2022/3/11 20:06
 * @Description:
 */
@Component
public class ContestValidator {

    @Resource
    private ContestRegisterEntityService contestRegisterEntityService;

    @Resource
    private ContestEntityService contestEntityService;

    @Autowired
    private GroupValidator groupValidator;

    @Resource
    private CommonValidator commonValidator;

    @Resource
    private ContestAccessPolicyManager contestAccessPolicyManager;

    public boolean isConfidential(Contest contest) {
        return contestAccessPolicyManager.isConfidential(contest);
    }

    public boolean isReleased(Contest contest) {
        return contestAccessPolicyManager.isReleased(contest);
    }

    public boolean isContestAdmin(Contest contest, AccountProfile user, boolean root) {
        String uid = user == null ? null : user.getUid();
        return root || (uid != null && Objects.equals(contest.getUid(), uid))
                || (uid != null && Boolean.TRUE.equals(contest.getIsGroup())
                && groupValidator.isGroupRoot(uid, contest.getGid()));
    }

    public boolean hasPasswordAccess(Contest contest, AccountProfile user) {
        if (user == null) return false;
        ContestRegister registration = contestRegisterEntityService.getOne(new QueryWrapper<ContestRegister>()
                .eq("cid", contest.getId()).eq("uid", user.getUid()), false);
        return registration != null && !Objects.equals(registration.getStatus(), 1);
    }

    public void validateContest(AdminContestVO adminContestVO) throws StatusFailException {
        commonValidator.validateContent(adminContestVO.getTitle(), "比赛标题", 500);
        commonValidator.validateContentLength(adminContestVO.getDescription(), "比赛描述", 65535);

        if (!Objects.equals(Constants.Contest.TYPE_OI.getCode(), adminContestVO.getType())
                && !Objects.equals(Constants.Contest.TYPE_ACM.getCode(), adminContestVO.getType())) {
            throw new StatusFailException("比赛的赛制必须为ACM(0)、OI(1)！");
        }

        if (Objects.equals(Constants.Contest.TYPE_OI.getCode(), adminContestVO.getType())) {
            if (!Objects.equals(Constants.Contest.OI_RANK_RECENT_SCORE.getName(), adminContestVO.getOiRankScoreType())
                    && !Objects.equals(Constants.Contest.OI_RANK_HIGHEST_SCORE.getName(), adminContestVO.getOiRankScoreType())) {
                throw new StatusFailException("OI比赛排行榜得分类型必须为最近得分(Recent)、最高得分(Highest)！");
            }
        }

        if (!Objects.equals(Constants.Contest.AUTH_PUBLIC.getCode(), adminContestVO.getAuth())
                && !Objects.equals(Constants.Contest.AUTH_PRIVATE.getCode(), adminContestVO.getAuth())
                && !Objects.equals(Constants.Contest.AUTH_PROTECT.getCode(), adminContestVO.getAuth())) {
            throw new StatusFailException("比赛的权限必须为公开赛(0)、私有赛(1)、保护赛(2)！");
        }
    }

    public boolean isSealRank(String uid, Contest contest, Boolean forceRefresh, Boolean isRoot) {
        if (isReleased(contest)) return false;
        if (!contest.getSealRank()) {
            return false;
        }
        // 如果是管理员同时选择强制刷新榜单，则封榜无效
        Long gid = contest.getGid();
        boolean isContestAdmin = isRoot || contest.getUid().equals(uid);
        if (forceRefresh && (isContestAdmin || (contest.getIsGroup() && groupValidator.isGroupRoot(uid, gid)))) {
            return false;
        } else if (contest.getSealRank() && contest.getSealRankTime() != null) { // 该比赛开启封榜模式
            Date now = new Date();
            // 如果现在时间处于封榜开始到比赛结束之间
            if (now.after(contest.getSealRankTime()) && now.before(contest.getEndTime())) {
                return true;
            }
            // 或者没有开启赛后自动解除封榜，不可刷新榜单
            return !contest.getAutoRealRank() && now.after(contest.getEndTime());
        }
        return false;
    }


    /**
     * @param contest
     * @param userRolesVo
     * @param isRoot
     * @MethodName validateContestAuth
     * @Description 需要对该比赛做判断，是否处于开始或结束状态才可以获取，同时若是私有赛需要判断是否已注册（比赛管理员包括超级管理员可以直接获取）
     * @Since 2021/1/17
     */
    public void validateContestAuth(Contest contest, AccountProfile userRolesVo, Boolean isRoot) throws StatusFailException, StatusForbiddenException {
        if (contest == null || !Boolean.TRUE.equals(contest.getVisible())) {
            throw new StatusFailException("对不起，该比赛不存在！");
        }
        if (isContestAdmin(contest, userRolesVo, Boolean.TRUE.equals(isRoot))) return;
        ContestAccessPolicy policy = contestAccessPolicyManager.getPolicy(contest.getId());
        if (policy != null) {
            Date now = new Date();
            int status = contestAccessPolicyManager.currentStatus(contest, now);
            if (status == Constants.Contest.STATUS_SCHEDULED.getCode()) {
                throw new StatusForbiddenException("比赛还未开始，您无权访问该比赛！");
            }
            if (contestAccessPolicyManager.isReleased(contest, policy, now)) return;
            if (status == Constants.Contest.STATUS_ENDED.getCode()) {
                throw new StatusForbiddenException("比赛题目已封存，请等待管理员解封！");
            }
            if (!hasPasswordAccess(contest, userRolesVo)) {
                throw new StatusForbiddenException("请先登录并输入比赛密码！");
            }
            return;
        }
        int status = contest.getStatus();
        if (status != Constants.Contest.STATUS_RUNNING.getCode() && status != Constants.Contest.STATUS_ENDED.getCode()) {
            throw new StatusForbiddenException("比赛还未开始，您无权访问该比赛！");
        }
        String uid = userRolesVo == null ? null : userRolesVo.getUid();
        if (Boolean.TRUE.equals(contest.getIsGroup()) && (uid == null
                || !groupValidator.isGroupMember(uid, contest.getGid()))) {
            throw new StatusForbiddenException("对不起，您并非团队内的成员无法参加该团队内的比赛！");
        }
        if (Objects.equals(contest.getAuth(), Constants.Contest.AUTH_PRIVATE.getCode())) {
            if (!hasPasswordAccess(contest, userRolesVo)) {
                throw new StatusForbiddenException("对不起，请先到比赛首页输入比赛密码进行注册！");
            }
            validateAccountAccess(contest, userRolesVo);
        } else if (!Objects.equals(contest.getAuth(), Constants.Contest.AUTH_PUBLIC.getCode())
                && !Objects.equals(contest.getAuth(), Constants.Contest.AUTH_PROTECT.getCode())) {
            throw new StatusForbiddenException("比赛权限配置无效！");
        }
    }


    public void validateJudgeAuth(Contest contest, String uid) throws StatusForbiddenException {
        AccountProfile viewer = (AccountProfile) org.apache.shiro.SecurityUtils.getSubject().getPrincipal();
        if (viewer == null || !Objects.equals(viewer.getUid(), uid)) {
            throw new StatusForbiddenException("请先登录！");
        }
        validateJudgeAuth(contest, viewer, org.apache.shiro.SecurityUtils.getSubject().hasRole("root"));
    }

    public void validateJudgeAuth(Contest contest, AccountProfile user, boolean root) throws StatusForbiddenException {
        if (user == null) throw new StatusForbiddenException("请先登录！");
        try {
            validateContestAuth(contest, user, root);
        } catch (StatusFailException e) {
            throw new StatusForbiddenException(e.getMessage());
        }
        if (isContestAdmin(contest, user, root)) return;
        if (isConfidential(contest)) return; // The unified gate already checks time, seal and password.
        if (Objects.equals(contest.getStatus(), Constants.Contest.STATUS_ENDED.getCode())
                && !Boolean.TRUE.equals(contest.getAllowEndSubmit())) {
            throw new StatusForbiddenException("比赛已结束，不可再提交！");
        }
        if (!Objects.equals(contest.getAuth(), Constants.Contest.AUTH_PUBLIC.getCode())
                && !hasPasswordAccess(contest, user)) {
            throw new StatusForbiddenException("对不起，请先输入比赛密码！");
        }
        validateAccountAccess(contest, user);
    }

    private void validateAccountAccess(Contest contest, AccountProfile user) throws StatusForbiddenException {
        if (Boolean.TRUE.equals(contest.getOpenAccountLimit()) && (user == null
                || !validateAccountRule(contest.getAccountLimitRule(), user.getUsername()))) {
            throw new StatusForbiddenException("对不起！本次比赛只允许特定账号规则的用户参赛！");
        }
    }

    public AccessVO getAccess(Contest contest, AccountProfile user, boolean root) throws StatusFailException {
        if (contest == null || !Boolean.TRUE.equals(contest.getVisible())) {
            throw new StatusFailException("对不起，该比赛不存在！");
        }
        AccessVO access = new AccessVO();
        access.setAccess(hasPasswordAccess(contest, user));
        boolean confidential = isConfidential(contest);
        boolean released = confidential && isReleased(contest);
        access.setReleased(released);
        boolean admin = isContestAdmin(contest, user, root);
        int status = confidential ? contestAccessPolicyManager.currentStatus(contest, new Date()) : contest.getStatus();
        access.setCanViewProblems(false);
        access.setCanSubmit(false);
        if (admin) {
            access.setCanViewProblems(true); access.setCanSubmit(user != null); access.setAccessReason("ADMIN");
        } else if (status == Constants.Contest.STATUS_SCHEDULED.getCode()) {
            access.setAccessReason("SCHEDULED");
        } else if (confidential && status == Constants.Contest.STATUS_ENDED.getCode() && !released) {
            access.setAccessReason("SEALED");
        } else {
            try {
                validateContestAuth(contest, user, root);
                access.setCanViewProblems(true);
            } catch (StatusForbiddenException e) {
                access.setAccessReason(user == null ? "LOGIN_REQUIRED" : "PASSWORD_REQUIRED");
            }
            if (user != null) {
                try { validateJudgeAuth(contest, user, root); access.setCanSubmit(true); }
                catch (StatusForbiddenException ignored) { /* Access is a description, not authorization. */ }
            }
            if (released) access.setAccessReason("RELEASED");
            else if (Boolean.TRUE.equals(access.getCanViewProblems())) access.setAccessReason("READY");
        }
        return access;
    }

    public List<Integer> validateExternalRanks(List<Integer> requested, AccountProfile user, boolean root)
            throws StatusForbiddenException {
        if (requested != null && requested.size() > 10) {
            throw new StatusForbiddenException("关联比赛数量不能超过10场！");
        }
        List<Integer> result = RequestLimits.boundedDistinct(requested, 10);
        if (result.isEmpty()) return null;
        for (Integer id : result) {
            Contest other = contestEntityService.getById(id.longValue());
            boolean released = other != null && isReleased(other);
            if (other == null || !Boolean.TRUE.equals(other.getVisible()) || (!Boolean.TRUE.equals(other.getOpenRank()) && !released)
                    || (!released && !Objects.equals(other.getStatus(), Constants.Contest.STATUS_ENDED.getCode()))
                    || isSealRank(user == null ? null : user.getUid(), other, false, false)) {
                throw new StatusForbiddenException("关联比赛尚未公开最终榜单！");
            }
            try {
                validateContestAuth(other, user, root);
            } catch (StatusFailException e) {
                throw new StatusForbiddenException("关联比赛不可访问！");
            }
        }
        return result;
    }


    public boolean validateAccountRule(String accountRule, String username) {

        String prefix = ReUtil.get("<prefix>([\\s\\S]*?)</prefix>",
                accountRule, 1);
        String suffix = ReUtil.get("<suffix>([\\s\\S]*?)</suffix>",
                accountRule, 1);
        String start = ReUtil.get("<start>([\\s\\S]*?)</start>",
                accountRule, 1);
        String end = ReUtil.get("<end>([\\s\\S]*?)</end>",
                accountRule, 1);
        String extra = ReUtil.get("<extra>([\\s\\S]*?)</extra>",
                accountRule, 1);

        int startNum = Integer.parseInt(start);
        int endNum = Integer.parseInt(end);

        for (int i = startNum; i <= endNum; i++) {
            if (username.equals(prefix + i + suffix)) {
                return true;
            }
        }
        // 额外账号列表
        if (!StringUtils.isEmpty(extra)) {
            String[] accountList = extra.trim().split(" ");
            for (String account : accountList) {
                if (username.equals(account)) {
                    return true;
                }
            }
        }

        return false;
    }
}
