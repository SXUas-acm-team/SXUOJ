package top.hcode.hoj.validator;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import cn.hutool.crypto.SecureUtil;
import org.apache.shiro.SecurityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.dao.contest.ContestProblemEntityService;
import top.hcode.hoj.dao.problem.ProblemEntityService;
import top.hcode.hoj.manager.oj.ContestAccessPolicyManager;
import top.hcode.hoj.mapper.ContestProtectedResourceMapper;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.contest.ContestProblem;
import top.hcode.hoj.pojo.entity.problem.Problem;
import top.hcode.hoj.shiro.AccountProfile;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.LinkedHashSet;

/** Applies an unreleased contest's embargo to every route that exposes its problems. */
@Component
public class ProblemContentAccessValidator {
    @Autowired private ContestAccessPolicyManager contestAccessPolicyManager;
    @Autowired private ContestProblemEntityService contestProblemEntityService;
    @Autowired private ContestEntityService contestEntityService;
    @Autowired private ContestValidator contestValidator;
    @Autowired private GroupValidator groupValidator;
    @Autowired private ProblemEntityService problemEntityService;
    @Autowired private ContestProtectedResourceMapper protectedResourceMapper;

    public List<Contest> getUnreleasedContests(Long pid) {
        List<Contest> result = new ArrayList<>();
        String marker = problemMarker(pid);
        Set<Long> cids = new LinkedHashSet<>(protectedResourceMapper.findContestIds(SecureUtil.sha256(marker), marker));
        for (ContestProblem reference : contestProblemEntityService.list(
                new QueryWrapper<ContestProblem>().eq("pid", pid).select("cid"))) {
            cids.add(reference.getCid());
        }
        for (Long cid : cids) {
            Contest contest = contestEntityService.getById(cid);
            if (contest != null && contestAccessPolicyManager.isConfidential(contest)
                    && !contestAccessPolicyManager.isReleased(contest)) result.add(contest);
        }
        return result;
    }

    public static String problemMarker(Long pid) { return "problem:" + pid; }

    public void validateProblemAccess(Long pid, Long currentCid) throws StatusForbiddenException {
        validateProblemAccess(pid, currentCid,
                (AccountProfile) SecurityUtils.getSubject().getPrincipal(),
                SecurityUtils.getSubject().hasRole("root"));
    }

    public void validateProblemAccess(Long pid, Long currentCid, AccountProfile viewer, boolean root)
            throws StatusForbiddenException {
        if (root) return;
        List<Contest> protectedBy = getUnreleasedContests(pid);
        if (protectedBy.isEmpty()) return;

        // A password-verified participant may see the problem inside their live confidential
        // sitting. An old/public contest sharing the same pid must never become a bypass.
        if (currentCid != null && currentCid != 0) {
            for (Contest contest : protectedBy) {
                if (Objects.equals(currentCid, contest.getId())) {
                    try {
                        contestValidator.validateContestAuth(contest, viewer, false);
                        return;
                    } catch (StatusFailException e) {
                        throw new StatusForbiddenException("该保密比赛暂不可访问！");
                    }
                }
            }
        }
        for (Contest contest : protectedBy) {
            if (isContestAdministrator(contest, viewer)) return;
        }
        throw new StatusForbiddenException("该题目属于尚未解封的保密比赛，只能在获准参赛期间于比赛内访问！");
    }

    public boolean canAccessProblem(Long pid, Long currentCid) {
        try {
            validateProblemAccess(pid, currentCid);
            return true;
        } catch (StatusForbiddenException e) {
            return false;
        }
    }

    /** Legacy online-debug requests carry only a pid and the context type. */
    public void validateContestDebugAccess(Long pid) throws StatusForbiddenException {
        List<Contest> protectedBy = getUnreleasedContests(pid);
        if (protectedBy.isEmpty() || SecurityUtils.getSubject().hasRole("root")) return;
        AccountProfile viewer = (AccountProfile) SecurityUtils.getSubject().getPrincipal();
        for (Contest contest : protectedBy) {
            try {
                contestValidator.validateContestAuth(contest, viewer, false);
                return;
            } catch (StatusFailException | StatusForbiddenException ignored) {
                // Another live authorized sitting may reference this same problem.
            }
        }
        throw new StatusForbiddenException("您当前无权调试该保密赛题！");
    }

    public void validateProblemIdAccess(String problemId) throws StatusForbiddenException {
        if (problemId == null || problemId.isEmpty()) return;
        Problem problem = problemEntityService.getOne(new QueryWrapper<Problem>()
                .eq("problem_id", problemId).select("id"), false);
        if (problem != null) validateProblemAccess(problem.getId(), null);
    }

    public boolean canAccessProblemId(String problemId) {
        try {
            validateProblemIdAccess(problemId);
            return true;
        } catch (StatusForbiddenException e) {
            return false;
        }
    }

    public boolean isContestAdministrator(Contest contest, AccountProfile viewer) {
        return viewer != null && (Objects.equals(contest.getUid(), viewer.getUid())
                || (Boolean.TRUE.equals(contest.getIsGroup())
                && groupValidator.isGroupRoot(viewer.getUid(), contest.getGid())));
    }
}
