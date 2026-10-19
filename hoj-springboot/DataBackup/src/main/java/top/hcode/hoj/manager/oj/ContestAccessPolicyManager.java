package top.hcode.hoj.manager.oj;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.dao.contest.ContestAccessPolicyEntityService;
import top.hcode.hoj.mapper.ContestProtectedResourceMapper;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.contest.ContestAccessPolicy;
import top.hcode.hoj.pojo.vo.AdminContestVO;
import top.hcode.hoj.pojo.vo.ContestVO;
import top.hcode.hoj.utils.Constants;

import javax.annotation.Resource;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Policy reads deliberately bypass application caches; failures must never become legacy access. */
@Component
public class ContestAccessPolicyManager {
    @Resource
    private ContestAccessPolicyEntityService contestAccessPolicyEntityService;

    @Resource
    private ContestProtectedResourceMapper contestProtectedResourceMapper;

    public ContestAccessPolicy getPolicy(Long cid) {
        return contestAccessPolicyEntityService.getById(cid);
    }

    public List<Long> listConfidentialContestIds() {
        return contestAccessPolicyEntityService.list().stream()
                .map(ContestAccessPolicy::getCid).collect(Collectors.toList());
    }

    public boolean isConfidential(Contest contest) {
        return contest != null && getPolicy(contest.getId()) != null;
    }

    public boolean isReleased(Contest contest) {
        return contest != null && isReleased(contest, getPolicy(contest.getId()), new Date());
    }

    public boolean isReleased(Contest contest, ContestAccessPolicy policy, Date now) {
        return Boolean.TRUE.equals(contest.getVisible()) && policy != null
                && contest.getEndTime() != null && !now.before(contest.getEndTime())
                && policy.getReleaseAt() != null && !now.before(policy.getReleaseAt());
    }

    public int currentStatus(Contest contest, Date now) {
        if (contest.getStartTime() == null || contest.getEndTime() == null) {
            return contest.getStatus();
        }
        if (now.before(contest.getStartTime())) return Constants.Contest.STATUS_SCHEDULED.getCode();
        if (!now.before(contest.getEndTime())) return Constants.Contest.STATUS_ENDED.getCode();
        return Constants.Contest.STATUS_RUNNING.getCode();
    }

    public void decorate(ContestVO info) {
        decorate(info, getPolicy(info.getId()), new Date());
    }

    public void decorateAll(List<ContestVO> contests) {
        if (contests.isEmpty()) return;
        List<Long> ids = contests.stream().map(ContestVO::getId).distinct().collect(Collectors.toList());
        Map<Long, ContestAccessPolicy> policies = contestAccessPolicyEntityService.list(
                new QueryWrapper<ContestAccessPolicy>().in("cid", ids)).stream()
                .collect(Collectors.toMap(ContestAccessPolicy::getCid, p -> p));
        Date now = new Date();
        contests.forEach(c -> decorate(c, policies.get(c.getId()), now));
    }

    private void decorate(ContestVO info, ContestAccessPolicy policy, Date now) {
        info.setConfidential(policy != null);
        info.setReleaseAt(policy == null ? null : policy.getReleaseAt());
        boolean released = policy != null && info.getEndTime() != null && !now.before(info.getEndTime())
                && policy.getReleaseAt() != null && !now.before(policy.getReleaseAt());
        info.setReleased(released);
        if (policy != null) {
            if (!released) info.setDescription(null);
            Contest clock = new Contest().setStartTime(info.getStartTime()).setEndTime(info.getEndTime())
                    .setStatus(info.getStatus());
            info.setStatus(currentStatus(clock, now));
        }
        if (released) {
            info.setAllowEndSubmit(true);
            info.setOpenRank(true);
        }
    }

    public void decorate(AdminContestVO info) {
        ContestAccessPolicy policy = getPolicy(info.getId());
        info.setConfidential(policy != null);
        info.setReleaseAt(policy == null ? null : policy.getReleaseAt());
    }

    /** Called within the contest save transaction, after locking the existing contest row. */
    public void prepare(Contest contest, AdminContestVO input, Contest oldContest) throws StatusFailException {
        ContestAccessPolicy oldPolicy = oldContest == null ? null : getPolicy(oldContest.getId());
        boolean enabled = input.getConfidential() == null ? oldPolicy != null : input.getConfidential();
        if (!enabled) return;
        contest.setAuth(Constants.Contest.AUTH_PRIVATE.getCode()).setOpenAccountLimit(false);
        if (!StringUtils.hasText(contest.getPwd())) throw new StatusFailException("保密赛必须设置比赛密码！");
        if (contest.getStartTime() == null || contest.getEndTime() == null
                || !contest.getStartTime().before(contest.getEndTime())) {
            throw new StatusFailException("保密赛的开始和结束时间必须有效，且结束时间晚于开始时间！");
        }
        if (Boolean.TRUE.equals(input.getConfidential()) && input.getReleaseAt() != null
                && input.getReleaseAt().before(contest.getEndTime())) {
            throw new StatusFailException("解封时间不得早于比赛结束时间！");
        }
    }

    public void savePolicy(Contest contest, AdminContestVO input, Contest oldContest) throws StatusFailException {
        if (input.getConfidential() == null) {
            // An old admin page must not remove a policy. Rescheduling starts a fresh sealed stage.
            if (oldContest != null && (!Objects.equals(oldContest.getStartTime(), contest.getStartTime())
                    || !Objects.equals(oldContest.getEndTime(), contest.getEndTime()))
                    && getPolicy(contest.getId()) != null) {
                writeReleaseTime(contest.getId(), null);
            }
            return;
        }
        ContestAccessPolicy oldPolicy = getPolicy(contest.getId());
        if (!input.getConfidential()) {
            if (oldPolicy != null && !contestAccessPolicyEntityService.removeById(contest.getId())) {
                throw new StatusFailException("取消保密设置失败！");
            }
        } else if (oldPolicy == null) {
            if (!contestAccessPolicyEntityService.save(new ContestAccessPolicy()
                    .setCid(contest.getId()).setReleaseAt(input.getReleaseAt()))) {
                throw new StatusFailException("保存保密设置失败！");
            }
        } else {
            writeReleaseTime(contest.getId(), input.getReleaseAt());
        }
    }

    private void writeReleaseTime(Long cid, Date releaseAt) throws StatusFailException {
        // Explicit SET is required: MyBatis's default non-null strategy cannot reseal a row.
        if (!contestAccessPolicyEntityService.update(new UpdateWrapper<ContestAccessPolicy>()
                .eq("cid", cid).set("release_at", releaseAt))) {
            throw new StatusFailException("保存解封时间失败！");
        }
    }

    public void deletePolicy(Long cid) throws StatusFailException {
        if (getPolicy(cid) != null && !contestAccessPolicyEntityService.removeById(cid)) {
            throw new StatusFailException("删除比赛保密设置失败！");
        }
        // Remove the policy first: its write interceptor remembers the old references before deletion.
        long expected = contestProtectedResourceMapper.countByContestId(cid);
        int removed = contestProtectedResourceMapper.deleteByContestId(cid);
        if (removed != expected) throw new StatusFailException("清理比赛资源关联失败！");
    }
}
