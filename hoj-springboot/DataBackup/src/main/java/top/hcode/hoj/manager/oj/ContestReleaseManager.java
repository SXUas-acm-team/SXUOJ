package top.hcode.hoj.manager.oj;

import lombok.extern.slf4j.Slf4j;
import org.apache.shiro.SecurityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.dao.group.GroupEntityService;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.group.Group;
import top.hcode.hoj.pojo.vo.AdminContestVO;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.validator.GroupValidator;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/** Changes only publication state; never saves other unsaved contest form fields. */
@Slf4j
@Component
public class ContestReleaseManager {
    @Autowired private ContestEntityService contestEntityService;
    @Autowired private ContestAccessPolicyManager policyManager;
    @Autowired private GroupEntityService groupEntityService;
    @Autowired private GroupValidator groupValidator;

    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> changeState(Long cid, Boolean released, boolean groupRoute)
            throws StatusFailException, StatusForbiddenException {
        if (cid == null || cid <= 0 || released == null) throw new StatusFailException("比赛ID及解封状态不能为空！");
        Contest contest = contestEntityService.getByIdForUpdate(cid);
        if (contest == null) throw new StatusFailException("比赛不存在！");
        AccountProfile operator = (AccountProfile) SecurityUtils.getSubject().getPrincipal();
        if (operator == null) throw new StatusForbiddenException("请先登录！");
        boolean root = SecurityUtils.getSubject().hasRole("root");
        boolean owner = operator.getUid().equals(contest.getUid());
        if (groupRoute) {
            if (contest.getGid() == null) throw new StatusForbiddenException("不可操作非团队内的比赛！");
            Group group = groupEntityService.getById(contest.getGid());
            if (group == null || (!root && Integer.valueOf(1).equals(group.getStatus()))) {
                throw new StatusForbiddenException("比赛所属团队不存在或已被封禁！");
            }
            if (!root && !owner && !groupValidator.isGroupRoot(operator.getUid(), contest.getGid())) {
                throw new StatusForbiddenException("对不起，您无权限操作！");
            }
        } else if (!root && !owner) {
            throw new StatusForbiddenException("对不起，您无权限操作！");
        }
        if (!policyManager.isConfidential(contest)) throw new StatusFailException("请先保存保密赛设置，再操作解封状态！");
        Date now = new Date();
        if (released && (contest.getEndTime() == null || now.before(contest.getEndTime()))) {
            throw new StatusFailException("比赛结束后才能手动解封！");
        }
        Date releaseAt = released ? now : null;
        AdminContestVO input = new AdminContestVO();
        input.setConfidential(true);
        input.setReleaseAt(releaseAt);
        policyManager.savePolicy(contest, input, contest);
        log.info("Contest publication changed: cid={}, released={}, operatorUid={}", cid, released, operator.getUid());
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("cid", cid);
        result.put("releaseAt", releaseAt);
        return result;
    }
}
