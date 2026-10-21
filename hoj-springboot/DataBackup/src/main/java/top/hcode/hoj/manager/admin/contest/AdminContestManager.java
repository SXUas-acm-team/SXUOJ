package top.hcode.hoj.manager.admin.contest;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.extern.slf4j.Slf4j;
import org.apache.shiro.SecurityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.common.exception.StatusSystemErrorException;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.dao.contest.ContestRegisterEntityService;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.dto.DeleteContestDTO;
import top.hcode.hoj.validator.ContestDeletionPasswordValidator;
import top.hcode.hoj.pojo.entity.contest.ContestRegister;
import top.hcode.hoj.pojo.vo.AdminContestVO;
import top.hcode.hoj.pojo.vo.ContestAwardConfigVO;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.utils.Constants;
import top.hcode.hoj.validator.ContestValidator;
import top.hcode.hoj.manager.oj.ContestAccessPolicyManager;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * @Author: Himit_ZH
 * @Date: 2022/3/9 11:20
 * @Description:
 */
@Component
@Slf4j(topic = "hoj")
public class AdminContestManager {

    @Autowired private top.hcode.hoj.manager.oj.ContestIdentityManager identityManager;

    @Autowired
    private ContestEntityService contestEntityService;

    @Autowired
    private ContestRegisterEntityService contestRegisterEntityService;

    @Autowired
    private ContestValidator contestValidator;

    @Autowired
    private ContestAccessPolicyManager contestAccessPolicyManager;

    @Autowired
    private ContestDeletionPasswordValidator contestDeletionPasswordValidator;

    public IPage<Contest> getContestList(Integer limit, Integer currentPage, String keyword) {

        currentPage = top.hcode.hoj.utils.RequestLimits.pageNumber(currentPage);
        limit = top.hcode.hoj.utils.RequestLimits.pageSize(limit, 10);
        IPage<Contest> iPage = new Page<>(currentPage, limit);
        QueryWrapper<Contest> queryWrapper = new QueryWrapper<>();
        // 过滤密码
        queryWrapper.select(Contest.class, info -> !info.getColumn().equals("pwd"));
        if (!StringUtils.isEmpty(keyword)) {
            keyword = keyword.trim();
            queryWrapper
                    .like("title", keyword).or()
                    .like("id", keyword);
        }
        queryWrapper.eq("is_group", false).orderByDesc("start_time");
        return contestEntityService.page(iPage, queryWrapper);
    }

    public AdminContestVO getContest(Long cid) throws StatusFailException, StatusForbiddenException {
        Contest contest = contestEntityService.getById(cid);
        if (contest == null) { // 查询不存在
            throw new StatusFailException("查询失败：该比赛不存在,请检查参数cid是否准确！");
        }
        // 获取当前登录的用户
        AccountProfile userRolesVo = (AccountProfile) SecurityUtils.getSubject().getPrincipal();

        // 是否为超级管理员
        boolean isRoot = top.hcode.hoj.utils.ContentAdmin.allowed();

        // 只有超级管理员和比赛拥有者才能操作
        if (!isRoot && !userRolesVo.getUid().equals(contest.getUid())) {
            throw new StatusForbiddenException("对不起，你无权限操作！");
        }
        AdminContestVO adminContestVo = BeanUtil.copyProperties(contest, AdminContestVO.class, "starAccount");
        if (StringUtils.isEmpty(contest.getStarAccount())) {
            adminContestVo.setStarAccount(new ArrayList<>());
        } else {
            try {
                JSONObject jsonObject = JSONUtil.parseObj(contest.getStarAccount());
                List<String> starAccount = jsonObject.get("star_account", List.class);
                adminContestVo.setStarAccount(starAccount);
            } catch (Exception e) {
                adminContestVo.setStarAccount(new ArrayList<>());
            }
        }

        if (contest.getAwardType() != null && contest.getAwardType() != 0) {
            try {
                JSONObject jsonObject = JSONUtil.parseObj(contest.getAwardConfig());
                List<ContestAwardConfigVO> awardConfigList = jsonObject.get("config", List.class);
                adminContestVo.setAwardConfigList(awardConfigList);
            } catch (Exception e) {
                adminContestVo.setAwardConfigList(new ArrayList<>());
            }
        } else {
            adminContestVo.setAwardConfigList(new ArrayList<>());
        }

        contestAccessPolicyManager.decorate(adminContestVo);
        return adminContestVo;
    }

    @Transactional(rollbackFor = Exception.class)
    public void deleteContest(DeleteContestDTO request) throws StatusFailException, StatusForbiddenException {
        AccountProfile userRolesVo = (AccountProfile) SecurityUtils.getSubject().getPrincipal();
        if (userRolesVo == null || !top.hcode.hoj.utils.ContentAdmin.allowed()) {
            throw new StatusForbiddenException("对不起，只有管理员可以删除全站比赛！");
        }
        if (request == null || request.getCid() == null) {
            throw new StatusFailException("请在JSON请求正文中提供比赛ID及当前登录账号密码！");
        }
        Long cid = request.getCid();
        Contest stored = contestEntityService.getByIdForUpdate(cid);
        if (stored == null) throw new StatusFailException("删除失败，该比赛不存在！");
        contestDeletionPasswordValidator.validate(userRolesVo, request.getPassword());
        contestAccessPolicyManager.deletePolicy(cid);
        identityManager.delete(cid);
        boolean isOk = contestEntityService.removeById(cid);
        /*
        contest的id为其他表的外键的表中的对应数据都会被一起删除！
         */
        if (!isOk) { // 删除成功
            throw new StatusFailException("删除失败");
        }
        log.info("[{}],[{}],cid:[{}],operatorUid:[{}],operatorUsername:[{}]",
                "Admin_Contest", "Delete", cid, userRolesVo.getUid(), userRolesVo.getUsername());
    }

    @Transactional(rollbackFor = Exception.class)
    public void addContest(AdminContestVO adminContestVo) throws StatusFailException {
        contestValidator.validateContest(adminContestVo);

        Contest contest = BeanUtil.copyProperties(adminContestVo, Contest.class, "starAccount");
        contestAccessPolicyManager.prepare(contest, adminContestVo, null);
        JSONObject accountJson = new JSONObject();
        if (adminContestVo.getStarAccount() == null) {
            accountJson.set("star_account", new ArrayList<>());
        } else {
            accountJson.set("star_account", adminContestVo.getStarAccount());
        }
        contest.setStarAccount(accountJson.toString());

        if (adminContestVo.getAwardType() != null && adminContestVo.getAwardType() != 0) {
            JSONObject awardConfigJson = new JSONObject();
            List<ContestAwardConfigVO> awardConfigList = adminContestVo.getAwardConfigList();
            awardConfigList.sort(Comparator.comparingInt(ContestAwardConfigVO::getPriority));
            awardConfigJson.set("config", awardConfigList);
            contest.setAwardConfig(awardConfigJson.toString());
        }

        boolean isOk = contestEntityService.save(contest);
        if (!isOk) { // 删除成功
            throw new StatusFailException("添加失败");
        }
        contestAccessPolicyManager.savePolicy(contest, adminContestVo, null);
    }

    @Transactional(rollbackFor = Exception.class)
    public void cloneContest(Long cid) throws StatusSystemErrorException {
        Contest contest = contestEntityService.getById(cid);
        if (contest == null) {
            throw new StatusSystemErrorException("该比赛不存在，无法克隆！");
        }
        // 获取当前登录的用户
        AccountProfile userRolesVo = (AccountProfile) SecurityUtils.getSubject().getPrincipal();

        if (!top.hcode.hoj.utils.ContentAdmin.allowed() && !Objects.equals(contest.getUid(), userRolesVo.getUid())) {
            throw new StatusSystemErrorException("无权克隆其他用户的比赛！");
        }
        boolean confidential = contestAccessPolicyManager.isConfidential(contest);
        // A confidential clone remains sealed and hidden until configured with a new password.
        contest.setPwd(null).setAuth(confidential ? Constants.Contest.AUTH_PRIVATE.getCode()
                : Constants.Contest.AUTH_PUBLIC.getCode()).setVisible(false);
        contest.setUid(userRolesVo.getUid())
                .setAuthor(userRolesVo.getUsername())
                .setSource(cid.intValue())
                .setId(null)
                .setGmtCreate(null)
                .setGmtModified(null);
        contest.setTitle(contest.getTitle() + " [Cloned]");
        if (!contestEntityService.save(contest)) throw new StatusSystemErrorException("克隆比赛失败！");
        if (confidential) {
            AdminContestVO policy = new AdminContestVO();
            policy.setConfidential(true);
            try { contestAccessPolicyManager.savePolicy(contest, policy, null); }
            catch (StatusFailException e) { throw new StatusSystemErrorException(e.getMessage()); }
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public void updateContest(AdminContestVO adminContestVo) throws StatusForbiddenException, StatusFailException {
        contestValidator.validateContest(adminContestVo);

        // 获取当前登录的用户
        AccountProfile userRolesVo = (AccountProfile) SecurityUtils.getSubject().getPrincipal();
        // 是否为超级管理员
        boolean isRoot = top.hcode.hoj.utils.ContentAdmin.allowed();
        // 只有超级管理员和比赛拥有者才能操作
        Contest storedContest = contestEntityService.getByIdForUpdate(adminContestVo.getId());
        if (storedContest == null) throw new StatusFailException("比赛不存在！");
        if (!isRoot && !userRolesVo.getUid().equals(storedContest.getUid())) {
            throw new StatusForbiddenException("对不起，你无权限操作！");
        }
        adminContestVo.setUid(storedContest.getUid());
        Contest contest = BeanUtil.copyProperties(adminContestVo, Contest.class, "starAccount");
        boolean previouslyConfidential = contestAccessPolicyManager.isConfidential(storedContest);
        contestAccessPolicyManager.prepare(contest, adminContestVo, storedContest);

        JSONObject accountJson = new JSONObject();
        accountJson.set("star_account", adminContestVo.getStarAccount());
        contest.setStarAccount(accountJson.toString());

        if (adminContestVo.getAwardType() != null && adminContestVo.getAwardType() != 0) {
            List<ContestAwardConfigVO> awardConfigList = adminContestVo.getAwardConfigList();
            awardConfigList.sort(Comparator.comparingInt(ContestAwardConfigVO::getPriority));
            JSONObject awardConfigJson = new JSONObject();
            awardConfigJson.set("config", awardConfigList);
            contest.setAwardConfig(awardConfigJson.toString());
        }


        Contest oldContest = storedContest;
        boolean isOk = contestEntityService.saveOrUpdate(contest);
        if (isOk) {
            if (!contest.getAuth().equals(Constants.Contest.AUTH_PUBLIC.getCode())) {
                if (!Objects.equals(oldContest.getPwd(), contest.getPwd())
                        || (Boolean.TRUE.equals(adminContestVo.getConfidential()) && !previouslyConfidential)
                        || (previouslyConfidential && (!Objects.equals(oldContest.getStartTime(), contest.getStartTime())
                        || !Objects.equals(oldContest.getEndTime(), contest.getEndTime())))) {
                    UpdateWrapper<ContestRegister> updateWrapper = new UpdateWrapper<>();
                    updateWrapper.eq("cid", contest.getId());
                    contestRegisterEntityService.remove(updateWrapper);
                }
            }
        } else {
            throw new StatusFailException("修改失败");
        }
        contestAccessPolicyManager.savePolicy(contest, adminContestVo, oldContest);
    }

    public void changeContestVisible(Long cid, String uid, Boolean visible) throws StatusFailException, StatusForbiddenException {
        // 获取当前登录的用户
        AccountProfile userRolesVo = (AccountProfile) SecurityUtils.getSubject().getPrincipal();
        // 是否为超级管理员
        boolean isRoot = top.hcode.hoj.utils.ContentAdmin.allowed();
        // 只有超级管理员和比赛拥有者才能操作
        Contest storedContest = contestEntityService.getById(cid);
        if (storedContest == null) throw new StatusFailException("比赛不存在！");
        if (!isRoot && !userRolesVo.getUid().equals(storedContest.getUid())) {
            throw new StatusForbiddenException("对不起，你无权限操作！");
        }

        boolean isOK = contestEntityService.updateById(new Contest().setId(cid).setVisible(visible));

        if (!isOK) {
            throw new StatusFailException("修改失败");
        }
        log.info("[{}],[{}],value:[{}],cid:[{}],operatorUid:[{}],operatorUsername:[{}]",
                "Admin_Contest", "Change_Visible", visible, cid, userRolesVo.getUid(), userRolesVo.getUsername());
    }

}
