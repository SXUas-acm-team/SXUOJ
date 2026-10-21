package top.hcode.hoj.manager.oj;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import org.apache.shiro.SecurityUtils;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.common.exception.StatusNotFoundException;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.dao.contest.ContestProblemEntityService;
import top.hcode.hoj.pojo.dto.ContestRankDTO;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.contest.ContestProblem;
import top.hcode.hoj.pojo.vo.ContestOutsideInfoVO;
import top.hcode.hoj.pojo.vo.ContestVO;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.utils.Constants;
import top.hcode.hoj.utils.RedisUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import top.hcode.hoj.validator.ContestValidator;
import top.hcode.hoj.validator.ProblemContentAccessValidator;

import javax.annotation.Resource;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * @Author: Himit_ZH
 * @Date: 2022/3/11 20:02
 * @Description:
 */
@Component
public class ContestScoreboardManager {

    @Autowired private ContestIdentityManager identityManager;

    @Resource
    private ContestEntityService contestEntityService;

    @Resource
    private ContestProblemEntityService contestProblemEntityService;

    @Resource
    private ContestValidator contestValidator;

    @Resource
    private ContestRankManager contestRankManager;

    @Resource
    private RedisUtils redisUtils;

    @Resource
    private ContestAccessPolicyManager contestAccessPolicyManager;

    @Resource
    private ProblemContentAccessValidator problemContentAccessValidator;

    private void validateOutsideAccess(Contest contest) throws StatusForbiddenException {
        AccountProfile viewer = (AccountProfile) SecurityUtils.getSubject().getPrincipal();
        boolean root = top.hcode.hoj.utils.ContentAdmin.allowed();
        boolean confidential = contestValidator.isConfidential(contest);
        if (confidential) {
            try { contestValidator.validateContestAuth(contest, viewer, root); }
            catch (StatusFailException e) { throw new StatusForbiddenException(e.getMessage()); }
            if (contestValidator.isReleased(contest) || contestValidator.isContestAdmin(contest, viewer, root)) return;
        }
        if (!Boolean.TRUE.equals(contest.getOpenRank())) {
            throw new StatusForbiddenException("本场比赛未开启外榜，禁止访问外榜！");
        }
        if (Objects.equals(contest.getStatus(), Constants.Contest.STATUS_SCHEDULED.getCode())) {
            throw new StatusForbiddenException("本场比赛正在筹备中，禁止访问外榜！");
        }
    }

    public ContestOutsideInfoVO getContestOutsideInfo(Long cid) throws StatusNotFoundException, StatusForbiddenException {

        ContestVO contestInfo = contestEntityService.getContestInfoById(cid);
        if (contestInfo != null) contestInfo.setIdentityVerificationEnabled(identityManager.enabled(cid));
        Contest definition = contestEntityService.getById(cid);

        if (contestInfo == null || definition == null || !Boolean.TRUE.equals(definition.getVisible())) {
            throw new StatusNotFoundException("访问错误：该比赛不存在！");
        }

        validateOutsideAccess(definition);

        contestInfo.setNow(new Date());
        ContestOutsideInfoVO contestOutsideInfoVO = new ContestOutsideInfoVO();
        contestOutsideInfoVO.setContest(contestInfo);

        QueryWrapper<ContestProblem> contestProblemQueryWrapper = new QueryWrapper<>();
        contestProblemQueryWrapper.eq("cid", cid).orderByAsc("display_id");
        List<ContestProblem> contestProblemList = contestProblemEntityService.list(contestProblemQueryWrapper)
                .stream().filter(p -> problemContentAccessValidator.canAccessProblem(p.getPid(), cid))
                .collect(java.util.stream.Collectors.toList());
        contestOutsideInfoVO.setProblemList(contestProblemList);

        return contestOutsideInfoVO;
    }


    public IPage getContestOutsideScoreboard(ContestRankDTO contestRankDto) throws StatusFailException, StatusForbiddenException {

        Long cid = contestRankDto.getCid();
        List<String> concernedList = top.hcode.hoj.utils.RequestLimits.boundedDistinct(contestRankDto.getConcernedList(), 100);
        Boolean removeStar = contestRankDto.getRemoveStar();
        Boolean forceRefresh = contestRankDto.getForceRefresh();

        if (cid == null) {
            throw new StatusFailException("错误：比赛id不能为空");
        }
        AccountProfile caller = (AccountProfile) SecurityUtils.getSubject().getPrincipal();
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        String callerKey = caller != null ? "user:" + caller.getUid()
                : "ip:" + (attributes == null ? "unknown" : attributes.getRequest().getRemoteAddr());
        if (!redisUtils.isWithinRateLimit("scoreboard:" + callerKey + ":" + cid, 2)) {
            throw new StatusForbiddenException("榜单请求过于频繁，请稍后重试！");
        }
        if (removeStar == null) {
            removeStar = false;
        }
        if (forceRefresh == null) {
            forceRefresh = false;
        }

        // 获取本场比赛的状态
        Contest contest = contestEntityService.getById(cid);

        if (contest == null || !Boolean.TRUE.equals(contest.getVisible())) {
            throw new StatusFailException("访问错误：该比赛不存在！");
        }

        validateOutsideAccess(contest);

        // 获取当前登录的用户
        AccountProfile userRolesVo = (AccountProfile) SecurityUtils.getSubject().getPrincipal();

        // 超级管理员或者该比赛的创建者，则为比赛管理者
        boolean isRoot = false;
        String currentUid = null;

        if (userRolesVo != null) {
            currentUid = userRolesVo.getUid();
            isRoot = top.hcode.hoj.utils.ContentAdmin.allowed();
            // 不是比赛创建者或者超管无权限开启强制实时榜单
            if (!isRoot && !contest.getUid().equals(currentUid)) {
                forceRefresh = false;
            }
        }


        Integer currentPage = contestRankDto.getCurrentPage();
        Integer limit = contestRankDto.getLimit();
        // 页数，每页题数若为空，设置默认值
        currentPage = top.hcode.hoj.utils.RequestLimits.pageNumber(currentPage);
        limit = top.hcode.hoj.utils.RequestLimits.pageSize(limit, 50);

        // 校验该比赛是否开启了封榜模式，超级管理员和比赛创建者可以直接看到实际榜单
        boolean isOpenSealRank = contestValidator.isSealRank(currentUid, contest, forceRefresh, isRoot);
        boolean isContainsAfterContestJudge = (contestValidator.isReleased(contest) || Objects.equals(contest.getAllowEndSubmit(), true))
                && Objects.equals(contestRankDto.getContainsEnd(), true);

        if (contest.getType().intValue() == Constants.Contest.TYPE_ACM.getCode()) {


            // 获取ACM比赛排行榜外榜
            return contestRankManager.getACMContestScoreboard(isOpenSealRank,
                    removeStar,
                    contest,
                    null,
                    concernedList,
                    contestValidator.validateExternalRanks(contestRankDto.getExternalCidList(), userRolesVo, isRoot),
                    currentPage,
                    limit,
                    contestRankDto.getKeyword(),
                    !forceRefresh,
                    15L, // 默认15s缓存
                    isContainsAfterContestJudge);

        } else {
            // 获取OI比赛排行榜外榜
            return contestRankManager.getOIContestScoreboard(isOpenSealRank,
                    removeStar,
                    contest,
                    null,
                    concernedList,
                    contestValidator.validateExternalRanks(contestRankDto.getExternalCidList(), userRolesVo, isRoot),
                    currentPage,
                    limit,
                    contestRankDto.getKeyword(),
                    !forceRefresh,
                    15L, // 默认15s缓存
                    isContainsAfterContestJudge);
        }
    }
}
