package top.hcode.hoj.dao.contest.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import top.hcode.hoj.dao.contest.ContestRecordEntityService;
import top.hcode.hoj.dao.user.UserInfoEntityService;
import top.hcode.hoj.mapper.ContestRecordMapper;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.contest.ContestRecord;
import top.hcode.hoj.pojo.vo.ContestRecordVO;
import top.hcode.hoj.utils.Constants;
import top.hcode.hoj.utils.RedisUtils;

import java.util.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author Himit_ZH
 * @since 2020-10-23
 */
@Service
public class ContestRecordEntityServiceImpl extends ServiceImpl<ContestRecordMapper, ContestRecord> implements ContestRecordEntityService {

    @Autowired
    private ContestRecordMapper contestRecordMapper;

    @Autowired
    private UserInfoEntityService userInfoEntityService;

    @Autowired
    private RedisUtils redisUtils;

    @Override
    public IPage<ContestRecord> getACInfo(Integer currentPage,
                                          Integer limit,
                                          Integer status,
                                          Long cid,
                                          String contestCreatorId,
                                          Date startTime,
                                          Date endTime) {

        List<String> excluded = new ArrayList<>(userInfoEntityService.getSuperAdminUidList());
        excluded.add(contestCreatorId);
        Page<ContestRecord> page = new Page<>(top.hcode.hoj.utils.RequestLimits.pageNumber(currentPage),
                top.hcode.hoj.utils.RequestLimits.pageSize(limit, 30));
        return contestRecordMapper.getACInfoPage(page, status, cid, excluded, startTime, endTime);
    }

    @Override
    public List<ContestRecordVO> getOIContestRecord(Contest contest, List<Integer> externalCidList,
                                                    Boolean isOpenSealRank, Boolean isContainsAfterContestJudge) {

        String oiRankScoreType = contest.getOiRankScoreType();
        Long cid = contest.getId();
        String contestCreatorUid = contest.getUid();

        if (!isOpenSealRank) {
            // 封榜解除 获取最新数据
            // 获取每个用户每道题最近一次提交
            Long endTime = contest.getDuration();
            if (Objects.equals(Constants.Contest.OI_RANK_RECENT_SCORE.getName(), oiRankScoreType)) {
                return contestRecordMapper.getOIContestRecordByRecentSubmission(cid,
                        externalCidList,
                        contestCreatorUid,
                        false,
                        null,
                        endTime,
                        isContainsAfterContestJudge);
            } else {
                return contestRecordMapper.getOIContestRecordByHighestSubmission(cid,
                        externalCidList,
                        contestCreatorUid,
                        false,
                        null,
                        endTime,
                        isContainsAfterContestJudge);
            }

        } else {
            String key = null;
            if (isContainsAfterContestJudge) {
                key = Constants.Contest.OI_CONTEST_RANK_CACHE.getName() + "_contains_after_" + oiRankScoreType + "_" + cid;
            } else {
                key = Constants.Contest.OI_CONTEST_RANK_CACHE.getName() + "_" + oiRankScoreType + "_" + cid;
            }
            List<ContestRecordVO> oiContestRecordList = (List<ContestRecordVO>) redisUtils.get(key);
            if (oiContestRecordList == null) {
                Long sealTime = DateUtil.between(contest.getStartTime(), contest.getSealRankTime(), DateUnit.SECOND);
                if (sealTime > 0) {
                    sealTime--;
                }
                if (Objects.equals(Constants.Contest.OI_RANK_RECENT_SCORE.getName(), oiRankScoreType)) {
                    oiContestRecordList = contestRecordMapper.getOIContestRecordByRecentSubmission(cid,
                            externalCidList,
                            contestCreatorUid,
                            true,
                            sealTime,
                            null,
                            isContainsAfterContestJudge);
                } else {
                    oiContestRecordList = contestRecordMapper.getOIContestRecordByHighestSubmission(cid,
                            externalCidList,
                            contestCreatorUid,
                            true,
                            sealTime,
                            null,
                            isContainsAfterContestJudge);
                }
                redisUtils.set(key, oiContestRecordList, 2 * 3600);
            }
            return oiContestRecordList;
        }

    }

    @Override
    public List<ContestRecordVO> getACMContestRecord(String contestCreatorUid, Long cid, List<Integer> externalCidList, Date startTime) {
        if (CollectionUtil.isEmpty(externalCidList)) {
            return contestRecordMapper.getACMContestRecord(contestCreatorUid, cid, null, null);
        } else {
            long time = DateUtil.between(startTime, new Date(), DateUnit.SECOND);
            return contestRecordMapper.getACMContestRecord(contestCreatorUid, cid, externalCidList, time);
        }
    }

}
