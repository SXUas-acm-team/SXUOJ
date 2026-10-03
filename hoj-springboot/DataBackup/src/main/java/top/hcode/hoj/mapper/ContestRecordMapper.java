package top.hcode.hoj.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;
import top.hcode.hoj.pojo.entity.contest.ContestRecord;
import top.hcode.hoj.pojo.vo.ContestRecordVO;

import java.util.List;
import java.util.Date;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

/**
 * <p>
 * Mapper 接口
 * </p>
 *
 * @author Himit_ZH
 * @since 2020-10-23
 */
@Mapper
@Repository
public interface ContestRecordMapper extends BaseMapper<ContestRecord> {
    List<ContestRecord> getACInfo(@Param("status") Integer status, @Param("cid") Long cid);
    IPage<ContestRecord> getACInfoPage(Page<ContestRecord> page, @Param("status") Integer status,
            @Param("cid") Long cid, @Param("excludedUids") List<String> excludedUids,
            @Param("startTime") Date startTime, @Param("endTime") Date endTime);

    List<ContestRecordVO> getOIContestRecordByRecentSubmission(@Param("cid") Long cid,
                                                               @Param("externalCidList") List<Integer> externalCidList,
                                                               @Param("contestCreatorUid") String contestCreatorUid,
                                                               @Param("isOpenSealRank") Boolean isOpenSealRank,
                                                               @Param("sealTime") Long sealTime,
                                                               @Param("endTime") Long endTime,
                                                               @Param("isContainsAfterContestJudge") Boolean isContainsAfterContestJudge);

    List<ContestRecordVO> getOIContestRecordByHighestSubmission(@Param("cid") Long cid,
                                                                @Param("externalCidList") List<Integer> externalCidList,
                                                                @Param("contestCreatorUid") String contestCreatorUid,
                                                                @Param("isOpenSealRank") Boolean isOpenSealRank,
                                                                @Param("sealTime") Long sealTime,
                                                                @Param("endTime") Long endTime,
                                                                @Param("isContainsAfterContestJudge") Boolean isContainsAfterContestJudge);

    List<ContestRecordVO> getACMContestRecord(@Param("contestCreatorUid") String contestCreatorUid,
                                              @Param("cid") Long cid,
                                              @Param("externalCidList") List<Integer> externalCidList,
                                              @Param("time") Long time);
}
