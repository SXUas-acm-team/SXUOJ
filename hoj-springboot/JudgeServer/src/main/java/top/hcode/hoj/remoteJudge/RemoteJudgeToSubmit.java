package top.hcode.hoj.remoteJudge;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;
import top.hcode.hoj.dao.JudgeEntityService;
import top.hcode.hoj.judge.JudgeContext;
import top.hcode.hoj.pojo.entity.judge.Judge;
import top.hcode.hoj.remoteJudge.entity.RemoteJudgeDTO;
import top.hcode.hoj.remoteJudge.task.RemoteJudgeStrategy;
import top.hcode.hoj.service.RemoteJudgeService;
import top.hcode.hoj.util.Constants;

@Component
@Slf4j(topic = "hoj")
@RefreshScope
public class RemoteJudgeToSubmit {

    @Autowired
    private JudgeEntityService judgeEntityService;

    @Autowired
    private RemoteJudgeService remoteJudgeService;

    @Autowired
    private JudgeContext judgeContext;

    public boolean process(RemoteJudgeStrategy remoteJudgeStrategy) {

        RemoteJudgeDTO remoteJudgeDTO = remoteJudgeStrategy.getRemoteJudgeDTO();
        log.info("Ready remote judge task, OJ={}, judgeId={}", remoteJudgeDTO.getOj(), remoteJudgeDTO.getJudgeId());

        String errLog = null;
        // A new submission must obtain a new remote ID. A failed re-submit must
        // not reuse the ID retained on the local record from an earlier attempt.
        remoteJudgeDTO.setSubmitId(null);
        try {
            remoteJudgeStrategy.submit();
        } catch (Exception e) {
            log.error("Submit Failed! Error:", e);
            errLog = e.getMessage();
        }

        Long submitId = remoteJudgeDTO.getSubmitId();
        // 提交失败 前端手动按按钮再次提交 修改状态 STATUS_SUBMITTED_FAILED
        if (submitId == null || submitId <= 0L) {
            errLog = "[" + remoteJudgeDTO.getOj() + "] Submitted Failed! Failed to obtain the ID submitted by the platform!";
            submissionFailed(remoteJudgeDTO, errLog);
            return false;
        }

        // 提交成功顺便更新状态为-->STATUS_PENDING 等待判题中...
        try {
            if (!judgeEntityService.updateById(new Judge()
                    .setSubmitId(remoteJudgeDTO.getJudgeId())
                    .setStatus(Constants.Judge.STATUS_PENDING.getStatus())
                    .setVjudgeSubmitId(submitId)
                    .setVjudgeUsername(remoteJudgeDTO.getUsername())
                    .setVjudgePassword(remoteJudgeDTO.getPassword()))) {
                throw new IllegalStateException("Failed to store remote submission ID");
            }
        } catch (Exception e) {
            log.error("Failed to store remote submission ID", e);
            submissionFailed(remoteJudgeDTO, "Failed to store the remote submission ID, please try again later");
            return false;
        }

        log.info("[{}] Submit Successfully! The submit_id of remote judge is [{}]. Waiting the result of the task!",
                submitId, remoteJudgeDTO.getOj());
        return true;
    }

    private void submissionFailed(RemoteJudgeDTO dto, String error) {
        try {
            UpdateWrapper<Judge> update = new UpdateWrapper<>();
            update.set("status", Constants.Judge.STATUS_SUBMITTED_FAILED.getStatus())
                    .set("error_message", error).eq("submit_id", dto.getJudgeId());
            judgeEntityService.update(update);
        } catch (Exception e) {
            log.error("Failed to store remote submission failure", e);
        }
        try {
            judgeContext.updateOtherTable(dto.getJudgeId(), Constants.Judge.STATUS_SYSTEM_ERROR.getStatus(),
                    dto.getCid(), dto.getUid(), dto.getPid(), dto.getGid(), null, null);
        } catch (Exception e) {
            log.error("Failed to synchronize remote submission failure", e);
        } finally {
            try {
                remoteJudgeService.changeAccountStatus(dto.getOj(), dto.getUsername(), dto.getAccountVersion());
                if (RemoteJudgeContext.openCodeforcesFixServer
                        && (Constants.RemoteJudge.GYM_JUDGE.getName().equals(dto.getOj())
                        || Constants.RemoteJudge.CF_JUDGE.getName().equals(dto.getOj()))) {
                    remoteJudgeService.changeServerSubmitCFStatus(dto.getServerIp(), dto.getServerPort());
                }
            } catch (Exception e) {
                log.error("Failed to release remote submission account", e);
            }
        }
    }
}
