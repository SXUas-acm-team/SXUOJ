package top.hcode.hoj.remoteJudge;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import top.hcode.hoj.dao.JudgeCaseEntityService;
import top.hcode.hoj.dao.JudgeEntityService;
import top.hcode.hoj.judge.JudgeContext;
import top.hcode.hoj.pojo.entity.judge.Judge;
import top.hcode.hoj.remoteJudge.entity.RemoteJudgeDTO;
import top.hcode.hoj.remoteJudge.entity.RemoteJudgeRes;
import top.hcode.hoj.remoteJudge.task.RemoteJudgeStrategy;
import top.hcode.hoj.service.RemoteJudgeService;
import top.hcode.hoj.util.Constants;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j(topic = "hoj")
@Component
public class RemoteJudgeGetResult {
    @Autowired
    private JudgeEntityService judgeEntityService;

    @Autowired
    private JudgeContext judgeContext;

    @Autowired
    private RemoteJudgeService remoteJudgeService;

    @Resource
    private JudgeCaseEntityService judgeCaseEntityService;

    private static final ScheduledExecutorService DEFAULT_SCHEDULER =
            Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    private final ScheduledExecutorService scheduler;
    private final Map<String, PollTask> activeTasks = new ConcurrentHashMap<>();

    public RemoteJudgeGetResult() { this(DEFAULT_SCHEDULER); }

    RemoteJudgeGetResult(ScheduledExecutorService scheduler) { this.scheduler = scheduler; }

    public void process(RemoteJudgeStrategy strategy) {
        RemoteJudgeDTO dto = strategy.getRemoteJudgeDTO();
        String key = dto.getOj() + ":" + dto.getJudgeId();
        PollTask task = new PollTask(key, strategy, dto);
        if (activeTasks.putIfAbsent(key, task) != null) return;
        try {
            task.future = scheduler.scheduleWithFixedDelay(task, 2500, 2500, TimeUnit.MILLISECONDS);
            // Completion can precede handle assignment when a scheduler runs immediately.
            if (task.done.get()) task.future.cancel(false);
        } catch (RuntimeException e) {
            task.finish();
            throw e;
        }
    }

    private final class PollTask implements Runnable {
        private final String key;
        private final RemoteJudgeStrategy strategy;
        private final RemoteJudgeDTO dto;
        private final AtomicBoolean done = new AtomicBoolean();
        private volatile ScheduledFuture<?> future;
        private int attempts;
        // Remote parsers may advance a test-case cursor. Retry a failed database write with
        // the same result rather than dropping those cases by fetching the next response.
        private RemoteJudgeRes pendingResult;
        private boolean casesStored;
        private boolean terminalResultStored;

        private PollTask(String key, RemoteJudgeStrategy strategy, RemoteJudgeDTO dto) {
            this.key = key;
            this.strategy = strategy;
            this.dto = dto;
        }

        @Override
        public void run() {
            if (done.get()) return;
            if (attempts++ >= 60) {
                fail();
                return;
            }
            try {
                if (pendingResult == null) pendingResult = strategy.result();
                if (pendingResult == null || pendingResult.getStatus() == null
                        || pendingResult.getStatus() < -4 || pendingResult.getStatus() > 8) {
                    pendingResult = null;
                    throw new IllegalStateException("Remote OJ returned no valid judge status");
                }
                int status = pendingResult.getStatus();
                if (!casesStored && !CollectionUtils.isEmpty(pendingResult.getJudgeCaseList())) {
                    if (!judgeCaseEntityService.saveBatch(pendingResult.getJudgeCaseList())) {
                        throw new IllegalStateException("Failed to store remote judge cases");
                    }
                    casesStored = true;
                }
                Judge judge = new Judge().setSubmitId(dto.getJudgeId()).setStatus(status);
                boolean isPending = status == Constants.Judge.STATUS_PENDING.getStatus()
                        || status == Constants.Judge.STATUS_COMPILING.getStatus()
                        || status == Constants.Judge.STATUS_JUDGING.getStatus();
                if (!isPending) {
                    judge.setTime(pendingResult.getTime()).setMemory(pendingResult.getMemory());
                    if (status == Constants.Judge.STATUS_COMPILE_ERROR.getStatus()) {
                        judge.setErrorMessage(pendingResult.getErrorInfo());
                    } else if (status == Constants.Judge.STATUS_SYSTEM_ERROR.getStatus()) {
                        judge.setErrorMessage("There is something wrong with the " + dto.getOj() + ", please try again later");
                    }
                    boolean inContest = dto.getCid() != null && dto.getCid() != 0;
                    Integer score = inContest
                            ? (Objects.equals(status, Constants.Judge.STATUS_ACCEPTED.getStatus()) ? 100 : 0) : null;
                    judge.setScore(score);
                    if (!terminalResultStored) {
                        if (!judgeEntityService.updateById(judge)) {
                            throw new IllegalStateException("Failed to store remote judge result");
                        }
                        terminalResultStored = true;
                    }
                    judgeContext.updateOtherTable(dto.getJudgeId(), status, dto.getCid(), dto.getUid(),
                            dto.getPid(), dto.getGid(), score, inContest ? judge.getTime() : null);
                    log.info("[{}] Get Result Successfully! Status:[{}]", dto.getOj(), status);
                    finish();
                } else {
                    if (!judgeEntityService.updateById(judge)) {
                        throw new IllegalStateException("Failed to store remote judge status");
                    }
                    pendingResult = null;
                    casesStored = false;
                }
            } catch (Exception e) {
                // Any uncaught exception cancels subsequent scheduled executions. Keep
                // invalid responses and transient persistence failures inside the retry limit.
                if (attempts == 60) log.error("Failed to obtain or save remote judge result", e);
            }
        }

        private void fail() {
            if (terminalResultStored) {
                // The OJ verdict is already durable. A failed derived-table update must
                // not turn a valid accepted/wrong-answer verdict into a submission failure.
                log.error("Remote judge result is saved but derived-table synchronization exhausted retries, judgeId={}",
                        dto.getJudgeId());
                finish();
                return;
            }
            try {
                UpdateWrapper<Judge> update = new UpdateWrapper<>();
                update.set("status", Constants.Judge.STATUS_SUBMITTED_FAILED.getStatus())
                        .set("error_message", "Waiting for remote judge result exceeds the maximum number of times, please try submitting again!")
                        .eq("submit_id", dto.getJudgeId());
                judgeEntityService.update(update);
            } catch (Exception e) {
                log.error("Failed to store remote judge timeout", e);
            }
            try {
                judgeContext.updateOtherTable(dto.getJudgeId(), Constants.Judge.STATUS_SYSTEM_ERROR.getStatus(),
                        dto.getCid(), dto.getUid(), dto.getPid(), dto.getGid(), null, null);
            } catch (Exception e) {
                log.error("Failed to synchronize remote judge timeout", e);
            } finally {
                finish();
            }
        }

        private void finish() {
            if (!done.compareAndSet(false, true)) return;
            try {
                changeRemoteJudgeLock(dto.getOj(), dto.getUsername(), dto.getServerIp(), dto.getServerPort(),
                        dto.getSubmitId(), dto.getAccountVersion());
            } catch (Exception e) {
                log.error("Failed to release remote judge account", e);
            } finally {
                ScheduledFuture<?> handle = future;
                if (handle != null) handle.cancel(false);
                // An old task must never remove a newly scheduled task with the same key.
                activeTasks.remove(key, this);
            }
        }
    }

    private void changeRemoteJudgeLock(String remoteJudge, String username, String ip, Integer port,
                                       Long resultSubmitId, Long version) {
        log.info("After Get Result,remote_judge:[{}],submit_id: [{}]! Begin to return the account to other task!",
                remoteJudge, resultSubmitId);
        remoteJudgeService.changeAccountStatus(remoteJudge, username, version);
        if (RemoteJudgeContext.openCodeforcesFixServer
                && (Constants.RemoteJudge.GYM_JUDGE.getName().equals(remoteJudge)
                || Constants.RemoteJudge.CF_JUDGE.getName().equals(remoteJudge))) {
            remoteJudgeService.changeServerSubmitCFStatus(ip, port);
        }
    }
}
