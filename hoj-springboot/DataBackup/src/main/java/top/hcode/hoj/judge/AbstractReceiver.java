package top.hcode.hoj.judge;

import cn.hutool.json.JSONObject;

/**
 * @Author: Himit_ZH
 * @Date: 2021/12/22 12:40
 * @Description:
 */

public abstract class AbstractReceiver {

    protected Long getJudgeId(JSONObject task) {
        Long judgeId = task.getLong("judgeId");
        if (judgeId != null) return judgeId;
        JSONObject legacyJudge = task.getJSONObject("judge");
        return legacyJudge == null ? null : legacyJudge.getLong("submitId");
    }

    public void handleWaitingTask(String... queues) {
        for (String queue : queues) {
            String taskStr = getTaskByRedis(queue);
            if (taskStr != null) {
                handleJudgeMsg(taskStr, queue);
            }
        }
    }

    public abstract String getTaskByRedis(String queue);

    public abstract void handleJudgeMsg(String taskStr, String queueName);

}
