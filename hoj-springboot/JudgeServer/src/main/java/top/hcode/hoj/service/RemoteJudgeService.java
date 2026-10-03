package top.hcode.hoj.service;

public interface RemoteJudgeService {

    public void changeAccountStatus(String remoteJudge, String username, Long version);

    public void changeServerSubmitCFStatus(String ip, Integer port);
}
