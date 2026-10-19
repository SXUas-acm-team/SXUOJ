package top.hcode.hoj.remoteJudge.task;

import lombok.Getter;
import lombok.Setter;
import cn.hutool.http.HttpRequest;
import top.hcode.hoj.http.SecureHttp;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import top.hcode.hoj.remoteJudge.entity.RemoteJudgeDTO;
import top.hcode.hoj.remoteJudge.entity.RemoteJudgeRes;


/**
 * 远程评测抽象类
 */
public abstract class RemoteJudgeStrategy {

    @Setter
    @Getter
    private RemoteJudgeDTO remoteJudgeDTO;

    public abstract void submit();

    public abstract RemoteJudgeRes result();

    public abstract void login();

    public abstract String getLanguage(String language);

    protected HttpRequest getRequest(String url) { return SecureHttp.get(url); }

    protected HttpRequest postRequest(String url) { return SecureHttp.post(url); }

    protected void pauseBeforeRetry(int seconds) {
        try {
            java.util.concurrent.TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Remote submission interrupted", interrupted);
        }
    }

    protected static String queryValue(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (UnsupportedEncodingException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

}
