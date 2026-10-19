package top.hcode.hoj.dao.user.impl;

import cn.hutool.core.date.DateUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import top.hcode.hoj.mapper.SessionMapper;
import top.hcode.hoj.pojo.entity.msg.AdminSysNotice;
import top.hcode.hoj.pojo.entity.user.Session;
import top.hcode.hoj.pojo.entity.msg.UserSysNotice;
import top.hcode.hoj.dao.msg.AdminSysNoticeEntityService;
import top.hcode.hoj.dao.msg.UserSysNoticeEntityService;
import top.hcode.hoj.dao.user.SessionEntityService;

import javax.annotation.Resource;
import java.util.Date;
import java.util.List;

/**
 * @Author: Himit_ZH
 * @Date: 2020/12/3 22:46
 * @Description:
 */
@Service
public class SessionEntityServiceImpl extends ServiceImpl<SessionMapper, Session> implements SessionEntityService {

    @Resource
    private SessionMapper sessionMapper;

    @Resource
    private AdminSysNoticeEntityService adminSysNoticeEntityService;

    @Resource
    private UserSysNoticeEntityService userSysNoticeEntityService;

    @Override
    @Async
    public void checkRemoteLogin(String uid) {
        QueryWrapper<Session> sessionQueryWrapper = new QueryWrapper<>();
        sessionQueryWrapper.eq("uid", uid)
                .orderByDesc("gmt_create")
                .last("limit 2");
        List<Session> sessionList = sessionMapper.selectList(sessionQueryWrapper);
        if (sessionList.size() < 2) {
            return;
        }
        Session nowSession = sessionList.get(0);
        Session lastSession = sessionList.get(1);
        // 如果两次登录的ip不相同，需要发通知给用户
        if (!java.util.Objects.equals(nowSession.getIp(), lastSession.getIp())) {
            String remoteLoginContent = getRemoteLoginContent(lastSession.getIp(), nowSession.getIp(), nowSession.getGmtCreate());
            AdminSysNotice adminSysNotice = new AdminSysNotice();
            adminSysNotice
                    .setType("Single")
                    .setContent(remoteLoginContent)
                    .setTitle("账号异地登录通知(Account Remote Login Notice)")
                    .setAdminId("1")
                    .setState(false)
                    .setRecipientId(uid);
            boolean isSaveOk = adminSysNoticeEntityService.save(adminSysNotice);
            if (isSaveOk) {
                UserSysNotice userSysNotice = new UserSysNotice();
                userSysNotice.setType("Sys")
                        .setSysNoticeId(adminSysNotice.getId())
                        .setRecipientId(uid)
                        .setState(false);
                boolean isOk = userSysNoticeEntityService.save(userSysNotice);
                if (isOk) {
                    adminSysNotice.setState(true);
                    adminSysNoticeEntityService.saveOrUpdate(adminSysNotice);
                }
            }
        }
    }

    protected String getRemoteLoginContent(String oldIp, String newIp, Date loginDate) {
        String dateStr = DateUtil.format(loginDate, "yyyy-MM-dd HH:mm:ss");
        StringBuilder sb = new StringBuilder();
        sb.append("亲爱的用户，您好！您的账号于").append(dateStr);
        String addr = null;
        try {
            String newRes = lookupAddress(newIp);
            JSONObject newResJson = JSONUtil.parseObj(newRes);
            addr = newResJson.getStr("addr");

        } catch (Exception ignored) {
            // Address enrichment must never suppress a different-IP security notice.
        }
        if (!StringUtils.isEmpty(addr)) {
            sb.append("在【")
                    .append(addr)
                    .append("】");
        }
        sb.append("登录，登录IP为：【")
                .append(newIp)
                .append("】，若非本人操作，请立即修改密码。")
                .append("\n\n")
                .append("Hello! Dear user, Your account was logged in in");

        if (!StringUtils.isEmpty(addr)) {
            sb.append(" 【")
                    .append(addr)
                    .append("】 on ")
                    .append(dateStr)
                    .append(". If you do not operate by yourself, please change your password immediately.");
        }

        return sb.toString();
    }

    protected String lookupAddress(String ip) throws java.io.UnsupportedEncodingException {
        return HttpUtil.get("https://whois.pconline.com.cn/ipJson.jsp?ip="
                + java.net.URLEncoder.encode(ip == null ? "" : ip, "UTF-8"), 3000);
    }
}
