package top.hcode.hoj.manager.oj;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.mapper.ContestIdentityBindingMapper;
import top.hcode.hoj.mapper.ContestIdentityConfigMapper;
import top.hcode.hoj.pojo.entity.contest.*;
import java.util.Date;
import java.util.Objects;

@Component
public class ContestIdentityBindingWriter {
    @Autowired private ContestEntityService contests;
    @Autowired private ContestIdentityConfigMapper configs;
    @Autowired private ContestIdentityBindingMapper bindings;

    @Transactional(rollbackFor = Exception.class)
    public void bind(ContestIdentityConfig expected, String uid, String number, String name) throws StatusFailException {
        if (contests.getByIdForUpdate(expected.getCid()) == null) throw new StatusFailException("比赛不存在。");
        ContestIdentityConfig current = configs.selectById(expected.getCid());
        if (current == null || !Boolean.TRUE.equals(current.getEnabled())
                || !Objects.equals(expected.getApiUrl(), current.getApiUrl())
                || !Objects.equals(expected.getApiKey(), current.getApiKey())) {
            throw new StatusFailException("比赛核验设置已改变，请刷新后重试。");
        }
        ContestIdentityBinding old = bindings.selectOne(new QueryWrapper<ContestIdentityBinding>()
                .eq("cid", expected.getCid()).eq("uid", uid));
        if (old != null) {
            if (number.equals(old.getStudentNumber()) && name.equals(old.getVerifiedName())) return;
            throw new StatusFailException("本场比赛已绑定身份，不能更换学号或姓名。");
        }
        ContestIdentityBinding binding = new ContestIdentityBinding();
        binding.setCid(expected.getCid()); binding.setUid(uid); binding.setStudentNumber(number);
        binding.setVerifiedName(name); binding.setVerifiedAt(new Date());
        try {
            if (bindings.insert(binding) != 1) throw new StatusFailException("保存核验结果失败，请稍后重试。");
        } catch (DuplicateKeyException e) { throw new StatusFailException("本场账号或该学号已绑定身份，不能重复绑定。"); }
    }
}
