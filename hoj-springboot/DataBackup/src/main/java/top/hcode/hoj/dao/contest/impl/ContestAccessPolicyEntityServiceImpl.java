package top.hcode.hoj.dao.contest.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;
import top.hcode.hoj.dao.contest.ContestAccessPolicyEntityService;
import top.hcode.hoj.mapper.ContestAccessPolicyMapper;
import top.hcode.hoj.pojo.entity.contest.ContestAccessPolicy;

@Service
public class ContestAccessPolicyEntityServiceImpl
        extends ServiceImpl<ContestAccessPolicyMapper, ContestAccessPolicy>
        implements ContestAccessPolicyEntityService {
}
