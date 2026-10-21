package top.hcode.hoj.controller.oj;

import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.common.result.CommonResult;
import top.hcode.hoj.manager.oj.ContestIdentityManager;
import top.hcode.hoj.pojo.dto.ContestIdentityConfigDTO;
import top.hcode.hoj.pojo.dto.ContestIdentityVerifyDTO;
import java.util.Map;

@RestController
@RequestMapping("/api/contest-identity")
@RequiresAuthentication
public class ContestIdentityController {
    @Autowired private ContestIdentityManager manager;
    @GetMapping("/{cid}/config")
    public CommonResult<Map<String, Object>> config(@PathVariable Long cid) throws StatusFailException, StatusForbiddenException {
        return CommonResult.successResponse(manager.adminConfig(cid));
    }
    @PutMapping("/{cid}/config")
    public CommonResult<Void> save(@PathVariable Long cid, @RequestBody ContestIdentityConfigDTO input) throws StatusFailException, StatusForbiddenException {
        manager.saveConfig(cid, input); return CommonResult.successResponse();
    }
    @PostMapping("/verify")
    public CommonResult<Void> verify(@RequestBody ContestIdentityVerifyDTO input) throws StatusFailException, StatusForbiddenException {
        manager.verify(input); return CommonResult.successResponse();
    }
}
