package top.hcode.hoj.shiro;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.shiro.authc.*;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import top.hcode.hoj.dao.user.UserInfoEntityService;
import top.hcode.hoj.dao.user.UserRoleEntityService;
import top.hcode.hoj.mapper.RoleAuthMapper;
import top.hcode.hoj.pojo.entity.user.Auth;
import top.hcode.hoj.pojo.entity.user.Role;
import top.hcode.hoj.pojo.entity.user.UserInfo;
import top.hcode.hoj.utils.JwtUtils;
import top.hcode.hoj.pojo.vo.RoleAuthsVO;

import java.util.LinkedList;
import java.util.List;

/**
 * @Author: Himit_ZH
 * @Date: 2020/7/19 22:57
 * @Description:
 */
@Slf4j
@Component
public class AccountRealm extends AuthorizingRealm {

    public AccountRealm() {
        // Disabled roles/permissions must take effect on the next request. The request-local
        // memo below still avoids repeating these queries for every hasRole/hasPermission call.
        setAuthorizationCachingEnabled(false);
        setAuthenticationCachingEnabled(false);
    }

    @Autowired
    private JwtUtils jwtUtils;

    @Autowired
    private UserRoleEntityService userRoleEntityService;

    @Autowired
    private UserInfoEntityService userInfoEntityService;

    @Autowired
    private RoleAuthMapper roleAuthMapper;

    @Override
    public boolean supports(AuthenticationToken token) {
        return token instanceof JwtToken;
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principals) {
        AccountProfile user = (AccountProfile) principals.getPrimaryPrincipal();
        RequestAttributes request = RequestContextHolder.getRequestAttributes();
        String requestKey = AccountRealm.class.getName() + ".authorization." + user.getUid();
        if (request != null) {
            Object memo = request.getAttribute(requestKey, RequestAttributes.SCOPE_REQUEST);
            if (memo instanceof AuthorizationInfo) return (AuthorizationInfo) memo;
        }
        //角色权限列表
        List<String> permissionsNameList = new LinkedList<>();
        //用户角色列表
        List<String> roleNameList = new LinkedList<>();
        //获取该用户角色所有的权限
        List<Role> roles = userRoleEntityService.getRolesByUid(user.getUid());
        for (Role role : roles) {
            if (!Integer.valueOf(0).equals(role.getStatus())) continue;
            roleNameList.add(role.getRole());
            RoleAuthsVO granted = roleAuthMapper.getRoleAuths(role.getId());
            if (granted == null || granted.getAuths() == null) continue;
            for (Auth auth : granted.getAuths()) {
                if (!Integer.valueOf(0).equals(auth.getStatus()) || auth.getPermission() == null
                        || auth.getPermission().trim().isEmpty()) continue;
                permissionsNameList.add(auth.getPermission());
            }
        }
        SimpleAuthorizationInfo authorizationInfo = new SimpleAuthorizationInfo();

        authorizationInfo.addRoles(roleNameList);
        //添加权限
        authorizationInfo.addStringPermissions(permissionsNameList);
        if (request != null) request.setAttribute(requestKey, authorizationInfo, RequestAttributes.SCOPE_REQUEST);
        return authorizationInfo;
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) throws AuthenticationException {

        JwtToken jwt = (JwtToken) token;

        String userId = jwtUtils.getClaimByToken((String) jwt.getPrincipal()).getSubject();

        QueryWrapper<UserInfo> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("uuid", userId)
                .select("uuid", "username", "nickname", "realname", "title_name", "title_color", "avatar", "status");

        UserInfo userInfo = userInfoEntityService.getOne(queryWrapper, false);
        if (userInfo == null) {
            throw new UnknownAccountException("账户不存在！");
        }
        if (userInfo.getStatus() != 0) {
            throw new LockedAccountException("该账户已被封禁，请联系管理员进行处理！");
        }
        AccountProfile profile = new AccountProfile();
        BeanUtil.copyProperties(userInfo, profile);
        profile.setUid(userInfo.getUuid());
        return new SimpleAuthenticationInfo(profile, jwt.getCredentials(), getName());
    }
}
