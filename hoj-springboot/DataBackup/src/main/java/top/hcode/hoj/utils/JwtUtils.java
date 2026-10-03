package top.hcode.hoj.utils;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import top.hcode.hoj.shiro.ShiroConstant;
import top.hcode.hoj.security.SecretPolicy;
import cn.hutool.crypto.SecureUtil;

import java.util.Date;
import java.util.UUID;
import java.util.Objects;


@Slf4j(topic = "hoj")
@Data
@ToString(onlyExplicitlyIncluded = true)
@Component
@ConfigurationProperties(prefix = "hoj.jwt")
public class JwtUtils {

    private String secret;

    private long expire;

    private String header;

    private long checkRefreshExpire;

    @Autowired
    private RedisUtils redisUtils;

    public void setSecret(String value) {
        if (!SecretPolicy.isStrongJwt(value)) {
            throw new IllegalArgumentException("JWT signing key must contain at least 64 random bytes in Base64.");
        }
        this.secret = value;
    }

    /**
     * 生成jwt token
     */
    public String generateToken(String userId) {
        String userKey = ShiroConstant.SHIRO_TOKEN_KEY + userId;
        Object storedEpoch = redisUtils.get(userKey);
        if (!(storedEpoch instanceof String) || !SecretPolicy.isStrongJwt((String) storedEpoch)) {
            String freshEpoch = SecretPolicy.randomSecret();
            if (storedEpoch == null) {
                redisUtils.getLock(userKey, (int) Math.min(expire, Integer.MAX_VALUE), freshEpoch);
                storedEpoch = redisUtils.get(userKey);
            } else {
                if (!redisUtils.set(userKey, freshEpoch, expire)) throw new IllegalStateException("Cannot persist login session.");
                storedEpoch = freshEpoch;
            }
        }
        if (!(storedEpoch instanceof String)) throw new IllegalStateException("Cannot persist login session.");
        return issueToken(userId, (String) storedEpoch);
    }

    /** Refresh may extend an existing epoch, but cannot create or replace one after revocation. */
    public String refreshToken(String userId, String oldToken) {
        Claims claims = getClaimByToken(oldToken);
        if (claims == null || !hasValidSession(userId, oldToken)) {
            throw new IllegalStateException("Login session has been revoked.");
        }
        String expectedEpoch = claims.get("sessionEpoch", String.class);
        String refreshed = issueToken(userId, expectedEpoch);
        if (!Objects.equals(expectedEpoch, redisUtils.get(ShiroConstant.SHIRO_TOKEN_KEY + userId))) {
            throw new IllegalStateException("Login session has been revoked.");
        }
        return refreshed;
    }

    private String issueToken(String userId, String epoch) {
        Date nowDate = new Date();
        Date expireDate = new Date(nowDate.getTime() + expire * 1000);
        String userKey = ShiroConstant.SHIRO_TOKEN_KEY + userId;
        String sessionId = UUID.randomUUID().toString();

        String token = Jwts.builder()
                .setHeaderParam("type", "JWT")
                .setSubject(userId)
                .setId(sessionId)
                .claim("sessionEpoch", epoch)
                .setIssuedAt(nowDate)
                .setExpiration(expireDate)
                .signWith(SignatureAlgorithm.HS512, secret)
                .compact();
        if (!redisUtils.set(sessionKey(userId, sessionId), SecureUtil.sha256(token), expire)) {
            throw new IllegalStateException("Cannot persist login session.");
        }
        redisUtils.expire(userKey, expire);
        redisUtils.set(refreshKey(userId, sessionId), "1", checkRefreshExpire);
        return token;
    }

    public Claims getClaimByToken(String token) {
        try {
            return Jwts.parser()
                    .setSigningKey(secret)
                    .parseClaimsJws(token)
                    .getBody();
        } catch (Exception e) {
            log.debug("JWT validation failed");
            return null;
        }
    }

    public void cleanToken(String uid) {
        redisUtils.del(ShiroConstant.SHIRO_TOKEN_KEY + uid, ShiroConstant.SHIRO_TOKEN_REFRESH + uid);
    }

    public boolean hasToken(String uid) {
        return redisUtils.hasKey(ShiroConstant.SHIRO_TOKEN_KEY + uid);
    }

    public boolean hasValidSession(String uid, String token) {
        Claims claims = getClaimByToken(token);
        if (claims == null || !Objects.equals(uid, claims.getSubject()) || claims.getId() == null) return false;
        Object epoch = redisUtils.get(ShiroConstant.SHIRO_TOKEN_KEY + uid);
        return epoch != null && Objects.equals(epoch, claims.get("sessionEpoch"))
                && Objects.equals(redisUtils.get(sessionKey(uid, claims.getId())), SecureUtil.sha256(token));
    }

    private String sessionKey(String uid, String sessionId) {
        return ShiroConstant.SHIRO_TOKEN_KEY + "session:" + uid + ":" + sessionId;
    }

    public boolean needsRefresh(String uid, String token) {
        Claims claims = getClaimByToken(token);
        return claims == null || claims.getId() == null || !redisUtils.hasKey(refreshKey(uid, claims.getId()));
    }

    private String refreshKey(String uid, String sessionId) {
        return ShiroConstant.SHIRO_TOKEN_REFRESH + uid + ":" + sessionId;
    }

    /**
     * token是否过期
     *
     * @return true：过期
     */
    public boolean isTokenExpired(Date expiration) {
        return expiration == null || expiration.before(new Date());
    }


}
