package top.hcode.hoj.config;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import javax.annotation.Resource;
import java.util.List;

/** Refuse to start without the reviewed migration and its race-safe uniqueness constraints. */
@Component
public class ContestIdentitySchemaCheck implements ApplicationRunner {
    @Resource private JdbcTemplate jdbcTemplate;
    @Override public void run(ApplicationArguments args) {
        try {
            jdbcTemplate.queryForList("SELECT cid, enabled, replace_realname, star_prefixes, api_url, api_key FROM contest_identity_config WHERE 1=0");
            jdbcTemplate.queryForList("SELECT id, cid, uid, student_number, verified_name, verified_at FROM contest_identity_binding WHERE 1=0");
            List<String> uniqueColumns = jdbcTemplate.queryForList("SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')"
                    + " FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='contest_identity_binding'"
                    + " AND NON_UNIQUE=0 GROUP BY INDEX_NAME", String.class);
            if (!uniqueColumns.contains("cid,uid") || !uniqueColumns.contains("cid,student_number")) {
                throw new IllegalStateException("Missing identity binding uniqueness constraints");
            }
        } catch (RuntimeException failure) {
            throw new IllegalStateException("请先在 HOJ 数据库执行 deploy/contest-identity-v1.1.0.sql，核对两张新表及比赛用户、比赛学号唯一索引。", failure);
        }
    }
}
