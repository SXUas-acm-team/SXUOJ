package top.hcode.hoj.config;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import top.hcode.hoj.manager.file.ProtectedProblemResourceManager;

import javax.annotation.Resource;

/** Read-only startup gate: deploy the reviewed schema migration before this backend. */
@Component
public class ContestAccessPolicySchemaCheck implements ApplicationRunner {
    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private ProtectedProblemResourceManager protectedProblemResourceManager;

    @Override
    public void run(ApplicationArguments args) {
        jdbcTemplate.queryForList("SELECT cid, release_at FROM contest_access_policy WHERE 1 = 0");
        jdbcTemplate.queryForList("SELECT cid, resource_hash, resource_path FROM contest_protected_resource WHERE 1 = 0");
        protectedProblemResourceManager.rememberAllConfidentialResources();
    }
}
