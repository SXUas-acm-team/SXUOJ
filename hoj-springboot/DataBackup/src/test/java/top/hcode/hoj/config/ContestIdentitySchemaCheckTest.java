package top.hcode.hoj.config;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.Arrays;
import java.util.Collections;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ContestIdentitySchemaCheckTest {
    @Test void completeMigrationCanStart() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForList(anyString(), eq(String.class))).thenReturn(Arrays.asList("id", "cid,uid", "cid,student_number"));
        ContestIdentitySchemaCheck check = new ContestIdentitySchemaCheck(); ReflectionTestUtils.setField(check, "jdbcTemplate", jdbc);
        assertDoesNotThrow(() -> check.run(null));
        verify(jdbc, never()).execute(anyString());
    }
    @Test void missingUniquenessNeverStartsAnUnsafeBindingService() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForList(anyString(), eq(String.class))).thenReturn(Collections.singletonList("cid,uid"));
        ContestIdentitySchemaCheck check = new ContestIdentitySchemaCheck(); ReflectionTestUtils.setField(check, "jdbcTemplate", jdbc);
        assertTrue(assertThrows(IllegalStateException.class, () -> check.run(null)).getMessage().contains("contest-identity-v1.1.0.sql"));
    }
    @Test void missingTablesGiveAnActionableMigrationMessage() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForList(anyString())).thenThrow(new org.springframework.jdbc.BadSqlGrammarException("schema", "SELECT", new java.sql.SQLException()));
        ContestIdentitySchemaCheck check = new ContestIdentitySchemaCheck(); ReflectionTestUtils.setField(check, "jdbcTemplate", jdbc);
        assertTrue(assertThrows(IllegalStateException.class, () -> check.run(null)).getMessage().contains("四张新表"));
    }
}
