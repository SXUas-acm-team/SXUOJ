package top.hcode.hoj.interceptor;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.plugin.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import top.hcode.hoj.manager.file.ProtectedProblemResourceManager;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Capture assets at write time, including old URLs that disappear from edited statements. */
@Component
@Intercepts(@Signature(type = Executor.class, method = "update", args = {MappedStatement.class, Object.class}))
public class ConfidentialResourceWriteInterceptor implements Interceptor {
    private static final Set<String> WATCHED = new HashSet<>(Arrays.asList("ProblemMapper", "ContestMapper",
            "ContestProblemMapper", "ContestAccessPolicyMapper", "AnnouncementMapper", "ContestAnnouncementMapper",
            "DiscussionMapper", "CommentMapper", "ReplyMapper"));
    private static final Set<String> COUNTERS = new HashSet<>(Arrays.asList("view_num", "like_num", "comment_num"));
    private final ThreadLocal<Boolean> recording = new ThreadLocal<>();
    @Autowired @Lazy private ProtectedProblemResourceManager protectedProblemResourceManager;

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        String statement = ((MappedStatement) invocation.getArgs()[0]).getId();
        int separator = statement.lastIndexOf('.');
        String mapper = separator < 0 ? statement : statement.substring(0, separator);
        mapper = mapper.substring(mapper.lastIndexOf('.') + 1);
        if (!WATCHED.contains(mapper) || Boolean.TRUE.equals(recording.get())) return invocation.proceed();
        if (isCounterUpdate((MappedStatement) invocation.getArgs()[0], invocation.getArgs()[1])) return invocation.proceed();
        recording.set(true);
        try {
            protectedProblemResourceManager.rememberAllConfidentialResources();
            Object result = invocation.proceed();
            protectedProblemResourceManager.rememberAllConfidentialResources();
            return result;
        } finally {
            recording.remove();
        }
    }

    @Override public Object plugin(Object target) { return Plugin.wrap(target, this); }

    private boolean isCounterUpdate(MappedStatement statement, Object parameter) {
        if (statement.getSqlCommandType() != SqlCommandType.UPDATE) return false;
        String sql = statement.getBoundSql(parameter).getSql().replace("`", "").toLowerCase(java.util.Locale.ROOT)
                .replaceAll("\\s+", " ");
        int set = sql.indexOf(" set "), where = sql.lastIndexOf(" where ");
        if (set < 0) return false;
        String assignments = sql.substring(set + 5, where > set ? where : sql.length()) + ",";
        int depth = 0, start = 0, count = 0;
        for (int i = 0; i < assignments.length(); i++) {
            char character = assignments.charAt(i);
            if (character == '(') depth++;
            if (character == ')') depth--;
            if (character == ',' && depth == 0) {
                String assignment = assignments.substring(start, i).trim();
                int equals = assignment.indexOf('=');
                if (equals < 0 || !COUNTERS.contains(assignment.substring(0, equals).trim())) return false;
                start = i + 1; count++;
            }
        }
        return count > 0 && depth == 0;
    }
}
