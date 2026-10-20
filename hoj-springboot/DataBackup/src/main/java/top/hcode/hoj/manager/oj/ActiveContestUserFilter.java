package top.hcode.hoj.manager.oj;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.stereotype.Component;
import top.hcode.hoj.dao.user.UserInfoEntityService;
import top.hcode.hoj.pojo.entity.user.UserInfo;

import javax.annotation.Resource;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Rank caches contain scores, never the authority to display a disabled account. */
@Component
public class ActiveContestUserFilter {
    private static final int QUERY_BATCH = 500;

    @Resource
    private UserInfoEntityService userInfoEntityService;

    public Set<String> getActiveUids(Collection<String> requested) {
        List<String> ids = requested.stream().filter(Objects::nonNull).filter(s -> !s.isEmpty())
                .distinct().collect(Collectors.toList());
        Set<String> active = new HashSet<>();
        for (int start = 0; start < ids.size(); start += QUERY_BATCH) {
            List<String> batch = ids.subList(start, Math.min(start + QUERY_BATCH, ids.size()));
            // SQL equality also rejects NULL status, which the legacy primitive Java field cannot represent.
            for (UserInfo user : userInfoEntityService.list(new QueryWrapper<UserInfo>()
                    .select("uuid", "status").in("uuid", batch).eq("status", 0))) {
                if (user != null && user.getStatus() == 0 && batch.contains(user.getUuid())) active.add(user.getUuid());
            }
        }
        return active;
    }

    public <T> List<T> filterActive(List<T> rows, Function<T, String> uid) {
        if (rows.isEmpty()) return new ArrayList<>();
        Set<String> active = getActiveUids(rows.stream().map(uid).collect(Collectors.toList()));
        return rows.stream().filter(row -> active.contains(uid.apply(row))).collect(Collectors.toList());
    }
}
