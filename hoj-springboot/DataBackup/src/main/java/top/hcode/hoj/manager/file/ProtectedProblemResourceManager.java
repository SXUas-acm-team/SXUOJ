package top.hcode.hoj.manager.file;

import cn.hutool.json.JSONUtil;
import cn.hutool.crypto.SecureUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import io.jsonwebtoken.Claims;
import org.apache.shiro.SecurityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriUtils;
import org.jsoup.parser.Parser;
import top.hcode.hoj.common.exception.StatusFailException;
import top.hcode.hoj.common.exception.StatusForbiddenException;
import top.hcode.hoj.dao.common.AnnouncementEntityService;
import top.hcode.hoj.dao.contest.ContestAnnouncementEntityService;
import top.hcode.hoj.dao.contest.ContestEntityService;
import top.hcode.hoj.dao.contest.ContestProblemEntityService;
import top.hcode.hoj.dao.discussion.DiscussionEntityService;
import top.hcode.hoj.dao.discussion.CommentEntityService;
import top.hcode.hoj.dao.discussion.ReplyEntityService;
import top.hcode.hoj.dao.problem.ProblemEntityService;
import top.hcode.hoj.dao.user.UserInfoEntityService;
import top.hcode.hoj.dao.user.UserRoleEntityService;
import top.hcode.hoj.manager.oj.ContestAccessPolicyManager;
import top.hcode.hoj.mapper.ContestProtectedResourceMapper;
import top.hcode.hoj.pojo.entity.common.Announcement;
import top.hcode.hoj.pojo.entity.contest.Contest;
import top.hcode.hoj.pojo.entity.contest.ContestAnnouncement;
import top.hcode.hoj.pojo.entity.contest.ContestProblem;
import top.hcode.hoj.pojo.entity.discussion.Discussion;
import top.hcode.hoj.pojo.entity.discussion.Comment;
import top.hcode.hoj.pojo.entity.discussion.Reply;
import top.hcode.hoj.pojo.entity.problem.Problem;
import top.hcode.hoj.pojo.entity.user.UserInfo;
import top.hcode.hoj.shiro.AccountProfile;
import top.hcode.hoj.shiro.ShiroConstant;
import top.hcode.hoj.utils.JwtUtils;
import top.hcode.hoj.utils.RedisUtils;
import top.hcode.hoj.validator.ContestValidator;
import top.hcode.hoj.validator.ProblemContentAccessValidator;

import javax.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Local statement images/attachments have the same embargo as the statement itself. */
@Component
public class ProtectedProblemResourceManager {
    private static final Pattern LOCAL_RESOURCE = Pattern.compile(
            "(?:(?:https?:)?//[^/\\s\\\"'<>]+)?/api/public/(?:img|file)/([^\\s\\\"'<>?#)\\]\\}]+)(?:\\?[^\\s\\\"'<>#)\\]\\}]*)?");
    private static final Pattern ENCODED_LINK = Pattern.compile("(?i)(\\b(?:src|href)\\s*=\\s*[\\\"']|\\]\\()([^\\\"'<>\\s)]*)([\\\"']|\\))");
    private static final String TOKEN_PREFIX = "CONTEST_RESOURCE_ACCESS:";
    private static final long TOKEN_SECONDS = 600;
    private static final SecureRandom RANDOM = new SecureRandom();

    @Autowired private ContestAccessPolicyManager contestAccessPolicyManager;
    @Autowired private ContestProtectedResourceMapper protectedResourceMapper;
    @Autowired private ContestEntityService contestEntityService;
    @Autowired private ContestProblemEntityService contestProblemEntityService;
    @Autowired private ProblemEntityService problemEntityService;
    @Autowired private DiscussionEntityService discussionEntityService;
    @Autowired private CommentEntityService commentEntityService;
    @Autowired private ReplyEntityService replyEntityService;
    @Autowired private ContestAnnouncementEntityService contestAnnouncementEntityService;
    @Autowired private AnnouncementEntityService announcementEntityService;
    @Autowired private UserInfoEntityService userInfoEntityService;
    @Autowired private UserRoleEntityService userRoleEntityService;
    @Autowired private ContestValidator contestValidator;
    @Autowired private ProblemContentAccessValidator problemContentAccessValidator;
    @Autowired private RedisUtils redisUtils;
    @Autowired private JwtUtils jwtUtils;

    public void protectProblem(Problem problem, Long cid) {
        problem.setDescription(protectContent(problem.getDescription(), cid));
        problem.setInput(protectContent(problem.getInput(), cid));
        problem.setOutput(protectContent(problem.getOutput(), cid));
        problem.setExamples(protectContent(problem.getExamples(), cid));
        problem.setHint(protectContent(problem.getHint(), cid));
        problem.setSource(protectContent(problem.getSource(), cid));
    }

    /** Only mutates the response text; access capabilities must never be saved to a problem. */
    public String protectContent(String content, Long cid) {
        if (content == null || content.isEmpty()) return content;
        Contest contest = contestEntityService.getById(cid);
        if (contest == null || !contestAccessPolicyManager.isConfidential(contest)
                || contestAccessPolicyManager.isReleased(contest)) return content;
        AccountProfile viewer = (AccountProfile) SecurityUtils.getSubject().getPrincipal();
        boolean root = SecurityUtils.getSubject().hasRole("root");
        try {
            contestValidator.validateContestAuth(contest, viewer, root);
        } catch (StatusFailException | StatusForbiddenException e) {
            throw new org.apache.shiro.authz.AuthorizationException("无权访问保密比赛资源！");
        }
        Matcher matcher = LOCAL_RESOURCE.matcher(normalizeEncodedLinks(content));
        StringBuffer rewritten = new StringBuffer();
        while (matcher.find()) {
            String path = normalizeRelativePath(matcher.group(1));
            remember(cid, path);
            String original = matcher.group();
            String base = original.substring(original.indexOf("/api/public/"));
            if (base.contains("?")) base = base.substring(0, base.indexOf('?'));
            String replacement = base + "?contestResourceToken=" + issueCapability(viewer, root, cid, path);
            matcher.appendReplacement(rewritten, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(rewritten);
        return rewritten.toString();
    }

    /** Invoked before Spring can send a file, including conditional/HEAD/range responses. */
    public void validateResourceRequest(HttpServletRequest request) throws StatusForbiddenException {
        String path = resourcePath(request.getRequestURI());
        List<Contest> protectedBy = findUnreleasedReferences(path);
        if (protectedBy.isEmpty()) return;
        String token = request.getParameter("contestResourceToken");
        if (token != null && token.matches("[A-Za-z0-9_-]{43}")) {
            Object stored = redisUtils.get(TOKEN_PREFIX + token);
            if (stored instanceof String) {
                Map<?, ?> capability = JSONUtil.toBean((String) stored, Map.class);
                String uid = Objects.toString(capability.get("uid"), null);
                Object epoch = redisUtils.get(ShiroConstant.SHIRO_TOKEN_KEY + uid);
                if (Objects.equals(path, capability.get("path")) && epoch != null
                        && Objects.equals(epoch, capability.get("epoch"))
                        && ((Number) capability.get("expires")).longValue() > System.currentTimeMillis()) {
                    AccountProfile viewer = loadViewer(uid);
                    boolean root = Boolean.TRUE.equals(capability.get("root")) && isRoot(uid);
                    Long cid = ((Number) capability.get("cid")).longValue();
                    validateReferenceAccess(protectedBy, cid, viewer, root);
                    return;
                }
            }
        }
        // API clients can also fetch a protected asset with their regular Authorization header.
        String bearer = request.getHeader("Authorization");
        Claims claims = bearer == null ? null : jwtUtils.getClaimByToken(bearer);
        if (claims != null && !jwtUtils.isTokenExpired(claims.getExpiration())
                && jwtUtils.hasValidSession(claims.getSubject(), bearer)) {
            AccountProfile viewer = loadViewer(claims.getSubject());
            Long cid = null;
            try { if (request.getParameter("cid") != null) cid = Long.valueOf(request.getParameter("cid")); }
            catch (NumberFormatException ignored) { /* Invalid context cannot grant access. */ }
            validateReferenceAccess(protectedBy, cid, viewer, isRoot(viewer.getUid()));
            return;
        }
        throw new StatusForbiddenException("该图片或附件属于尚未解封的保密比赛！");
    }

    private void validateReferenceAccess(List<Contest> protectedBy, Long cid, AccountProfile viewer, boolean root)
            throws StatusForbiddenException {
        if (root) return;
        for (Contest contest : protectedBy) {
            if (Objects.equals(cid, contest.getId()) || problemContentAccessValidator.isContestAdministrator(contest, viewer)) {
                try {
                    contestValidator.validateContestAuth(contest, viewer, false);
                    return;
                } catch (StatusFailException e) {
                    throw new StatusForbiddenException("比赛资源不可访问！");
                }
            }
        }
        throw new StatusForbiddenException("只能在获准参赛期间访问本场比赛资源！");
    }

    private AccountProfile loadViewer(String uid) throws StatusForbiddenException {
        UserInfo user = userInfoEntityService.getById(uid);
        if (user == null || user.getStatus() != 0) throw new StatusForbiddenException("登录状态已失效！");
        AccountProfile viewer = new AccountProfile();
        viewer.setUid(uid);
        viewer.setUsername(user.getUsername());
        return viewer;
    }

    private boolean isRoot(String uid) {
        return userRoleEntityService.getRolesByUid(uid).stream()
                .anyMatch(role -> "root".equals(role.getRole()) && !Objects.equals(role.getStatus(), 1));
    }

    private String issueCapability(AccountProfile viewer, boolean root, Long cid, String path) {
        if (viewer == null) throw new IllegalStateException("Authenticated resource access is required.");
        Object epoch = redisUtils.get(ShiroConstant.SHIRO_TOKEN_KEY + viewer.getUid());
        if (!(epoch instanceof String)) throw new IllegalStateException("Login session is unavailable.");
        byte[] bytes = new byte[32]; RANDOM.nextBytes(bytes);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        Map<String, Object> capability = new LinkedHashMap<>();
        capability.put("uid", viewer.getUid()); capability.put("root", root);
        capability.put("epoch", epoch); capability.put("cid", cid); capability.put("path", path);
        capability.put("expires", System.currentTimeMillis() + TOKEN_SECONDS * 1000);
        if (!redisUtils.set(TOKEN_PREFIX + token, JSONUtil.toJsonStr(capability), TOKEN_SECONDS)) {
            throw new IllegalStateException("Cannot persist protected resource access.");
        }
        return token;
    }

    /** Also remembers historical references so editing a statement cannot reopen its old URL. */
    public List<Contest> findUnreleasedReferences(String path) {
        Set<Long> cids = new LinkedHashSet<>(protectedResourceMapper.findContestIds(SecureUtil.sha256(path), path));
        List<Contest> result = new ArrayList<>();
        for (Long cid : cids) {
            Contest contest = contestEntityService.getById(cid);
            if (contest != null && contestAccessPolicyManager.isConfidential(contest)
                    && !contestAccessPolicyManager.isReleased(contest)) result.add(contest);
        }
        return result;
    }

    public void rememberAllConfidentialResources() {
        for (Long cid : contestAccessPolicyManager.listConfidentialContestIds()) rememberContestResources(cid);
    }

    public void rememberContestResources(Long cid) {
        Contest contest = contestEntityService.getById(cid);
        if (contest == null || !contestAccessPolicyManager.isConfidential(contest)
                || contestAccessPolicyManager.isReleased(contest)) return;
        rememberContent(cid, contest.getDescription());
        List<Long> pids = contestProblemEntityService.list(new QueryWrapper<ContestProblem>()
                .eq("cid", contest.getId()).select("pid")).stream().map(ContestProblem::getPid).collect(Collectors.toList());
        for (Long pid : pids) remember(cid, ProblemContentAccessValidator.problemMarker(pid));
        List<Integer> discussionIds = new ArrayList<>();
        if (!pids.isEmpty()) {
            List<Problem> problems = problemEntityService.listByIds(pids).stream().collect(Collectors.toList());
            for (Problem problem : problems) {
                rememberContent(cid, problem.getDescription()); rememberContent(cid, problem.getInput());
                rememberContent(cid, problem.getOutput()); rememberContent(cid, problem.getExamples());
                rememberContent(cid, problem.getHint()); rememberContent(cid, problem.getSource());
                rememberContent(cid, problem.getUserExtraFile());
            }
            List<String> ids = problems.stream().map(Problem::getProblemId).filter(Objects::nonNull).collect(Collectors.toList());
            if (!ids.isEmpty()) {
                for (Discussion discussion : discussionEntityService.list(new QueryWrapper<Discussion>().in("pid", ids))) {
                    rememberContent(cid, discussion.getContent()); rememberContent(cid, discussion.getDescription());
                    discussionIds.add(discussion.getId());
                }
            }
        }
        List<Comment> comments = commentEntityService.list(new QueryWrapper<Comment>().eq("cid", cid)
                .or(!discussionIds.isEmpty()).in(!discussionIds.isEmpty(), "did", discussionIds));
        for (Comment comment : comments) rememberContent(cid, comment.getContent());
        if (!comments.isEmpty()) {
            List<Integer> commentIds = comments.stream().map(Comment::getId).collect(Collectors.toList());
            for (Reply reply : replyEntityService.list(new QueryWrapper<Reply>().in("comment_id", commentIds))) {
                rememberContent(cid, reply.getContent());
            }
        }
        List<Long> aids = contestAnnouncementEntityService.list(new QueryWrapper<ContestAnnouncement>()
                .eq("cid", contest.getId()).select("aid")).stream().map(ContestAnnouncement::getAid).collect(Collectors.toList());
        if (!aids.isEmpty()) {
            for (Announcement announcement : announcementEntityService.listByIds(aids)) {
                rememberContent(cid, announcement.getContent());
            }
        }
    }

    private void rememberContent(Long cid, String content) {
        if (content == null) return;
        Matcher matcher = LOCAL_RESOURCE.matcher(Parser.unescapeEntities(content, true).replace("\\/", "/"));
        while (matcher.find()) remember(cid, normalizeRelativePath(matcher.group(1)));
    }

    private void remember(Long cid, String path) {
        protectedResourceMapper.remember(cid, SecureUtil.sha256(path), path);
    }

    static boolean containsPath(String content, String path) {
        if (content == null) return false;
        Matcher matcher = LOCAL_RESOURCE.matcher(content);
        while (matcher.find()) {
            if (Objects.equals(normalizeRelativePath(matcher.group(1)), path)) return true;
        }
        return false;
    }

    private static String normalizeEncodedLinks(String content) {
        Matcher matcher = ENCODED_LINK.matcher(content);
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String decoded = Parser.unescapeEntities(matcher.group(2), true);
            if (LOCAL_RESOURCE.matcher(decoded).matches()) {
                matcher.appendReplacement(result, Matcher.quoteReplacement(matcher.group(1) + decoded + matcher.group(3)));
            } else {
                matcher.appendReplacement(result, Matcher.quoteReplacement(matcher.group()));
            }
        }
        matcher.appendTail(result);
        return result.toString();
    }

    public static String resourcePath(String uri) {
        String prefix = uri.startsWith("/api/public/img/") ? "/api/public/img/" : "/api/public/file/";
        if (!uri.startsWith(prefix)) throw new IllegalArgumentException("Invalid local resource path.");
        return normalizeRelativePath(uri.substring(prefix.length()));
    }

    private static String normalizeRelativePath(String path) {
        String decoded = UriUtils.decode(path, StandardCharsets.UTF_8);
        if (decoded.isEmpty() || decoded.length() > 512 || decoded.startsWith("/") || decoded.contains("\\")
                || decoded.contains("%") || decoded.contains(";") || decoded.chars().anyMatch(c -> c < 32 || c == 127)) {
            throw new IllegalArgumentException("Invalid local resource path.");
        }
        for (String segment : decoded.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) throw new IllegalArgumentException("Invalid local resource path.");
        }
        // Upload names are UUIDs; folding case also protects development on Windows' filesystem.
        return decoded.toLowerCase(Locale.ROOT);
    }
}
