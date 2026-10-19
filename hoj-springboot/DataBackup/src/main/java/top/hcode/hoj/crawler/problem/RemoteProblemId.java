package top.hcode.hoj.crawler.problem;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Platform identifiers are data, never URL paths, queries, or HTML fragments. */
final class RemoteProblemId {
    private static final Pattern CODEFORCES = Pattern.compile("([1-9][0-9]{0,9})([A-Z]+[0-9]*|[0-9]{2})");

    private RemoteProblemId() { }

    static String atCoder(String id) {
        if (id == null || id.length() > 128
                || !id.matches("[A-Za-z0-9-]+_[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("AtCoder: Incorrect problem id format! Must be like `abc110_a`");
        }
        return id.toLowerCase(Locale.ROOT);
    }

    static String spoj(String id) {
        if (id == null || id.length() > 80 || !id.matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("SPOJ: Incorrect problem id format!");
        }
        return id.toUpperCase(Locale.ROOT);
    }

    static String[] codeforces(String id) {
        if (id == null || id.length() > 64) {
            throw new IllegalArgumentException("Codeforces: Incorrect problem id format!");
        }
        Matcher matcher = CODEFORCES.matcher(id);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Codeforces: Incorrect problem id format!");
        }
        return new String[]{matcher.group(1), matcher.group(2)};
    }
}
