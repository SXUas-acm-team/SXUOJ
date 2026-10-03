package top.hcode.hoj.utils;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

/** Bounds for request-controlled queries and public profile links. */
public final class RequestLimits {
    private RequestLimits() { }

    public static int pageSize(Integer value, int fallback) {
        return value == null || value < 1 ? fallback : Math.min(value, 100);
    }

    public static int pageNumber(Integer value) {
        return value == null || value < 1 ? 1 : Math.min(value, 1000000);
    }

    public static <T> List<T> boundedDistinct(Collection<T> values, int maximum) {
        LinkedHashSet<T> result = new LinkedHashSet<>();
        if (values != null) {
            int examined = 0;
            for (T value : values) {
                if (++examined > maximum) break;
                if (value != null && (!(value instanceof String) || ((String) value).length() <= 128)) {
                    result.add(value);
                }
            }
        }
        return new ArrayList<>(result);
    }

    public static String normalizeAccount(String value) {
        if (value == null) return null;
        int begin = 0, end = value.length();
        while (begin < end && isSpace(value.charAt(begin))) begin++;
        while (end > begin && isSpace(value.charAt(end - 1))) end--;
        return value.substring(begin, end);
    }

    private static boolean isSpace(char value) {
        return Character.isWhitespace(value) || Character.isSpaceChar(value) || value == '\uFEFF';
    }

    public static boolean isSafeProfileUrl(String value) {
        if (value == null || value.trim().isEmpty()) return true;
        try {
            URI uri = new URI(value.trim());
            return ("https".equalsIgnoreCase(uri.getScheme()) || "http".equalsIgnoreCase(uri.getScheme()))
                    && uri.getHost() != null && uri.getUserInfo() == null;
        } catch (Exception ignored) {
            return false;
        }
    }
}
