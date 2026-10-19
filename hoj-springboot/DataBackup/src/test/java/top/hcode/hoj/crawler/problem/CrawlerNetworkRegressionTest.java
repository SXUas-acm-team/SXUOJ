package top.hcode.hoj.crawler.problem;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Invalid identifiers must fail at the entry point before any remote connection/login. */
class CrawlerNetworkRegressionTest {
    @Test void supportsHistoricalAtCoderAndSpOjIdentifiers() {
        for (String id : new String[]{"abc001_1", "ABC110_A", "dp_a", "code-festival-2018-quala_a", "practice2_a"}) {
            assertNotNull(RemoteProblemId.atCoder(id));
        }
        for (String id : new String[]{"TEST", "tHeNeXt", "ADABEHIVE", "ABC_DEF1"}) {
            assertNotNull(RemoteProblemId.spoj(id));
        }
        assertEquals("abc110_a", RemoteProblemId.atCoder("ABC110_A"));
        assertEquals("TEST", RemoteProblemId.spoj("test"));
    }

    @Test void supportsMultipleLetterAndLegacyNumericCodeforcesIndices() {
        assertArrayEquals(new String[]{"123", "A"}, RemoteProblemId.codeforces("123A"));
        assertArrayEquals(new String[]{"123", "A1"}, RemoteProblemId.codeforces("123A1"));
        assertArrayEquals(new String[]{"123", "AA"}, RemoteProblemId.codeforces("123AA"));
        assertArrayEquals(new String[]{"123", "AA12"}, RemoteProblemId.codeforces("123AA12"));
        assertArrayEquals(new String[]{"12345", "01"}, RemoteProblemId.codeforces("1234501"));
    }

    @Test void atCoderRejectsPathQueryAndMarkupBeforeFetching() {
        for (String id : new String[]{null, "", "abc110", "abc110_a?redirect=evil", "abc110_a/../../login", "abc110_a#fragment",
                "abc110_a' onclick='alert(1)", "abc110_a%2f..", "abc110_a\r\nHost: other", "_a"}) {
            assertThrows(IllegalArgumentException.class, () -> new AtCoderProblemStrategy().getProblemInfo(id, "author"), String.valueOf(id));
        }
    }

    @Test void spojRejectsPathQueryAndMarkupBeforeFetching() {
        for (String id : new String[]{null, "", "TEST/../../login", "TEST?foo=bar", "TEST#fragment", "TEST' onclick='alert(1)",
                "TEST%2f..", "TEST\r\nCookie: injected"}) {
            assertThrows(IllegalArgumentException.class, () -> new SPOJProblemStrategy().getProblemInfo(id, "author"), String.valueOf(id));
        }
    }

    @Test void codeforcesRequiresWholeInputAndGymDoesNotFetchFallbackForInvalidInput() {
        for (String id : new String[]{null, "", "123A/../../enter", "garbage123A", "123A?foo=bar", "123A'", "123A#fragment", "0A", "12"}) {
            assertThrows(IllegalArgumentException.class, () -> new CFProblemStrategy().getProblemInfo(id, "author"), String.valueOf(id));
            assertThrows(IllegalArgumentException.class, () -> new GYMProblemStrategy().getProblemInfo(id, "author"), String.valueOf(id));
        }
    }

    @Test void invalidGymIdentifierIsRejectedBeforeCredentialLogin() {
        GYMProblemStrategy strategy = new GYMProblemStrategy() {
            @Override public void login(String username, String password) {
                fail("Must not attempt login for an invalid problem identifier");
            }
        };
        assertThrows(IllegalArgumentException.class,
                () -> strategy.getProblemInfoByLogin("123A?injected=1", "author", "test", "test"));
    }
}
