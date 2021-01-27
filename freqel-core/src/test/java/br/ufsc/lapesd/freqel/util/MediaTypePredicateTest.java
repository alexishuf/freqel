package br.ufsc.lapesd.freqel.util;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

@Test(groups = {"fast"})
public class MediaTypePredicateTest {
    @DataProvider
    public static Object[][] data() {
        return Stream.of(
                asList("text/plain", "text/plain", true),
                asList("text/plain", "text/turtle", false),
                asList("text/*",     "text/turtle", true),
                asList("text/*",     "text/*", true),
                asList("text/plain; charset=UTF-8", "text/plain",               false),
                asList("text/plain; charset=UTF-8", "text/plain; charset=UTF-8", true),
                asList("text/*; charset=UTF-8",     "text/plain; charset=UTF-8", true)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "data")
    public void test(String pattern, String candidate, boolean expected) {
        MediaType patternMT = MediaType.valueOf(pattern);

        assertEquals(new MediaTypePredicate(patternMT).test(candidate), expected);
        assertEquals(new MediaTypePredicate(pattern).test(candidate), expected);
        assertEquals(new MediaTypePredicate(pattern), new MediaTypePredicate(patternMT));
        if (!candidate.equals(pattern))
            assertNotEquals(new MediaTypePredicate(pattern), new MediaTypePredicate(candidate));
    }
}