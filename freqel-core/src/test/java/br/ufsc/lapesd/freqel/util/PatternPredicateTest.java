package br.ufsc.lapesd.freqel.util;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

@Test(groups = {"fast"})
public class PatternPredicateTest {

    @DataProvider
    public static Object[][] data() {
        return Stream.of(
                asList("asd",       "asd",    true),
                asList("asd",       ".*",     false),
                asList("asd.*",     "asdqwe", true),
                asList("asd.*",     "ASD",    false),
                asList("(?i)asd.*", "ASD",    true),
                asList("asd",       " asd",   false),
                asList("asd",       ".*asd",  false),
                asList("asd",       "as d",   false)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "data")
    public void test(String rx, String candidate, boolean expected) {
        Pattern pattern = Pattern.compile(rx);
        assertEquals(new PatternPredicate(pattern).test(candidate), expected);
        assertEquals(new PatternPredicate(pattern), new PatternPredicate(rx));
        assertEquals(new PatternPredicate(pattern).equals(new PatternPredicate(candidate)),
                     rx.equals(candidate));
    }
}