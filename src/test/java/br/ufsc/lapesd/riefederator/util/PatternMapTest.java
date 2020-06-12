package br.ufsc.lapesd.riefederator.util;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

@Test(groups = {"fast"})
public class PatternMapTest {

    @DataProvider
    public static Object[][] entryTestData() {
        return Stream.of(
                asList(".*",     null, "asd",   "asd", true),
                asList("asd",    null, "asd",   "asd", true),
                asList("asd",    null, "asd",   "qwe", true),
                asList("asd",    null, "asd_",  "qwe", false),
                asList("asd", "qwe.*", "asd",   "qwe", true),
                asList("asd", "qwe.*", "asd",  "qwe_", true),
                asList("asd", "qwe.*", "asd",   "asd", false),
                asList( null, "qwe.*", "asd",   "qwe", true),
                asList( null, "qwe.*", "asd",  "qwe_", true),
                asList( null, "qwe.*", "asd",   "asd", false),
                asList( null, "qwe.*", "qwe",   "qwe", true),
                asList( null, "qwe.*", "qwe",  "qwe_", true),
                asList( null, "qwe.*", "qwe",   "asd", false)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "entryTestData")
    public void testEntry(String rx1, String rx2, String s1, String s2, boolean expected) {
        Predicate<String> p1 = rx1 == null ? PatternMap.ANY : new PatternPredicate(rx1);
        Predicate<String> p2 = rx2 == null ? PatternMap.ANY : new PatternPredicate(rx2);

        PatternMap.Entry<Integer> entry = new PatternMap.Entry<>(p1, p2, 23);
        assertEquals(entry.matches(s1, s2), expected);
    }

    @Test
    public void testGetFromEmpty() {
        PatternMap<Integer> map = new PatternMap<>();
        assertNull(map.getLast("asd", "qwe"));
    }

    @Test
    public void testGetFreeSecond() {
        PatternMap<Integer> map = new PatternMap<>();
        map.add(new PatternPredicate("asd"), PatternMap.ANY, 23);
        assertEquals(map.getLast("asd", "qwe"), Integer.valueOf(23));
        assertEquals(map.getLast("asd", "asd"), Integer.valueOf(23));
        assertNull(map.getLast("qwe", "asd"));
    }

    @Test
    public void testGetBoth() {
        PatternMap<Integer> map = new PatternMap<>();
        map.add(new PatternPredicate("asd.*"), new PatternPredicate("qwe.*"), 23);
        map.add(new PatternPredicate("asd.*"), new PatternPredicate("qwe"), 27);
        assertEquals(map.getLast("asd", "qwe"), Integer.valueOf(27));
        assertEquals(map.getLast("asd", "qwex"), Integer.valueOf(23));
        assertEquals(map.getLast("asd_", "qwex"), Integer.valueOf(23));
        assertEquals(map.getLast("asd_", "qwe"), Integer.valueOf(27));
        assertNull(map.getLast("asd_", "asd"));
    }

    @Test
    public void testComplex() {
        PatternMap<Integer> map = new PatternMap<>();
        map.add(new PatternPredicate("asd"), PatternMap.ANY, 23);
        map.add(new PatternPredicate("asd"), new PatternPredicate("qwe.*"), 27);
        map.add(new PatternPredicate("asd"), new PatternPredicate("qwe"), 31);
        map.add(new PatternPredicate("asd.*"), new PatternPredicate("qwe"), 37);
        assertEquals(map.getLast("asd", ""), Integer.valueOf(23));
        assertEquals(map.getLast("asd", "x"), Integer.valueOf(23));
        assertEquals(map.getLast("asd", "qwe"), Integer.valueOf(37));
        assertEquals(map.getLast("asd", "qwe_"), Integer.valueOf(27));
        assertEquals(map.getLast("asd", "qw"), Integer.valueOf(23));
        assertEquals(map.getLast("asd_", "qwe"), Integer.valueOf(37));
        assertNull(map.getLast("_asd", "qwe"));
        assertNull(map.getLast("as", "qwe"));
    }

}