package br.ufsc.lapesd.riefederator.query;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class CardinalityTest {

    @DataProvider
    public static Object[][] parseData() {
        return Stream.of(
                asList("23", Cardinality.exact(23)),
                asList("LOWER_BOUND(27)", Cardinality.lowerBound(27)),
                asList("UPPER_BOUND(31)", Cardinality.upperBound(31)),
                asList("GUESS(37)", Cardinality.guess(37)),
                asList("NON_EMPTY", Cardinality.NON_EMPTY),
                asList("UNSUPPORTED", Cardinality.UNSUPPORTED),
                asList("lower_bound(27)", Cardinality.lowerBound(27)),
                asList("exact_bound(27)", null),
                asList("BOUND(27)", null),
                asList("lower(23)", null)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "parseData")
    public void testParse(String string, Cardinality expected) {
        Cardinality actual = Cardinality.parse(string);
        if (actual != null) {
            assertEquals(actual.getReliability(), expected.getReliability());
            assertEquals(actual.getValue(0), expected.getValue(0));
        }
        assertEquals(actual, expected);
    }
}