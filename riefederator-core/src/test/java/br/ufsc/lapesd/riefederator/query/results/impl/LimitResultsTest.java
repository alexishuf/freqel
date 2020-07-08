package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;

@Test(groups = {"fast"})
public class LimitResultsTest implements TestContext {
    @DataProvider
    public static Object[][] sizeData() {
        return Stream.of(
                asList(2, 4),
                asList(1, 4),
                asList(2, 2),
                asList(4, 2),
                asList(1, 100),
                asList(10, 100),
                asList(100, 1000),
                asList(1000, 1001),
                asList(1000, 2)
        ).map(List::toArray).toArray(Object[][]::new);
    }

    private @Nonnull CollectionResults createResults(int size) {
        ArraySolution.ValueFactory fac = ArraySolution.forVars(singleton("x"));
        List<Solution> list = new ArrayList<>();
        for (int i = 0; i < size; i++)
            list.add(fac.fromValues(lit(i)));
        return new CollectionResults(list, singleton("x"));
    }

    @Test(dataProvider = "sizeData")
    public void testEnforcesLimit(int limit, int inputs) {
        LimitResults limitResults = new LimitResults(createResults(inputs), limit);
        CollectionResults greedy = CollectionResults.greedy(limitResults);
        assertEquals(greedy, createResults(Math.min(limit, inputs)));
    }

    @Test(dataProvider = "sizeData")
    public void testEnforceAndReset(int limit, int inputs) {
        LimitResults limitResults = new LimitResults(createResults(inputs), limit);
        List<Solution> solutions = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            solutions.clear();
            limitResults.forEachRemaining(solutions::add);
            assertEquals(new CollectionResults(solutions, singleton("x")),
                         createResults(Math.min(limit, inputs)));
            limitResults.reset(true);
        }
    }
}