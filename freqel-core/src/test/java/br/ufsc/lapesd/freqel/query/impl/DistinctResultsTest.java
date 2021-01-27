package br.ufsc.lapesd.freqel.query.impl;

import br.ufsc.lapesd.freqel.NamedFunction;
import br.ufsc.lapesd.freqel.ResultsAssert;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class DistinctResultsTest {
    private static final List<NamedFunction<Results, Results>> factories = asList(
            new NamedFunction<>("HashDistinctResults",
                                HashDistinctResults::applyIfNotDistinct),
            new NamedFunction<>("WindowDistinctResults",
                                WindowDistinctResults::applyIfNotDistinct)
    );

    private static @Nonnull StdURI ex(int local) {
        return new StdURI("http://example.org/"+local);
    }

    @DataProvider
    public static @Nonnull Object[][] factoriesData() {
        return factories.stream().map(f -> new Object[]{f}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "factoriesData")
    public void testEmpty(Function<Results, Results> factory) {
        CollectionResults in = new CollectionResults(Collections.emptyList(), singleton("x"));
        Results d = factory.apply(in);
        assertEquals(d.getVarNames(), singleton("x"));
        assertFalse(d.hasNext());
    }

    @Test(dataProvider = "factoriesData")
    public void testNoDuplicates(Function<Results, Results> factory) {
        CollectionResults in = new CollectionResults(asList(
                MapSolution.build("x", ex(1)),
                MapSolution.build("x", ex(2))), singleton("x"));
        Results d = factory.apply(in);
        assertEquals(d.getVarNames(), singleton("x"));

        assertTrue(d.hasNext());
        assertEquals(d.next(), MapSolution.build("x", ex(1)));

        assertTrue(d.hasNext());
        assertEquals(d.next(), MapSolution.build("x", ex(2)));

        assertFalse(d.hasNext());
    }

    @Test(dataProvider = "factoriesData")
    public void testDuplicates(Function<Results, Results> factory) {
        CollectionResults in = new CollectionResults(asList(
                MapSolution.builder().put("x", ex(1)).put("y", ex(2)).build(),
                MapSolution.builder().put("x", ex(2)).put("y", ex(1)).build(),
                MapSolution.builder().put("x", ex(1)).put("y", ex(2)).build(),
                MapSolution.builder().put("x", ex(3)).put("y", ex(1)).build()),
                newHashSet("x", "y"));
        Results d = factory.apply(in);
        assertEquals(d.getVarNames(), newHashSet("x", "y"));

        assertTrue(d.hasNext());
        assertEquals(d.next(), MapSolution.builder().put("x", ex(1))
                                                    .put("y", ex(2)).build());

        assertTrue(d.hasNext());
        assertEquals(d.next(), MapSolution.builder().put("x", ex(2))
                                                    .put("y", ex(1)).build());

        assertTrue(d.hasNext());
        assertEquals(d.next(), MapSolution.builder().put("x", ex(3))
                                                    .put("y", ex(1)).build());

        assertFalse(d.hasNext());
    }

    @Test(dataProvider = "factoriesData")
    public void testLargeSequentialDuplicates(Function<Results, Results> factory) {
        ArraySolution.ValueFactory solFac = ArraySolution.forVars(singletonList("x"));
        List<Solution> list = new ArrayList<>(), expected = new ArrayList<>();
        for (int i = 0; i < 500000; i++) {
            ArraySolution solution = solFac.fromValues(ex(i));
            expected.add(solution);
            list.add(solution);
            list.add(solution);
        }
        CollectionResults in = new CollectionResults(list, singleton("x"));
        ResultsAssert.assertExpectedResults(factory.apply(in), expected);
    }
}