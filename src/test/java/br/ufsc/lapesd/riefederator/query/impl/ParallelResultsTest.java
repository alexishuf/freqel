package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.query.results.impl.ParallelResults;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;

public class ParallelResultsTest {
    private static final String EX = "http://example.org/";


    private static @Nonnull CollectionResults createMock(int count, @Nonnull String uriPrefix,
                                                         @Nonnull Collection<String> varNames) {
        List<Solution> solutions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            MapSolution.Builder builder = MapSolution.builder();
            for (String name : varNames)
                builder.put(name, new StdURI(uriPrefix + i));
            solutions.add(builder.build());
        }
        return new CollectionResults(solutions, new HashSet<>(varNames));
    }

    @DataProvider
    public static Object[][] consumeSingleData() {
        return new Object[][] {
            new Object[]{createMock(1, EX+"1/", singleton("x"))},
            new Object[]{createMock(3, EX+"1/", singleton("x"))},
            new Object[]{createMock(3, EX+"1/", singleton("x"))},
            new Object[]{createMock(3, EX+"1/", singleton("x"))},
            new Object[]{createMock(3, EX+"1/", singleton("x"))},
        };
    }

    @Test(dataProvider = "consumeSingleData")
    public void testConsumeSingle(CollectionResults in) {
        ParallelResults parallel = new ParallelResults(singletonList(in));
        Set<Solution> all = new HashSet<>();
        parallel.forEachRemaining(all::add);

        Set<Solution> expected = new HashSet<>(in.getCollection());
        assertEquals(all, expected);
    }

    @Test(dataProvider = "consumeSingleData")
    public void testCardinalitySingle(CollectionResults in) {
        ParallelResults parallel = new ParallelResults(singletonList(in));
        List<Solution> actual = new ArrayList<>();
        parallel.forEachRemaining(actual::add);
        assertEquals(actual, new ArrayList<>(in.getCollection()));
    }

    @Test
    public void testTwoSameVar() {
        List<Results> list = asList(
                createMock(3, EX + "1/", singletonList("x")),
                createMock(3, EX + "2/", singletonList("x"))
        );
        ParallelResults parallelResults = new ParallelResults(list);
        assertEquals(parallelResults.getVarNames(), singleton("x"));

        Set<Solution> all = new HashSet<>(), expected = new HashSet<>();
        for (Results results : list)
            expected.addAll(((CollectionResults) results).getCollection());
        parallelResults.forEachRemaining(all::add);
        assertEquals(all, expected);
    }

    @Test
    public void testTwoDifferentVars() {
        List<Results> list = asList(
                createMock(3, EX + "1/", singletonList("x")),
                createMock(3, EX + "2/", singletonList("y"))
        );
        ParallelResults parallelResults = new ParallelResults(list);
        assertEquals(parallelResults.getVarNames(), Sets.newHashSet("x", "y"));

        Set<Solution> all = new HashSet<>(), expected = new HashSet<>();
        for (Results results : list)
            expected.addAll(((CollectionResults) results).getCollection());
        parallelResults.forEachRemaining(all::add);
        assertEquals(all, expected);
    }

    @Test
    public void testThreeDifferentVarsAndCardinality() {
        List<Results> list = asList(
                createMock(3, EX + "1/", singletonList("x")),
                createMock(3, EX + "2/", singletonList("y")),
                createMock(3, EX + "3/", singletonList("x"))
        );
        ParallelResults parallelResults = new ParallelResults(list);
        assertEquals(parallelResults.getVarNames(), Sets.newHashSet("x", "y"));

        Set<Solution> all = new HashSet<>(), expected = new HashSet<>();
        for (Results results : list)
            expected.addAll(((CollectionResults) results).getCollection());
        parallelResults.forEachRemaining(all::add);
        assertEquals(all, expected);
    }
}