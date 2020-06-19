package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.EagerCartesianResults;
import br.ufsc.lapesd.riefederator.query.results.impl.LazyCartesianResults;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.BiFunction;

import static br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults.wrapSameVars;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class CartesianResultsTest {
    public static final List<BiFunction<Collection<Results>, Set<String>, Results>> factories =
            asList(
                    (collection, vars) -> {
                        ArrayList<Results> list = new ArrayList<>(collection);
                        Results first = list.remove(0);
                        return new EagerCartesianResults(first, list, vars);
                    },
                    LazyCartesianResults::new
            );

    private @Nonnull CollectionResults createResults(int count, String... varNames) {
        return new CollectionResults(createSolutions(count, varNames),
                                     Sets.newHashSet(varNames));
    }

    private @Nonnull List<Solution> createSolutions(int count, String... varNames) {
        List<Solution> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            MapSolution.Builder builder = MapSolution.builder();
            for (String name : varNames) builder.put(name, uri(name, i));
            list.add(builder.build());
        }
        return list;
    }

    private @Nonnull StdURI uri(String name, int i) {
        return new StdURI("http://example.org/" + name + "/" + i);
    }

    @DataProvider
    public static Object[][] factoriesData() {
        return factories.stream().map(f -> new Object[]{f}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "factoriesData")
    public void testEmpty(BiFunction<Collection<Results>, Set<String>, Results> f) {
        CollectionResults in = createResults(0, "x");
        Results r = f.apply(singletonList(in), Sets.newHashSet("x"));
        assertFalse(r.hasNext());
    }

    @Test(dataProvider = "factoriesData")
    public void testNoProduct(BiFunction<Collection<Results>, Set<String>, Results> f) {
        CollectionResults in = createResults(2, "x");
        Results r = f.apply(singletonList(in), Sets.newHashSet("x"));
        assertTrue(r.hasNext());

        List<Solution> actual = new ArrayList<>();
        r.forEachRemainingThenClose(actual::add);
        assertEquals(actual, in.getCollection());
    }

    @Test(dataProvider = "factoriesData")
    public void testProductWithSingleSingleton(BiFunction<Collection<Results>, Set<String>, Results> f) {
        CollectionResults largest = createResults(2, "x");
        List<Solution> list1 = createSolutions(1, "y");
        Results r = f.apply(asList(largest, wrapSameVars(list1)), Sets.newHashSet("x", "y"));

        assertTrue(r.hasNext());

        List<Solution> list = new ArrayList<>();
        r.forEachRemaining(list::add);
        assertEquals(list, asList(
                MapSolution.builder().put("x", uri("x", 0))
                                     .put("y", uri("y", 0)).build(),
                MapSolution.builder().put("x", uri("x", 1))
                                     .put("y", uri("y", 0)).build()
                ));
    }

    @Test(dataProvider = "factoriesData")
    public void testProductWithSingle(BiFunction<Collection<Results>, Set<String>, Results> f) {
        CollectionResults largest = createResults(2, "x");
        List<Solution> list1 = createSolutions(2, "y");
        Results r = f.apply(asList(largest, wrapSameVars(list1)), Sets.newHashSet("x", "y"));

        assertTrue(r.hasNext());

        List<Solution> list = new ArrayList<>();
        r.forEachRemaining(list::add);
        HashSet<Solution> set = new HashSet<>(list);
        assertEquals(set, Sets.newHashSet(
                MapSolution.builder().put("x", uri("x", 0))
                                     .put("y", uri("y", 0)).build(),
                MapSolution.builder().put("x", uri("x", 0))
                                     .put("y", uri("y", 1)).build(),
                MapSolution.builder().put("x", uri("x", 1))
                                     .put("y", uri("y", 0)).build(),
                MapSolution.builder().put("x", uri("x", 1))
                                     .put("y", uri("y", 1)).build()
        ));
        assertEquals(list.size(), set.size());
    }


    @Test(dataProvider = "factoriesData")
    public void testProductWithSingleAndEmpty(BiFunction<Collection<Results>, Set<String>, Results> f) {
        CollectionResults largest = createResults(2, "x");
        List<Solution> list1 = createSolutions(2, "y");
        List<Solution> list2 = emptyList();
        Results r = f.apply(asList(largest, wrapSameVars(list1), wrapSameVars(list2)),
                            Sets.newHashSet("x", "y"));

        assertFalse(r.hasNext());
        expectThrows(NoSuchElementException.class, r::next);
    }

    @Test(dataProvider = "factoriesData")
    public void testProductWithTwo(BiFunction<Collection<Results>, Set<String>, Results> f) {
        CollectionResults largest = createResults(2, "x");
        List<Solution> list1 = createSolutions(2, "y");
        List<Solution> list2 = createSolutions(2, "z");
        Results r = f.apply(asList(largest, wrapSameVars(list1), wrapSameVars(list2)),
                            Sets.newHashSet("x", "y", "z"));

        assertTrue(r.hasNext());

        List<Solution> list = new ArrayList<>();
        r.forEachRemaining(list::add);
        HashSet<Solution> set = new HashSet<>(list);
        assertEquals(set, Sets.newHashSet(
                MapSolution.builder().put("x", uri("x", 0))
                                     .put("y", uri("y", 0))
                                     .put("z", uri("z", 0)).build(),
                MapSolution.builder().put("x", uri("x", 0))
                                     .put("y", uri("y", 0))
                                     .put("z", uri("z", 1)).build(),
                MapSolution.builder().put("x", uri("x", 0))
                                     .put("y", uri("y", 1))
                                     .put("z", uri("z", 0)).build(),
                MapSolution.builder().put("x", uri("x", 0))
                                     .put("y", uri("y", 1))
                                     .put("z", uri("z", 1)).build(),

                MapSolution.builder().put("x", uri("x", 1))
                                     .put("y", uri("y", 0))
                                     .put("z", uri("z", 0)).build(),
                MapSolution.builder().put("x", uri("x", 1))
                                     .put("y", uri("y", 0))
                                     .put("z", uri("z", 1)).build(),
                MapSolution.builder().put("x", uri("x", 1))
                                     .put("y", uri("y", 1))
                                     .put("z", uri("z", 0)).build(),
                MapSolution.builder().put("x", uri("x", 1))
                                     .put("y", uri("y", 1))
                                     .put("z", uri("z", 1)).build()
        ));
        assertEquals(list.size(), set.size());
    }
}