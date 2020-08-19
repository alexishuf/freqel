package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.LazyCartesianResults;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import com.google.common.collect.Sets;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults.wrapSameVars;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class CartesianResultsTest implements TestContext {
    public static final List<BiFunction<Collection<Results>, Set<String>, Results>> factories =
            singletonList(
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

    @DataProvider
    public static @Nonnull Object[][] emptyProductData() {
        return Arrays.stream(factoriesData()).map(a -> a[0]).flatMap(f -> Stream.of(
                asList(f, 2, 0, false),
                asList(f, 2, 0, true),
                asList(f, 0, 2, false),
                asList(f, 0, 2, true)
        )).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "emptyProductData")
    public void testEmptyProduct(BiFunction<Collection<Results>, Set<String>, Results> f,
                                 int lSize, int rSize, boolean markOptional) {
        CollectionResults left = createResults(lSize, "x");
        CollectionResults right = createResults(rSize, "y");
        assertFalse(lSize == 0 && rSize == 0);
        assertTrue(lSize == 0 || rSize == 0);
        if (markOptional)
            (lSize == 0 ? left : right).setOptional(true);
        assertEquals(left.isOptional(), lSize == 0 && markOptional);
        assertEquals(right.isOptional(), rSize == 0 && markOptional);

        Results results = f.apply(asList(left, right), Sets.newHashSet("x", "y"));
        List<Solution> actual = new ArrayList<>();
        results.forEachRemainingThenClose(actual::add);

        List<Solution> expected = new ArrayList<>();
        if (markOptional) {
            createSolutions(lSize == 0 ? rSize : lSize, lSize == 0 ? "y" : "x").stream()
                    .map(s -> MapSolution.builder(s).put(lSize == 0 ? x : y, null).build())
                    .forEach(expected::add);
        }// else: eliminates all solutions
        assertEquals(actual, expected);
    }

    @Test(dataProvider = "factoriesData")
    public void testEmptyRightOptional(BiFunction<Collection<Results>, Set<String>, Results> f) {
        CollectionResults left = createResults(1, "x");
        CollectionResults right = createResults(0, "y");
        right.setOptional(true);
        Results results = f.apply(asList(left, right), Sets.newHashSet("x", "y"));

        List<Solution> list = new ArrayList<>();
        results.forEachRemainingThenClose(list::add);

        assertEquals(list, singletonList(
                MapSolution.builder().put(x, uri("x", 0)).put(y, null).build())
        );
    }

    @Test(dataProvider = "factoriesData")
    public void testEmptyLeftOptional(BiFunction<Collection<Results>, Set<String>, Results> f) {
        CollectionResults left = createResults(0, "x");
        left.setOptional(true);
        CollectionResults right = createResults(1, "y");
        Results results = f.apply(asList(left, right), Sets.newHashSet("x", "y"));

        List<Solution> list = new ArrayList<>();
        results.forEachRemainingThenClose(list::add);

        assertEquals(list, singletonList(
                MapSolution.builder().put(x, null).put(y, uri("y", 0)).build())
        );
    }

    @Test(dataProvider = "factoriesData")
    public void testNonEmptyLeftOptional(BiFunction<Collection<Results>, Set<String>, Results> f) {
        CollectionResults left = createResults(2, "x");
        left.setOptional(true);
        CollectionResults right = createResults(2, "y");
        Results results = f.apply(asList(left, right), Sets.newHashSet("x", "y"));

        List<Solution> list = new ArrayList<>();
        results.forEachRemainingThenClose(list::add);
        HashSet<Solution> set = new HashSet<>(list);

        assertEquals(set, Sets.newHashSet(
                MapSolution.builder().put(x, uri("x", 0)).put(y, uri("y", 0)).build(),
                MapSolution.builder().put(x, uri("x", 0)).put(y, uri("y", 1)).build(),
                MapSolution.builder().put(x, uri("x", 1)).put(y, uri("y", 0)).build(),
                MapSolution.builder().put(x, uri("x", 1)).put(y, uri("y", 1)).build()
        ));
        assertEquals(list.size(), 4);
    }

    @Test(dataProvider = "factoriesData")
    public void testSingleResultOptional(BiFunction<Collection<Results>, Set<String>, Results> f) {
        CollectionResults left = createResults(2, "x");
        CollectionResults right = createResults(1, "y");
        right.setOptional(true);
        Results results = f.apply(asList(left, right), Sets.newHashSet("x", "y"));

        List<Solution> list = new ArrayList<>();
        results.forEachRemainingThenClose(list::add);
        HashSet<Solution> set = new HashSet<>(list);

        assertEquals(set, Sets.newHashSet(
                MapSolution.builder().put(x, uri("x", 0)).put(y, uri("y", 0)).build(),
                MapSolution.builder().put(x, uri("x", 1)).put(y, uri("y", 0)).build()
        ));
    }
}