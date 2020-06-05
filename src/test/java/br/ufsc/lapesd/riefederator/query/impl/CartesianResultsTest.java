package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.CartesianResults;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults.wrapSameVars;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

public class CartesianResultsTest {
    private @Nonnull
    CollectionResults createResults(int count, String... varNames) {
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

    @Test
    public void testEmpty() {
        CollectionResults in = createResults(0, "x");
        CartesianResults r = new CartesianResults(in, emptyList(), Sets.newHashSet("x"));
        assertFalse(r.hasNext());
    }

    @Test
    public void testNoProduct() {
        CollectionResults in = createResults(2, "x");
        CartesianResults r = new CartesianResults(in, emptyList(), Sets.newHashSet("x"));
        assertTrue(r.hasNext());

        List<Solution> actual = new ArrayList<>();
        r.forEachRemainingThenClose(actual::add);
        assertEquals(actual, in.getCollection());
    }

    @Test
    public void testProductWithSingleSingleton() {
        CollectionResults largest = createResults(2, "x");
        List<Solution> list1 = createSolutions(1, "y");
        CartesianResults r = new CartesianResults(largest, singletonList(wrapSameVars(list1)),
                                                  Sets.newHashSet("x", "y"));

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

    @Test
    public void testProductWithSingle() {
        CollectionResults largest = createResults(2, "x");
        List<Solution> list1 = createSolutions(2, "y");
        CartesianResults r = new CartesianResults(largest, singletonList(wrapSameVars(list1)),
                                                  Sets.newHashSet("x", "y"));

        assertTrue(r.hasNext());

        List<Solution> list = new ArrayList<>();
        r.forEachRemaining(list::add);
        assertEquals(list, asList(
                MapSolution.builder().put("x", uri("x", 0))
                                     .put("y", uri("y", 0)).build(),
                MapSolution.builder().put("x", uri("x", 0))
                                     .put("y", uri("y", 1)).build(),
                MapSolution.builder().put("x", uri("x", 1))
                                     .put("y", uri("y", 0)).build(),
                MapSolution.builder().put("x", uri("x", 1))
                                     .put("y", uri("y", 1)).build()
        ));
    }


    @Test
    public void testProductWithSingleAndEmpty() {
        CollectionResults largest = createResults(2, "x");
        List<Solution> list1 = createSolutions(2, "y");
        List<Solution> list2 = emptyList();
        CartesianResults r = new CartesianResults(largest,
                                                  asList(wrapSameVars(list1), wrapSameVars(list2)),
                                                  Sets.newHashSet("x", "y"));

        assertFalse(r.hasNext());
        expectThrows(NoSuchElementException.class, r::next);
    }

    @Test
    public void testProductWithTwo() {
        CollectionResults largest = createResults(2, "x");
        List<Solution> list1 = createSolutions(2, "y");
        List<Solution> list2 = createSolutions(2, "z");
        CartesianResults r = new CartesianResults(largest,
                                                  asList(wrapSameVars(list1), wrapSameVars(list2)),
                                                  Sets.newHashSet("x", "y", "z"));

        assertTrue(r.hasNext());

        List<Solution> list = new ArrayList<>();
        r.forEachRemaining(list::add);
        assertEquals(list, asList(
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
    }
}