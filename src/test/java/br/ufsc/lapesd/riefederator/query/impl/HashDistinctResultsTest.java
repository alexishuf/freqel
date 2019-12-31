package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class HashDistinctResultsTest {
    private static @Nonnull StdURI ex(int local) {
        return new StdURI("http://example.org/"+local);
    }

    @Test
    public void testEmpty() {
        CollectionResults in = new CollectionResults(Collections.emptyList(), singleton("x"));
        HashDistinctResults d = new HashDistinctResults(in);
        assertEquals(d.getVarNames(), singleton("x"));
        assertFalse(d.hasNext());
        assertEquals(d.getCardinality(), in.getCardinality());
    }

    @Test
    public void testNoDuplicates() {
        CollectionResults in = new CollectionResults(Arrays.asList(
                MapSolution.build("x", ex(1)),
                MapSolution.build("x", ex(2))), singleton("x"));
        HashDistinctResults d = new HashDistinctResults(in);
        assertEquals(d.getVarNames(), singleton("x"));

        assertTrue(d.hasNext());
        assertEquals(d.getCardinality(), Cardinality.exact(2));
        assertEquals(d.next(), MapSolution.build("x", ex(1)));

        assertTrue(d.hasNext());
        assertEquals(d.getCardinality(), Cardinality.exact(1));
        assertEquals(d.next(), MapSolution.build("x", ex(2)));

        assertFalse(d.hasNext());
        assertEquals(d.getCardinality(), Cardinality.EMPTY);
    }

    @Test
    public void testDuplicates() {
        CollectionResults in = new CollectionResults(Arrays.asList(
                MapSolution.builder().put("x", ex(1)).put("y", ex(2)).build(),
                MapSolution.builder().put("x", ex(2)).put("y", ex(1)).build(),
                MapSolution.builder().put("x", ex(1)).put("y", ex(2)).build(),
                MapSolution.builder().put("x", ex(3)).put("y", ex(1)).build()),
                newHashSet("x", "y"));
        HashDistinctResults d = new HashDistinctResults(in);
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
        assertEquals(d.getCardinality(), Cardinality.EMPTY);
    }
}