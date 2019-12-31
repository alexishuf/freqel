package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash;

import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;

import static java.util.Collections.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class CrudeSolutionHashTableTest {

    private static @Nonnull StdURI ex(int i) {
        return new StdURI("http://example.org/"+i);
    }

    @Test
    public void testEmpty() {
        CrudeSolutionHashTable table = new CrudeSolutionHashTable(singleton("x"), 0);
        assertEquals(table.getAll(MapSolution.build("x", ex(1))), emptyList());
        table.clear();
        assertEquals(table.getAll(MapSolution.build("x", ex(1))), emptyList());
    }

    @Test
    public void testAddToZeroExpected() {
        CrudeSolutionHashTable table = new CrudeSolutionHashTable(singleton("x"), 0);
        table.add(MapSolution.build("x", ex(1)));

        HashSet<MapSolution> expected = Sets.newHashSet(MapSolution.build("x", ex(1)));
        assertEquals(new HashSet<>(table.getAll(MapSolution.build("x", ex(1)))), expected);

        //check if hashing is horrible or decent
        int emptyCount = 0;
        for (int i = 2; i < 40; i++) {
            if (table.getAll(MapSolution.build("x", ex(i))).isEmpty())
                ++emptyCount;
        }
        assertTrue(emptyCount > 30, "emptyCount="+emptyCount);
    }

    @Test
    public void testAddUnderExpected() {
        CrudeSolutionHashTable table = new CrudeSolutionHashTable(singleton("x"), 8);
        table.add(MapSolution.build("x", ex(1)));

        HashSet<MapSolution> expected = Sets.newHashSet(MapSolution.build("x", ex(1)));
        assertEquals(new HashSet<>(table.getAll(MapSolution.build("x", ex(1)))), expected);
        assertEquals(new HashSet<>(table.getAll(MapSolution.build("x", ex(2)))), emptySet());
    }

    @Test
    public void testAddMoreThanExpected() {
        int expectedValues = 32 * 128;
        CrudeSolutionHashTable t = new CrudeSolutionHashTable(singleton("y"), expectedValues);

        for (int i = 0; i < expectedValues*2; i++)
            t.add(MapSolution.builder().put("x", ex(i)).put("y", ex(i + 1)).build());

        for (int i = 0; i < expectedValues*2; i++) {
            MapSolution solution = MapSolution.builder().put("x", ex(i))
                                                        .put("y", ex(i + 1)).build();
            Collection<Solution> all = t.getAll(solution);
            assertTrue(all.contains(solution), "i="+i);
        }
    }
}