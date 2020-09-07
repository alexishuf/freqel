package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;

import static java.util.Collections.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class CrudeSolutionHashTableTest implements TestContext {

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

    @Test
    public void testRecordFetchesBeforeAdd() {
        CrudeSolutionHashTable table = new CrudeSolutionHashTable(singleton("x"), 4);
        table.recordFetches();
        for (int i = 0; i < 256; i++)
            table.add(MapSolution.builder().put(x, lit(i)).put(y, lit(0)).build());
        for (int i = 0; i < 256; i += 2) {
            Collection<Solution> actual = table.getAll(MapSolution.builder().put(x, lit(i)).build());
            Set<MapSolution> expected = singleton(
                    MapSolution.builder().put(x, lit(i)).put(y, lit(0)).build());
            assertEquals(actual, expected);
        }

        List<Solution> notFetched = new ArrayList<>();
        table.forEachNotFetched(notFetched::add);
        assertEquals(notFetched.size(), 128);
        Set<Solution> expected = new HashSet<>();
        for (int i = 1; i < 256; i += 2)
            expected.add(MapSolution.builder().put(x, lit(i)).put(y, lit(0)).build());
        assertEquals(new HashSet<>(notFetched), expected);

        for (int i = 0; i < 256; i++)
            table.add(MapSolution.builder().put(x, lit(i)).put(y, lit(1)).build());

        notFetched.clear();
        table.forEachNotFetched(notFetched::add);
        assertEquals(notFetched.size(), 128+256);
        for (int i = 0; i < 256; i++)
            expected.add(MapSolution.builder().put(x, lit(i)).put(y, lit(1)).build());
        assertEquals(new HashSet<>(notFetched), expected);

        for (int i = 0; i < 256; i += 2) {
            Collection<Solution> all = table.getAll(MapSolution.build(x, lit(i)));
            Set<MapSolution> ex = Sets.newHashSet(
                    MapSolution.builder().put(x, lit(i)).put(y, lit(0)).build(),
                    MapSolution.builder().put(x, lit(i)).put(y, lit(1)).build()
            );
            assertEquals(new HashSet<>(all), ex);
        }
        notFetched.clear();
        table.forEachNotFetched(notFetched::add);
        assertEquals(notFetched.size(), 256);
        expected.clear();
        for (int i = 1; i < 256; i += 2) {
            expected.add(MapSolution.builder().put(x, lit(i)).put(y, lit(0)).build());
            expected.add(MapSolution.builder().put(x, lit(i)).put(y, lit(1)).build());
        }
        assertEquals(new HashSet<>(notFetched), expected);
    }

    @Test
    public void testForEachNonFetchedSingleVar() {
        CrudeSolutionHashTable table = new CrudeSolutionHashTable(singleton("x"), 4);
        for (int i = 0; i < 4; i++)
            table.add(MapSolution.build(x, integer(i)));
        table.recordFetches();
        for (int i = 0; i < 4; i += 2) {
            Collection<Solution> all = table.getAll(MapSolution.build(x, integer(i)));
            assertEquals(all, singleton(MapSolution.build(x, integer(i))), "mismatch at i="+i);
        }
        List<Solution> nonFetched = new ArrayList<>();
        table.forEachNotFetched(nonFetched::add);
        assertEquals(nonFetched.size(), 2);

        Set<Solution> expected = new HashSet<>();
        for (int i = 1; i < 4; i += 2)
            expected.add(MapSolution.build(x, integer(i)));
        assertEquals(new HashSet<>(nonFetched), expected);
    }

    @Test
    public void testNonFetchedExtraVars() {
        CrudeSolutionHashTable table = new CrudeSolutionHashTable(singleton("x"), 256);
        for (int i = 0; i < 128; i++) {
            for (int j = 0; j < 4; j++)
                table.add(MapSolution.builder().put(x, integer(i)).put(y, integer(j)).build());
        }
        table.recordFetches();
        for (int i = 0; i < 128; i += 2) {
            MapSolution key = MapSolution.builder().put(x, integer(i)).put(z, integer(23)).build();
            Collection<Solution> actual = table.getAll(key);
            Set<Solution> expected = new HashSet<>();
            for (int j = 0; j < 4; j++)
                expected.add(MapSolution.builder().put(x, integer(i)).put(y, integer(j)).build());
            assertEquals(new HashSet<>(actual), expected);
        }

        List<Solution> nonFetched = new ArrayList<>();
        table.forEachNotFetched(nonFetched::add);
        Set<Solution> expected = new HashSet<>();
        for (int i = 1; i < 128; i += 2) {
            for (int j = 0; j < 4; j++)
                expected.add(MapSolution.builder().put(x, integer(i)).put(y, integer(j)).build());
        }
        assertEquals(nonFetched.size(), expected.size());
        assertEquals(new HashSet<>(nonFetched), expected);
    }

    @Test
    public void testNoVars() {
        CrudeSolutionHashTable table = new CrudeSolutionHashTable(emptySet(), 128);
        Set<MapSolution> expected = new HashSet<>();
        for (int i = 0; i < 1024; i++) {
            MapSolution solution = MapSolution.build(x, integer(i));
            table.add(solution);
            expected.add(solution);
        }

        assertEquals(new HashSet<>(table.getAll(MapSolution.build(y, integer(1)))), expected);

        // x is ignored, as it was not in table's varNames
        assertEquals(new HashSet<>(table.getAll(MapSolution.build(x, integer(1)))), expected);
    }

    @Test
    public void testNoVarsFetchAll() {
        CrudeSolutionHashTable table = new CrudeSolutionHashTable(emptySet(), 32);
        for (int i = 0; i < 4; i++)
            table.add(MapSolution.build(x, integer(i)));
        table.recordFetches();

        Collection<Solution> all = table.getAll(MapSolution.build(y, integer(1)));
        Set<MapSolution> expected = new HashSet<>();
        for (int i = 0; i < 4; i++)
            expected.add(MapSolution.build(x, integer(i)));
        assertEquals(new HashSet<>(all), expected);

        List<Solution> nonFetched = new ArrayList<>();
        table.forEachNotFetched(nonFetched::add);
        assertEquals(nonFetched, emptyList());

        table.add(MapSolution.build(x, integer(4)));
        table.forEachNotFetched(nonFetched::add);
        assertEquals(nonFetched, singletonList(MapSolution.build(x, integer(4))));

        all = table.getAll(MapSolution.build(x, integer(23)));
        expected.clear();
        for (int i = 0; i < 5; i++)
            expected.add(MapSolution.build(x, integer(i)));
        assertEquals(new HashSet<>(all), expected);

        nonFetched.clear();
        table.forEachNotFetched(nonFetched::add);
        assertEquals(nonFetched, emptyList());
    }
}