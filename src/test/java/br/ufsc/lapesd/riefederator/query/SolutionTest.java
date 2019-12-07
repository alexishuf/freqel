package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.jena.query.JenaSolution;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
import org.apache.jena.query.QuerySolutionMap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.toJena;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public class SolutionTest {
    private static final @Nonnull List<NamedSupplier<Solution>> empty;
    private static final @Nonnull List<NamedSupplier<Solution>> nonEmpty;
    private static final URI ALICE = new StdURI("http://example.org/Alice");
    private static final URI BOB = new StdURI("http://example.org/Bob");

    static {
        empty = new ArrayList<>();
        empty.add(new NamedSupplier<>("MapSolution", MapSolution::new));
        empty.add(new NamedSupplier<>("JenaSolution", JenaSolution::new));

        nonEmpty = new ArrayList<>();
        nonEmpty.add(new NamedSupplier<>("MapSolution", () -> {
            MapSolution s = new MapSolution();
            s.getMap().put("x", ALICE);
            return s;
        }));
        nonEmpty.add(new NamedSupplier<>("MapSolution(from map)", () -> {
            HashMap<String, Term> map = new HashMap<>();
            map.put("x", ALICE);
            return new MapSolution(map);
        }));
        nonEmpty.add(new NamedSupplier<>("MapSolution(from builder)",
                () -> MapSolution.builder().put("x", ALICE).build()
        ));
        nonEmpty.add(new NamedSupplier<>("JenaSolution", () -> {
            QuerySolutionMap m = new QuerySolutionMap();
            m.add("x", toJena(ALICE));
            return new JenaSolution(m);
        }));
    }

    @DataProvider
    public static Object[][] emptyData() {
        return empty.stream().map(s -> singleton(s).toArray()).toArray(Object[][]::new);
    }

    @DataProvider
    public static Object[][] nonEmptyData() {
        return nonEmpty.stream().map(s -> singleton(s).toArray()).toArray(Object[][]::new);
    }

    @Test(dataProvider = "emptyData")
    public void testEmpty(Supplier<Solution> supplier) {
        Solution s = supplier.get();
        assertFalse(s.has("x"));
        assertFalse(s.has("missing"));
        assertEquals(s.get("x", ALICE), ALICE);
        assertEquals(s.get("missing", ALICE), ALICE);
        assertNull(s.get("x"));
        assertNull(s.get("missing"));
    }

    @Test(dataProvider = "nonEmptyData")
    public void testGet(Supplier<Solution> supplier) {
        Solution s = supplier.get();
        assertEquals(s.get("x"), ALICE);
        assertEquals(s.get("missing", BOB), BOB);
        assertNull(s.get("missing", null));
        assertNull(s.get("missing"));
    }

    @Test(dataProvider = "nonEmptyData")
    public void testHas(Supplier<Solution> supplier) {
        Solution s = supplier.get();
        assertTrue(s.has("x"));
        assertFalse(s.has("missing"));
        assertFalse(s.has(""));
    }

    @Test(dataProvider = "nonEmptyData")
    public void testEquals(Supplier<Solution> supplier) {
        MapSolution std = MapSolution.build("x", ALICE);
        assertEquals(supplier.get(), std);
    }

    @Test(dataProvider = "nonEmptyData")
    public void testNotEquals(Supplier<Solution> supplier) {
        MapSolution a = MapSolution.build("y", ALICE);
        MapSolution b = MapSolution.build("x", BOB);
        assertNotEquals(supplier.get(), a);
        assertNotEquals(supplier.get(), b);
    }

    @Test(dataProvider = "nonEmptyData")
    public void testNameIsCaseSensitive(Supplier<Solution> supplier) {
        MapSolution a = MapSolution.build("X", ALICE);
        assertNotEquals(supplier.get(), a);
    }

    @Test(dataProvider = "nonEmptyData")
    public void testMoreVarsNotEquals(Supplier<Solution> supplier) {
        MapSolution a = MapSolution.builder().put("x", ALICE).put("y", BOB).build();
        assertNotEquals(supplier.get(), a);
    }
}