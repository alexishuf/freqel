package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

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
}