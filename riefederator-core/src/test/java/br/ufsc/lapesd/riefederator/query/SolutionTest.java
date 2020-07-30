package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.jena.query.JenaBindingSolution;
import br.ufsc.lapesd.riefederator.jena.query.JenaSolution;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.ArraySolution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.util.ArraySet;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.QuerySolutionMap;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.*;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class SolutionTest implements TestContext {
    private static final @Nonnull List<NamedSupplier<Solution>> empty;
    private static final @Nonnull List<NamedSupplier<Solution>> nonEmpty;
    private static final @Nonnull List<Function<MapSolution, Solution>> converters;

    static {
        empty = new ArrayList<>();
        empty.add(new NamedSupplier<>("MapSolution", MapSolution::new));
        empty.add(new NamedSupplier<>("JenaSolution", JenaSolution::new));
        empty.add(new NamedSupplier<>("ArraySolution", () -> ArraySolution.EMPTY));
        empty.add(new NamedSupplier<>("JenaBindingSolution", () -> JenaBindingSolution.EMPTY));

        nonEmpty = new ArrayList<>();
        nonEmpty.add(new NamedSupplier<>("MapSolution", () -> {
            MapSolution s = new MapSolution();
            s.getMap().put("x", Alice);
            return s;
        }));
        nonEmpty.add(new NamedSupplier<>("MapSolution(from map)", () -> {
            HashMap<String, Term> map = new HashMap<>();
            map.put("x", Alice);
            return new MapSolution(map);
        }));
        nonEmpty.add(new NamedSupplier<>("MapSolution(from builder)",
                () -> MapSolution.builder().put("x", Alice).build()
        ));
        nonEmpty.add(new NamedSupplier<>("JenaSolution", () -> {
            QuerySolutionMap m = new QuerySolutionMap();
            m.add("x", toJena(Alice));
            return new JenaSolution(m);
        }));
        nonEmpty.add(new NamedSupplier<>("ArraySolution(from collection)",
                () -> ArraySolution.forVars(singletonList("x"))
                                   .fromValues(singletonList(Alice))));
        nonEmpty.add(new NamedSupplier<>("JenaBindingSolution", () -> {
            BindingHashMap bhm = new BindingHashMap();
            bhm.add(Var.alloc("x"), NodeFactory.createURI(Alice.getURI()));
            return new JenaBindingSolution(bhm, Collections.singleton("x"));
        }));
        Map<String, Term> x2Alice = new HashMap<>();
        x2Alice.put("x", Alice);
        x2Alice.put("y", Bob);
        nonEmpty.add(new NamedSupplier<>("ArraySolution(from function)",
                () -> ArraySolution.forVars(singletonList("x"))
                        .fromFunction(x2Alice::get)));

        converters = new ArrayList<>();
        converters.add(s -> {
            QuerySolutionMap m = new QuerySolutionMap();
            s.forEach((n, t) -> m.add(n, toJena(t)));
            return new JenaSolution(m);
        });
        converters.add(s -> ArraySolution.forVars(s.getVarNames()).fromFunction(s::get));
        converters.add(s -> {
            BindingHashMap map = new BindingHashMap();
            s.forEach((name, term) -> map.add(Var.alloc(name), toJenaNode(term)));
            return JenaBindingSolution.forVars(s.getVarNames()).apply(map);
        });
    }

    @DataProvider
    public static @Nonnull Object[][] emptyData() {
        return empty.stream().map(s -> singleton(s).toArray()).toArray(Object[][]::new);
    }

    @DataProvider
    public static @Nonnull Object[][] nonEmptyData() {
        return nonEmpty.stream().map(s -> singleton(s).toArray()).toArray(Object[][]::new);
    }

    @DataProvider
    public static @Nonnull Object[][] converterData() {
        return converters.stream().map(f -> new Object[]{f}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "emptyData")
    public void testEmpty(Supplier<Solution> supplier) {
        Solution s = supplier.get();
        assertTrue(s.isEmpty());
        assertFalse(s.has("x"));
        assertFalse(s.has("missing"));
        assertEquals(s.get("x", Alice), Alice);
        assertEquals(s.get("missing", Alice), Alice);
        assertNull(s.get("x"));
        assertNull(s.get("missing"));
    }

    @Test(dataProvider = "nonEmptyData")
    public void testNonEmpty(Supplier<Solution> supplier) {
        Solution s = supplier.get();
        assertFalse(s.isEmpty());
    }

    @Test(dataProvider = "nonEmptyData")
    public void testGet(Supplier<Solution> supplier) {
        Solution s = supplier.get();
        assertEquals(s.get("x"), Alice);
        assertEquals(s.get("missing", Bob), Bob);
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
        MapSolution std = MapSolution.build("x", Alice);
        assertEquals(supplier.get(), std);
    }

    @Test(dataProvider = "converterData")
    public void testEqualsRegression1(Function<MapSolution, Solution> converter) {
        MapSolution solution = MapSolution.builder()
                .put("director", new StdURI("http://dbpedia.org/resource/Gabriele_Muccino"))
                .put("genre", new StdURI("http://data.linkedmdb.org/resource/film_genre/4"))
                .put("film", new StdURI("http://dbpedia.org/resource/Remember_Me%2C_My_Love"))
                .build();
        Solution converted = converter.apply(solution);
        assertEquals(converted, solution);
    }

    @Test
    public void testEqualsRegression2() {
        Solution s = MapSolution.builder()
                .put("director", new StdURI("http://dbpedia.org/resource/Gabriele_Muccino"))
                .put("genre", new StdURI("http://data.linkedmdb.org/resource/film_genre/4"))
                .put("film", new StdURI("http://dbpedia.org/resource/Remember_Me%2C_My_Love"))
                .build();
        QuerySolutionMap qsm = new QuerySolutionMap();
        qsm.add("director", ResourceFactory.createResource("http://dbpedia.org/resource/Gabriele_Muccino"));
        qsm.add("genre", ResourceFactory.createResource("http://data.linkedmdb.org/resource/film_genre/4"));
        qsm.add("film", ResourceFactory.createResource("http://dbpedia.org/resource/Remember_Me%2C_My_Love"));
        Solution js = new JenaSolution(qsm);

        ArraySolution as1 = ArraySolution.forVars(ArraySet.fromDistinct(s.getVarNames()))
                                         .fromFunction(s::get);
        List<String> shuffled = new ArrayList<>(s.getVarNames());
        Collections.shuffle(shuffled);
        ArraySolution as2 = ArraySolution.forVars(ArraySet.fromDistinct(shuffled))
                                         .fromFunction(s::get);
        ArraySolution as3 = ArraySolution.forVars(ImmutableSet.copyOf(shuffled))
                                         .fromFunction(s::get);
        ArraySolution as4 = ArraySolution.forVars(ImmutableSet.copyOf(new HashSet<>(shuffled)))
                                         .fromFunction(s::get);

        assertEquals(as1, s);
        assertEquals(as2, s);
        assertEquals(as3, s);
        assertEquals(as4, s);

        assertEquals(as1, js);
        assertEquals(as2, js);
        assertEquals(as3, js);
        assertEquals(as4, js);
    }

    @Test(dataProvider = "converterData")
    public void testEqualsRegression3(Function<MapSolution, Solution> converter) {
        MapSolution s1 = MapSolution.builder()
                .put("director", new StdURI("http://dbpedia.org/resource/Gabriele_Muccino"))
                .put("genre", new StdURI("http://data.linkedmdb.org/resource/film_genre/4"))
                .put("film", new StdURI("http://dbpedia.org/resource/Remember_Me%2C_My_Love"))
                .build();
        MapSolution s2 = MapSolution.builder()
                .put("director", new StdURI("http://dbpedia.org/resource/Gabriele_Muccino"))
                .put("film", new StdURI("http://dbpedia.org/resource/L%27ultimo_bacio,"))
                .put("genre", new StdURI("http://data.linkedmdb.org/resource/film_genre/4"))
                .build();

        HashSet<Solution> expected = Sets.newHashSet(s1, s2);
        assertEquals(converter.apply(s1), s1);
        assertEquals(converter.apply(s2), s2);
        assertEquals(Sets.newHashSet(converter.apply(s1), converter.apply(s2)), expected);
        assertEquals(Sets.newHashSet(converter.apply(s2), converter.apply(s1)), expected);
    }

    @Test
    public void testEqualsRegression4() {
        Solution s1 = MapSolution.builder()
                .put("director", new StdURI("http://dbpedia.org/resource/Gabriele_Muccino"))
                .put("genre", new StdURI("http://data.linkedmdb.org/resource/film_genre/4"))
                .put("film", new StdURI("http://dbpedia.org/resource/Remember_Me%2C_My_Love"))
                .build();
        Solution s2 = MapSolution.builder()
                .put("director", new StdURI("http://dbpedia.org/resource/Gabriele_Muccino"))
                .put("film", new StdURI("http://dbpedia.org/resource/L%27ultimo_bacio,"))
                .put("genre", new StdURI("http://data.linkedmdb.org/resource/film_genre/4"))
                .build();

        //noinspection UnstableApiUsage
        for (List<String> perm : Collections2.permutations(s1.getVarNames())) {
            Set<Solution> as1 = Sets.newHashSet(
                    ArraySolution.forVars(perm).fromFunction(s1::get),
                    ArraySolution.forVars(perm).fromFunction(s2::get)
            );
            Set<Solution> as2 = Sets.newHashSet(
                    ArraySolution.forVars(perm).fromFunction(n -> fromJena(toJena(s1.get(n)))),
                    ArraySolution.forVars(perm).fromFunction(n -> fromJena(toJena(s2.get(n))))
            );

            QuerySolutionMap qsm1 = new QuerySolutionMap(), qsm2 = new QuerySolutionMap();
            perm.forEach(n -> qsm1.add(n, toJena(s1.get(n))));
            perm.forEach(n -> qsm2.add(n, toJena(s2.get(n))));
            Set<Solution> js1 = Sets.newHashSet(new JenaSolution(qsm1), new JenaSolution(qsm2));

            JenaSolution.Factory jsFac = new JenaSolution.Factory(perm);
            Set<Solution> js2 = Sets.newHashSet(jsFac.transform(qsm1), jsFac.transform(qsm2));

            HashSet<Solution> expected = Sets.newHashSet(s1, s2);
            assertEquals(as1, expected);
            assertEquals(as2, expected);
            assertEquals(js1, expected);
            assertEquals(js2, expected);
        }
    }

    @Test(dataProvider = "converterData")
    public void testHashSet(Function<MapSolution, Solution> converter) {
        HashSet<Solution> actual = new HashSet<>(), expected = new HashSet<>();
        for (int i = 0; i < 8; i++) {
            MapSolution solution = MapSolution.builder()
                    .put(x, Alice)
                    .put(y, lit(i)).build();
            expected.add(solution);
            actual.add(converter.apply(solution));
        }
        assertEquals(actual, expected);

        for (int i = 0; i < 1024; i++) {
            MapSolution solution = MapSolution.builder().put(x, Alice).put(y, lit(i)).build();
            expected.add(solution);
            actual.add(converter.apply(solution));
        }
        assertEquals(actual, expected);
    }

    @Test(dataProvider = "nonEmptyData")
    public void testNotEquals(Supplier<Solution> supplier) {
        MapSolution a = MapSolution.build("y", Alice);
        MapSolution b = MapSolution.build("x", Bob);
        assertNotEquals(supplier.get(), a);
        assertNotEquals(supplier.get(), b);
    }

    @Test(dataProvider = "nonEmptyData")
    public void testNameIsCaseSensitive(Supplier<Solution> supplier) {
        MapSolution a = MapSolution.build("X", Alice);
        assertNotEquals(supplier.get(), a);
    }

    @Test(dataProvider = "nonEmptyData")
    public void testMoreVarsNotEquals(Supplier<Solution> supplier) {
        MapSolution a = MapSolution.builder().put("x", Alice).put("y", Bob).build();
        assertNotEquals(supplier.get(), a);
    }
}