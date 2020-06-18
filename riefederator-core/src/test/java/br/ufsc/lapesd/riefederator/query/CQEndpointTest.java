package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.NamedFunction;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import com.google.common.collect.Sets;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class CQEndpointTest extends EndpointTestBase {
    private static final @Nonnull
    List<NamedFunction<InputStream, Fixture<CQEndpoint>>> endpoints = new ArrayList<>();

    static {
        for (NamedFunction<InputStream, Fixture<TPEndpoint>> f : TPEndpointTest.endpoints) {
            if (f.toString().startsWith("ARQEndpoint") || f.toString().contains("SPARQL")) {
                //noinspection unchecked,rawtypes
                endpoints.add((NamedFunction) f);
            }
        }
    }

    @DataProvider
    public Object[][] fixtureFactories() {
        return endpoints.stream().map(f -> new Object[]{f}).toArray(Object[][]::new);
    }

    @SuppressWarnings("SameParameterValue")
    protected void queryResourceTest(Function<InputStream, Fixture<CQEndpoint>> f,
                                     @Nonnull Collection<Triple> query,
                                     @Nonnull Set<Solution> ex) {
        String filename = "../rdf-2.nt";
        try (Fixture<CQEndpoint> fixture = f.apply(getClass().getResourceAsStream(filename))) {
            Set<Solution> ac = new HashSet<>();
            CQuery cQuery = query instanceof CQuery ? (CQuery) query : CQuery.from(query);
            try (Results results = fixture.endpoint.query(cQuery)) {
                results.forEachRemaining(ac::add);
            }
            assertEquals(ac.stream().filter(s -> !ex.contains(s)).collect(toList()), emptyList());
            assertEquals(ex.stream().filter(s -> !ac.contains(s)).collect(toList()), emptyList());
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testTPSelect(Function<InputStream, Fixture<CQEndpoint>> f) {
        queryResourceTest(f, singletonList(new Triple(s, knows, Bob)),
                newHashSet(MapSolution.build(s, Alice),
                           MapSolution.build(s, Dave)));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testConjunctiveSelect(Function<InputStream, Fixture<CQEndpoint>> f) {
        List<Triple> query = asList(new Triple(s, knows, Bob),
                                    new Triple(s, age, A_AGE));
        queryResourceTest(f, query, singleton(MapSolution.build(s, Alice)));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testConjunctiveAsk(Function<InputStream, Fixture<CQEndpoint>> f) {
        List<Triple> query = asList(new Triple(Charlie, type, Person),
                                    new Triple(Charlie, age, A_AGE),
                                    new Triple(Alice, knows, Bob));
        queryResourceTest(f, query, singleton(MapSolution.EMPTY));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testNegativeConjunctiveAsk(Function<InputStream, Fixture<CQEndpoint>> f) {
        List<Triple> query = asList(new Triple(Charlie, type, Person), //ok
                                    new Triple(Charlie, age, A_AGE),   //ok
                                    new Triple(Alice, knows, Dave));   //wrong
        queryResourceTest(f, query, emptySet());
    }

    @Test(dataProvider = "fixtureFactories")
    public void testParallelQuerying(Function<InputStream, Fixture<CQEndpoint>> fac)
            throws InterruptedException, ExecutionException {
        ExecutorService exec = Executors.newCachedThreadPool();
        List<Future<?>> futures = new ArrayList<>();
        try (Fixture<CQEndpoint> f = fac.apply(getClass().getResourceAsStream("../rdf-2.nt"))) {
            CQEndpoint ep = f.endpoint;
            for (int i = 0; i < 50; i++) {
                futures.add(exec.submit(() -> {
                    for (int j = 0; j < 10; j++) {
                        Set<Solution> ac = new HashSet<>();
                        ep.query(createQuery(s, knows, Bob, s, age, A_AGE))
                                .forEachRemainingThenClose(ac::add);
                        assertEquals(ac, singleton(MapSolution.build(s, Alice)));

                        ac.clear();
                        ep.query(createQuery(Charlie, type, Person,
                                             Charlie, age, A_AGE,
                                             Alice, knows, Bob))
                                .forEachRemainingThenClose(ac::add);
                        assertEquals(ac, singleton(MapSolution.EMPTY));

                        ac.clear();
                        ep.query(createQuery(s, knows, Bob)).forEachRemainingThenClose(ac::add);
                        assertEquals(ac, Sets.newHashSet(MapSolution.build(s, Alice),
                                MapSolution.build(s, Dave)));
                    }
                }));
            }
            for (Future<?> future : futures)
                assertNull(future.get()); // re-throws AssertionErrors in ExecutionExceptions
        } finally {
            exec.shutdown();
            exec.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testConjunctiveSingleVarFilter(Function<InputStream, Fixture<CQEndpoint>> f) {
        CQuery query = createQuery(x, knows, Bob,
                                   x, age,   y,
                                   SPARQLFilter.build("?y > 20"));
        queryResourceTest(f, query,
                newHashSet(MapSolution.builder().put(x, Alice).put(y, lit(23)).build(),
                           MapSolution.builder().put(x, Dave).put(y, lit(25)).build()));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testRawSPARQLConjunctive(Function<InputStream, Fixture<CQEndpoint>> f) {
        String sparql = "PREFIX foaf: <"+ FOAF.NS +">\n" +
                "PREFIX ex: <"+EX+">\n" +
                "SELECT * WHERE {\n" +
                "  ?x foaf:knows ex:Bob ;" +
                "     foaf:age ?age FILTER(?age <= 23) .\n" +
                "}";
        try (Fixture<CQEndpoint> fixture = f.apply(getClass().getResourceAsStream("../rdf-2.nt"))) {
            if (!fixture.endpoint.canQuerySPARQL())
                return; //not a test target
            Set<Term> actual = new HashSet<>();
            Results results = fixture.endpoint.querySPARQL(sparql);
            results.forEachRemainingThenClose(s -> actual.add(s.get(x)));
            assertEquals(actual, Collections.singleton(Alice));
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testRawSPARQLUNION(Function<InputStream, Fixture<CQEndpoint>> f) {
        String sparql = "PREFIX ex: <" + EX + ">\n" +
                "PREFIX foaf: <"+ FOAF.NS +">\n" +
                "SELECT * WHERE {\n" +
                "  {\n" +
                "    ?x foaf:knows ex:Bob ;" +
                "       foaf:age ?age FILTER(?age <= 23) .\n" +
                "  } UNION {\n" +
                "    ?x foaf:name \"bob\"@en .\n" +
                "  }\n" +
                "}";
        try (Fixture<CQEndpoint> fixture = f.apply(getClass().getResourceAsStream("../rdf-2.nt"))) {
            if (!fixture.endpoint.canQuerySPARQL())
                return; //not a test target
            Set<Term> actual = new HashSet<>();
            Results results = fixture.endpoint.querySPARQL(sparql);
            results.forEachRemainingThenClose(s -> actual.add(s.get(x)));
            assertEquals(actual, Sets.newHashSet(Alice, Bob));
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testConjunctiveTwoVarsFilter(Function<InputStream, Fixture<CQEndpoint>> f) {
        CQuery query = createQuery(x, knows, Bob,
                                   x, age,   u,
                                   y, knows, Bob,
                                   y, age,   v,
                                   SPARQLFilter.build("?u > ?v"));
        queryResourceTest(f, query,
                          singleton(MapSolution.builder().put(x, Dave).put(y, Alice)
                                                         .put(u, lit(25))
                                                         .put(v, lit(23)).build()));

        query = createQuery(x, knows, Bob,
                            x, age,   u,
                            y, knows, Bob,
                            y, age,   v,
                            SPARQLFilter.builder("?v > ?u").map(u).map(v).build());
        queryResourceTest(f, query,
                          singleton(MapSolution.builder().put(x, Alice).put(y, Dave)
                                                         .put(u, lit(23))
                                                         .put(v, lit(25)).build()));
    }
}