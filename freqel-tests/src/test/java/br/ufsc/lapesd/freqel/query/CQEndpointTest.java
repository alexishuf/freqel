package br.ufsc.lapesd.freqel.query;

import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterFactory;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.util.NamedFunction;
import com.google.common.collect.Sets;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.*;

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
        queryResourceTest(f, query, ex, false);
    }

    @SuppressWarnings("SameParameterValue")
    protected void queryResourceTest(Function<InputStream, Fixture<CQEndpoint>> f,
                                     @Nonnull Collection<Triple> query,
                                     @Nonnull Set<Solution> ex, boolean poll) {
        String filename = "rdf-2.nt";
        try (Fixture<CQEndpoint> fixture = f.apply(open(filename))) {
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
    public void testTPSelectPoll(Function<InputStream, Fixture<CQEndpoint>> f) {
        queryResourceTest(f, singletonList(new Triple(s, knows, Bob)),
                newHashSet(MapSolution.build(s, Alice),
                           MapSolution.build(s, Dave)), true);
    }

    @Test(dataProvider = "fixtureFactories")
    public void testConjunctiveSelect(Function<InputStream, Fixture<CQEndpoint>> f) {
        List<Triple> query = asList(new Triple(s, knows, Bob),
                                    new Triple(s, age, A_AGE));
        queryResourceTest(f, query, singleton(MapSolution.build(s, Alice)));
    }


    @Test(dataProvider = "fixtureFactories")
    public void testConjunctiveSelectPoll(Function<InputStream, Fixture<CQEndpoint>> f) {
        List<Triple> query = asList(new Triple(s, knows, Bob),
                new Triple(s, age, A_AGE));
        queryResourceTest(f, query, singleton(MapSolution.build(s, Alice)), true);
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
        try (Fixture<CQEndpoint> f = fac.apply(open("rdf-2.nt"))) {
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
            assertTrue(exec.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testParallelQueryingPoll(Function<InputStream, Fixture<CQEndpoint>> fac)
            throws InterruptedException, ExecutionException {
        ExecutorService exec = Executors.newCachedThreadPool();
        List<Future<?>> futures = new ArrayList<>();
        try (Fixture<CQEndpoint> f = fac.apply(open("rdf-2.nt"))) {
            CQEndpoint ep = f.endpoint;
            for (int i = 0; i < 50; i++) {
                futures.add(exec.submit(() -> {
                    for (int j = 0; j < 10; j++) {
                        Set<Solution> ac = new HashSet<>();
                        ep.query(createQuery(s, knows, Bob, s, age, A_AGE))
                                .forEachRemainingThenClose(ac::add);
                        assertEquals(ac, singleton(MapSolution.build(s, Alice)));

                        ac.clear();
                        Results results = ep.query(createQuery(Charlie, type, Person,
                                Charlie, age, A_AGE,
                                Alice, knows, Bob));
                        while (results.hasNext(0) || results.hasNext())
                            ac.add(results.next());
                        results.close();
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
            assertTrue(exec.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testConjunctiveSingleVarFilter(Function<InputStream, Fixture<CQEndpoint>> f) {
        CQuery query = createQuery(x, knows, Bob,
                                   x, age,   y,
                                   SPARQLFilterFactory.parseFilter("?y > 20"));
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
        try (Fixture<CQEndpoint> fixture = f.apply(open("rdf-2.nt"))) {
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
        try (Fixture<CQEndpoint> fixture = f.apply(open("rdf-2.nt"))) {
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
                                   SPARQLFilterFactory.parseFilter("?u > ?v"));
        queryResourceTest(f, query,
                          singleton(MapSolution.builder().put(x, Dave).put(y, Alice)
                                                         .put(u, lit(25))
                                                         .put(v, lit(23)).build()));
        queryResourceTest(f, query,
                          singleton(MapSolution.builder().put(x, Dave).put(y, Alice)
                                                         .put(u, lit(25))
                                                         .put(v, lit(23)).build()), true);

        query = createQuery(x, knows, Bob,
                            x, age,   u,
                            y, knows, Bob,
                            y, age,   v,
                            SPARQLFilterFactory.parseFilter("?v > ?u"));
        queryResourceTest(f, query,
                          singleton(MapSolution.builder().put(x, Alice).put(y, Dave)
                                                         .put(u, lit(23))
                                                         .put(v, lit(25)).build()));
    }
}