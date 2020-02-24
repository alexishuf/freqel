package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.NamedFunction;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static br.ufsc.lapesd.riefederator.query.Capability.DISTINCT;
import static br.ufsc.lapesd.riefederator.query.Capability.PROJECTION;
import static com.google.common.collect.Sets.newHashSet;
import static java.net.InetAddress.getLocalHost;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.*;

public class TPEndpointTest extends EndpointTestBase {
    public static final @Nonnull List<NamedFunction<InputStream, Fixture<TPEndpoint>>> endpoints;

    static {
        endpoints = new ArrayList<>();
        endpoints.add(new NamedFunction<>("ARQEndpoint.forModel", stream -> {
            Model model = ModelFactory.createDefaultModel();
            RDFDataMgr.read(model, stream, "", Lang.TTL);
            return new Fixture<>(ARQEndpoint.forModel(model));
        }));
        endpoints.add(new NamedFunction<>("ARQEndpoint.forDataset", stream -> {
            Dataset ds = DatasetFactory.create();
            RDFDataMgr.read(ds, stream, "", Lang.TTL);
            return new Fixture<>(ARQEndpoint.forDataset(ds));
        }));
        endpoints.add(new NamedFunction<>("ARQEndpoint.forService", stream -> {
            Dataset ds = DatasetFactory.createTxnMem();
            RDFDataMgr.read(ds, stream, "", Lang.TTL);
            int port = 3331;
            try (ServerSocket serverSocket = new ServerSocket(0, 50, getLocalHost())) {
                port = serverSocket.getLocalPort();
            } catch (IOException ignored) { }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) { }
            FusekiServer server = FusekiServer.create().add("/ds", ds)
                    .loopback(true).port(port)
                    .build();
            server.start();
            String uri = "http://localhost:" + server.getPort() + "/ds/query";
            ARQEndpoint ep = ARQEndpoint.forService(uri);
            return new Fixture<TPEndpoint>(ep) {
                @Override
                public void close() {
                    server.stop();
                    server.join();
                }
            };
        }));
    }

    @DataProvider
    public Object[][] fixtureFactories() {
        return endpoints.stream().map(f -> new Object[]{f}).toArray(Object[][]::new);
    }

    protected void queryEmptyTest(@Nonnull Function<InputStream, Fixture<TPEndpoint>> f,
                                  @Nonnull Triple query) {
        Set<String> varNames = new HashSet<>();
        query.forEach(t -> {if (t.isVar()) varNames.add(t.asVar().getName());});

        String filename = "../empty.nt";
        try (Fixture<TPEndpoint> fixture = f.apply(getClass().getResourceAsStream(filename))) {
            try (Results results = fixture.endpoint.query(query)) {
                assertEquals(results.getVarNames(), varNames);
                assertFalse(results.hasNext());
                assertEquals(results.getCardinality(), Cardinality.EMPTY);
                assertEquals(results.getReadyCount(), 0);
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    protected void queryResourceTest(Function<InputStream, Fixture<TPEndpoint>> f,
                                     @Nonnull String filename,
                                     @Nonnull Triple query, @Nonnull Set<Solution> ex,
                                     Modifier... modifiers) {
        try (Fixture<TPEndpoint> fixture = f.apply(getClass().getResourceAsStream(filename))) {
            Set<Solution> ac = new HashSet<>();
            if (modifiers.length > 0) {
                CQuery cQuery = new CQuery(ImmutableList.of(query),
                                           ImmutableList.copyOf(modifiers));
                boolean repeated = false;
                try (Results results = fixture.endpoint.query(cQuery)) {
                    while (results.hasNext()) repeated |= !ac.add(results.next());
                }
                if (ModifierUtils.getFirst(DISTINCT, Arrays.asList(modifiers)) != null) {
                    if (fixture.endpoint.hasCapability(DISTINCT))
                        assertFalse(repeated);
                }
                if (!fixture.endpoint.hasCapability(PROJECTION))
                    return; // silently do not test result since it would fail
            } else {
                try (Results results = fixture.endpoint.query(query)) {
                    results.forEachRemaining(ac::add);
                }
            }
            assertEquals(ac.stream().filter(s -> !ex.contains(s)).collect(toList()), emptyList());
            assertEquals(ex.stream().filter(s -> !ac.contains(s)).collect(toList()), emptyList());
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testEmpty(@Nonnull Function<InputStream, Fixture<TPEndpoint>> f) {
        queryEmptyTest(f, new Triple(Alice, knows, x));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testAllOnEmpty(@Nonnull Function<InputStream, Fixture<TPEndpoint>> f) {
        queryEmptyTest(f, new Triple(s, p, o));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testAskOnEmpty(@Nonnull Function<InputStream, Fixture<TPEndpoint>> f) {
        queryEmptyTest(f, new Triple(Alice, knows, Bob));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testSingleObject(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(Alice, knows, x),
                singleton(MapSolution.build(x, Bob)));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testQueryTwoObjects(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(Bob, name, x),
                newHashSet(MapSolution.build(x, B_NAME1),
                           MapSolution.build(x, B_NAME2)));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testQueryTwoSubjects(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(x, type, Person),
                newHashSet(MapSolution.build(x, Alice),
                           MapSolution.build(x, Bob)));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testQueryObjectWithVarPredicate(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(Alice, p, o),
                newHashSet(MapSolution.builder().put(p, knows).put(o, Bob).build(),
                           MapSolution.builder().put(p, type).put(o, Person).build(),
                           MapSolution.builder().put(p, age).put(o, A_AGE).build(),
                           MapSolution.builder().put(p, name).put(o, A_NAME).build()));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testQuerySubjectFromLiteral(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(x, name, B_NAME1),
                singleton(MapSolution.build(x, Bob)));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testQuerySubjectObject(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(s, name, o),
                newHashSet(MapSolution.builder().put(s, Alice).put(o, A_NAME).build(),
                           MapSolution.builder().put(s, Bob).put(o, B_NAME1).build(),
                           MapSolution.builder().put(s, Bob).put(o, B_NAME2).build()));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testQueryDistinctPredicates(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(s, p, o),
                newHashSet(MapSolution.build(p, knows),
                        MapSolution.build(p, type),
                        MapSolution.build(p, age),
                        MapSolution.build(p, name)
                        ),
                Distinct.ADVISED, Projection.advised("p"));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testForceAskWithVars(Function<InputStream, Fixture<TPEndpoint>> f) {
        InputStream inputStream = getClass().getResourceAsStream("../rdf-1.nt");
        try (Fixture<TPEndpoint> fix = f.apply(inputStream)) {
            CQuery cQuery = CQuery.with(new Triple(s, p, o)).ask(true).build();
            try (Results results = fix.endpoint.query(cQuery)) {
                assertTrue(results.hasNext());
                assertFalse(results.next().has(p.getName()));
            }
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testForceAskWithVarsNegative(Function<InputStream, Fixture<TPEndpoint>> f) {
        InputStream inputStream = getClass().getResourceAsStream("../rdf-1.nt");
        try (Fixture<TPEndpoint> fix = f.apply(inputStream)) {
            CQuery cQuery = CQuery.with(new Triple(s, primaryTopic, o)).ask(true).build();
            try (Results results = fix.endpoint.query(cQuery)) {
                assertFalse(results.hasNext());
            }
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testAlternatives(Function<InputStream, Fixture<TPEndpoint>> f) {
        InputStream inputStream = getClass().getResourceAsStream("../rdf-1.nt");
        try (Fixture<TPEndpoint> fix = f.apply(inputStream)) {
            EmptyEndpoint a1 = new EmptyEndpoint(), a2 = new EmptyEndpoint();
            fix.endpoint.addAlternative(a1);
            assertEquals(fix.endpoint.getAlternatives(), singleton(a1));
            fix.endpoint.addAlternatives(Arrays.asList(a1, a2));
            assertEquals(fix.endpoint.getAlternatives(), Sets.newHashSet(a1, a2));
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testTransitiveAlternative(Function<InputStream, Fixture<TPEndpoint>> f) {
        InputStream inputStream = getClass().getResourceAsStream("../rdf-1.nt");
        try (Fixture<TPEndpoint> fix = f.apply(inputStream)) {
            EmptyEndpoint e1 = new EmptyEndpoint(), e2 = new EmptyEndpoint();
            fix.endpoint.addAlternative(e1);
            e1.addAlternative(e2);
            assertTrue(fix.endpoint.isAlternative(e1));
            assertTrue(fix.endpoint.isAlternative(e2));
            assertTrue(e1.isAlternative(fix.endpoint));
            assertTrue(e2.isAlternative(fix.endpoint));
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testReflexiveAlternatives(Function<InputStream, Fixture<TPEndpoint>> f) {
        InputStream inputStream = getClass().getResourceAsStream("../rdf-1.nt");
        try (Fixture<TPEndpoint> fix = f.apply(inputStream)) {
            EmptyEndpoint e = new EmptyEndpoint();
            fix.endpoint.addAlternative(e);
            e.addAlternative(fix.endpoint);

            assertTrue(fix.endpoint.isAlternative(e));
            assertTrue(e.isAlternative(fix.endpoint));
            assertTrue(fix.endpoint.getAlternatives().contains(e));
            assertTrue(e.getAlternatives().contains(fix.endpoint));
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testNotAlternative(Function<InputStream, Fixture<TPEndpoint>> f) {
        InputStream inputStream = getClass().getResourceAsStream("../rdf-1.nt");
        try (Fixture<TPEndpoint> fix = f.apply(inputStream)) {
            EmptyEndpoint e1 = new EmptyEndpoint(), e2 = new EmptyEndpoint();
            assertFalse(fix.endpoint.isAlternative(e1));
            assertFalse(fix.endpoint.isAlternative(e2));
            assertEquals(fix.endpoint.getAlternatives(), emptySet());

            fix.endpoint.addAlternative(e1);
            assertTrue(fix.endpoint.isAlternative(e1));
            assertFalse(fix.endpoint.isAlternative(e2));
            assertEquals(fix.endpoint.getAlternatives(), singleton(e1));
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testConcurrentAddAlternative(Function<InputStream, Fixture<TPEndpoint>> f)
    throws InterruptedException, ExecutionException {
        InputStream inputStream = getClass().getResourceAsStream("../rdf-1.nt");
        try (Fixture<TPEndpoint> fix = f.apply(inputStream)) {
            Map<TPEndpoint, Integer> observations = new HashMap<>();
            ExecutorService service = Executors.newCachedThreadPool();
            List<Future<?>> list = new ArrayList<>();
            list.add(service.submit(() -> {
                for (int i = 0; i < 4096; i++) {
                    for (TPEndpoint a : fix.endpoint.getAlternatives())
                        observations.put(a, observations.getOrDefault(a, 0) + 1);
                    assertTrue(fix.endpoint.getAlternatives().stream()
                            .allMatch(fix.endpoint::isAlternative));
                }
            }));
            for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
                list.add(service.submit(() -> {
                    for (int j = 0; j < 2048; j++) {
                        fix.endpoint.addAlternative(new EmptyEndpoint());
                    }
                }));
            }

            for (Future<?> future : list) future.get();
            service.shutdown();
            service.awaitTermination(1, TimeUnit.SECONDS);

            assertTrue(fix.endpoint.getAlternatives().stream()
                    .allMatch(fix.endpoint::isAlternative));
        }
    }
}