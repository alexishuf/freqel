package br.ufsc.lapesd.freqel.query;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.hdt.query.HDTEndpoint;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.impl.CompliantTSVSPARQLClient;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.impl.NettyCompliantTSVSPARQLClient;
import br.ufsc.lapesd.freqel.query.endpoint.impl.SPARQLClient;
import br.ufsc.lapesd.freqel.query.modifiers.*;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterFactory;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.util.NamedFunction;
import com.github.lapesd.rdfit.RIt;
import com.github.lapesd.rdfit.components.hdt.HDTHelpers;
import com.google.common.collect.Sets;
import org.apache.jena.fuseki.FusekiException;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.TripleString;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static br.ufsc.lapesd.freqel.query.endpoint.Capability.*;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.net.InetAddress.getLocalHost;
import static java.util.Collections.*;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.*;

public class TPEndpointTest extends EndpointTestBase {
    public static final @Nonnull List<NamedFunction<InputStream, Fixture<TPEndpoint>>> endpoints;

    public static class FusekiEndpoint implements AutoCloseable {
        public @Nonnull FusekiServer server;
        public @Nonnull ARQEndpoint ep;
        public @Nonnull String uri;

        public FusekiEndpoint(@Nonnull Dataset ds) {
            FusekiServer server = null;
            while (server == null) {
                int port = 3131;
                try (ServerSocket s = new ServerSocket(0, 50, getLocalHost())) {
                    port = s.getLocalPort();
                } catch (IOException ignored) { }
                for (int i = 0; server == null && i < 6 ; i++) {
                    try {
                        server = FusekiServer.create().add("/ds", ds)
                                             .loopback(true).port(port).build().start();
                    } catch (FusekiException e) {
                        if (!(e.getCause() instanceof BindException))
                            throw e;
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException ignored) { }
                    }
                }
            }
            this.server = server;
            this.uri = "http://localhost:" + server.getPort() + "/ds/query";
            this.ep = ARQEndpoint.forService(uri);
        }

        @Override
        public void close() {
            server.stop();
            server.join();
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) { }
        }
    }

    static {
        endpoints = new ArrayList<>();
        endpoints.add(new NamedFunction<>("ARQEndpoint.forModel", stream -> {
            assertNotNull(stream);
            Model model = ModelFactory.createDefaultModel();
            RDFDataMgr.read(model, stream, "", Lang.TTL);
            return new Fixture<>(ARQEndpoint.forModel(model));
        }));
        endpoints.add(new NamedFunction<>("ARQEndpoint.forDataset", stream -> {
            assertNotNull(stream);
            Dataset ds = DatasetFactory.create();
            RDFDataMgr.read(ds, stream, "", Lang.TTL);
            return new Fixture<>(ARQEndpoint.forDataset(ds));
        }));
        endpoints.add(new NamedFunction<>("ARQEndpoint.forService", stream -> {
            assertNotNull(stream);
            Dataset ds = DatasetFactory.createTxnMem();
            RDFDataMgr.read(ds, stream, "", Lang.TTL);
            FusekiEndpoint fusekiEndpoint = new FusekiEndpoint(ds);
            return new Fixture<TPEndpoint>(fusekiEndpoint.ep) {
                @Override
                public void close() {
                    fusekiEndpoint.close();
                }
            };
        }));
        endpoints.add(new NamedFunction<>("SPARQLClient+Fuseki", stream -> {
            assertNotNull(stream);
            Dataset ds = DatasetFactory.createTxnMem();
            RDFDataMgr.read(ds, stream, "", Lang.TTL);
            FusekiEndpoint fusekiEndpoint = new FusekiEndpoint(ds);
            SPARQLClient client = new SPARQLClient(fusekiEndpoint.uri);
            return new Fixture<TPEndpoint>(client) {
                @Override
                public void close() {
                    fusekiEndpoint.close();
                    client.close();
                }
            };
        }));
        endpoints.add(new NamedFunction<>("HDTEndpoint[in-memory]", stream -> {
            HDT hdt;
            hdt = HDTHelpers.toHDT(RIt.iterateTriples(TripleString.class, stream));
            HDTEndpoint ep = new HDTEndpoint(hdt, stream.toString());
            return new Fixture<TPEndpoint>(ep) {
                @Override public void close() {
                    ep.close();
                }
            };
        }));
        for (Boolean index : Arrays.asList(true, false)) {
            endpoints.add(new NamedFunction<>(
                    "HDTEndpoint["+(index ? "indexed,":"")+", mapped]", stream -> {
                File file;
                HDT hdt;
                try {
                    file = Files.createTempFile("freqel", ".hdt").toFile();
                    HDTHelpers.toHDTFile(file, stream);
                    if (index)
                        hdt = HDTManager.mapIndexedHDT(file.getAbsolutePath());
                    else
                        hdt = HDTManager.mapHDT(file.getAbsolutePath());
                } catch (IOException e) {
                    throw new AssertionError("Unexpected IOException", e);
                }
                HDTEndpoint ep = new HDTEndpoint(hdt, stream.toString());
                return new Fixture<TPEndpoint>(ep) {
                    @Override public void close() {
                        ep.close();
                        assertTrue(HDTHelpers.deleteWithIndex(file));
                    }
                };
            }));
        }
        endpoints.add(new NamedFunction<>("HDTSS+CompliantTSVSPARQLClient", stream -> {
            HDTSSProcess server = HDTSSProcess.forRDF(stream, Lang.TTL);
            CompliantTSVSPARQLClient client = new CompliantTSVSPARQLClient(server.getEndpoint());
            return new Fixture<TPEndpoint>(client) {
                @Override public void close() {
                    server.close();
                    client.close();
                }
            };
        }));
        endpoints.add(new NamedFunction<>("HDTSS+NettyCompliantTSVSPARQLClient", stream -> {
            HDTSSProcess server = HDTSSProcess.forRDF(stream, Lang.TTL);
            NettyCompliantTSVSPARQLClient client = new NettyCompliantTSVSPARQLClient(server.getEndpoint());
            return new Fixture<TPEndpoint>(client) {
                @Override public void close() {
                    server.close();
                    client.close();
                }
            };
        }));
        endpoints.add(new NamedFunction<>("HDTSS + POSTing NettyCompliantTSVSPARQLClient", stream -> {
            HDTSSProcess server = HDTSSProcess.forRDF(stream, Lang.TTL);
            NettyCompliantTSVSPARQLClient client = new NettyCompliantTSVSPARQLClient(server.getEndpoint()).usePOST();
            return new Fixture<TPEndpoint>(client) {
                @Override public void close() {
                    server.close();
                    client.close();
                }
            };
        }));
        endpoints.add(new NamedFunction<>("HDTSS + POSTing & 1-queue NettyCompliantTSVSPARQLClient", stream -> {
            HDTSSProcess server = HDTSSProcess.forRDF(stream, Lang.TTL);
            NettyCompliantTSVSPARQLClient client = new NettyCompliantTSVSPARQLClient(server.getEndpoint()).usePOST();
            client.setQueueCapacity(1);
            return new Fixture<TPEndpoint>(client) {
                @Override public void close() {
                    server.close();
                    client.close();
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

        String filename = "empty.nt";
        try (Fixture<TPEndpoint> fixture = f.apply(open(filename))) {
            try (Results results = fixture.endpoint.query(query)) {
                assertEquals(results.getVarNames(), varNames);
                assertFalse(results.hasNext());
                assertEquals(results.getReadyCount(), 0);
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    protected void queryResourceTest(Function<InputStream, Fixture<TPEndpoint>> f,
                                     @Nonnull String filename,
                                     @Nonnull Triple query, @Nonnull Set<Solution> ex,
                                     Modifier... modifiers) {
        queryResourceTest(f, filename, query, ex, false, modifiers);
    }

    @SuppressWarnings("SameParameterValue")
    protected void queryResourceTest(Function<InputStream, Fixture<TPEndpoint>> f,
                                     @Nonnull String filename,
                                     @Nonnull Triple query, @Nonnull Set<Solution> ex,
                                     boolean poll,
                                     Modifier... modifiers) {
        try (Fixture<TPEndpoint> fixture = f.apply(open(filename))) {
            Set<Solution> ac = new HashSet<>();
            if (modifiers.length > 0) {
                MutableCQuery cQuery = MutableCQuery.from(query);
                cQuery.mutateModifiers().addAll(Arrays.asList(modifiers));
                boolean repeated = false;
                if (poll) {
                    try (Results results = fixture.endpoint.query(cQuery)) {
                        while (results.hasNext(0) || results.hasNext())
                            repeated |= !ac.add(results.next());
                    }
                } else {
                    try (Results results = fixture.endpoint.query(cQuery)) {
                        while (results.hasNext()) repeated |= !ac.add(results.next());
                    }
                }
                if (ModifierUtils.getFirst(DISTINCT, Arrays.asList(modifiers)) != null) {
                    if (fixture.endpoint.hasCapability(DISTINCT))
                        assertFalse(repeated);
                }
                if (!fixture.endpoint.hasCapability(PROJECTION))
                    return; // silently do not test result since it would fail
            } else {
                if (poll) {
                    try (Results results = fixture.endpoint.query(query)) {
                        while (results.hasNext(0) || results.hasNext())
                            ac.add(results.next());
                    }
                } else {
                    try (Results results = fixture.endpoint.query(query)) {
                        results.forEachRemaining(ac::add);
                    }
                }
            }
            assertEquals(ac.stream().filter(s -> !ex.contains(s)).collect(toList()), emptyList());
            assertEquals(ex.stream().filter(s -> !ac.contains(s)).collect(toList()), emptyList());
        }
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testEmpty(@Nonnull Function<InputStream, Fixture<TPEndpoint>> f) {
        queryEmptyTest(f, new Triple(TestContext.Alice, TestContext.knows, TestContext.x));
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testAllOnEmpty(@Nonnull Function<InputStream, Fixture<TPEndpoint>> f) {
        queryEmptyTest(f, new Triple(TestContext.s, TestContext.p, TestContext.o));
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testAskOnEmpty(@Nonnull Function<InputStream, Fixture<TPEndpoint>> f) {
        queryEmptyTest(f, new Triple(TestContext.Alice, TestContext.knows, TestContext.Bob));
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testSingleObject(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "rdf-1.nt", new Triple(TestContext.Alice, TestContext.knows, TestContext.x),
                singleton(MapSolution.build(TestContext.x, TestContext.Bob)));
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testSingleObjectWithLimit(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "rdf-1.nt", new Triple(TestContext.Alice, TestContext.knows, TestContext.x),
                          singleton(MapSolution.build(TestContext.x, TestContext.Bob)), Limit.of(10));
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testSingleObjectPoll(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "rdf-1.nt", new Triple(TestContext.Alice, TestContext.knows, TestContext.x),
                singleton(MapSolution.build(TestContext.x, TestContext.Bob)), true);
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testSingleObjectPollWithLimit(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "rdf-1.nt", new Triple(TestContext.Alice, TestContext.knows, TestContext.x),
                singleton(MapSolution.build(TestContext.x, TestContext.Bob)), true, Limit.of(10));
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testQueryTwoObjects(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "rdf-1.nt", new Triple(TestContext.Bob, TestContext.name, TestContext.x),
                newHashSet(MapSolution.build(TestContext.x, EndpointTestBase.B_NAME1),
                           MapSolution.build(TestContext.x, EndpointTestBase.B_NAME2)));
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testQueryTwoObjectsPoll(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "rdf-1.nt", new Triple(TestContext.Bob, TestContext.name, TestContext.x),
                newHashSet(MapSolution.build(TestContext.x, EndpointTestBase.B_NAME1),
                        MapSolution.build(TestContext.x, EndpointTestBase.B_NAME2)), true);
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testQueryTwoSubjects(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "rdf-1.nt", new Triple(TestContext.x, TestContext.type, TestContext.Person),
                newHashSet(MapSolution.build(TestContext.x, TestContext.Alice),
                           MapSolution.build(TestContext.x, TestContext.Bob)));
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testQueryObjectWithVarPredicate(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "rdf-1.nt", new Triple(TestContext.Alice, TestContext.p, TestContext.o),
                newHashSet(MapSolution.builder().put(TestContext.p, TestContext.knows).put(TestContext.o, TestContext.Bob).build(),
                           MapSolution.builder().put(TestContext.p, TestContext.type).put(TestContext.o, TestContext.Person).build(),
                           MapSolution.builder().put(TestContext.p, TestContext.age).put(TestContext.o, EndpointTestBase.A_AGE).build(),
                           MapSolution.builder().put(TestContext.p, TestContext.name).put(TestContext.o, EndpointTestBase.A_NAME).build()));
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testQuerySubjectFromLiteral(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "rdf-1.nt", new Triple(TestContext.x, TestContext.name, EndpointTestBase.B_NAME1),
                singleton(MapSolution.build(TestContext.x, TestContext.Bob)));
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testQuerySubjectObject(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "rdf-1.nt", new Triple(TestContext.s, TestContext.name, TestContext.o),
                newHashSet(MapSolution.builder().put(TestContext.s, TestContext.Alice).put(TestContext.o, EndpointTestBase.A_NAME).build(),
                           MapSolution.builder().put(TestContext.s, TestContext.Bob).put(TestContext.o, EndpointTestBase.B_NAME1).build(),
                           MapSolution.builder().put(TestContext.s, TestContext.Bob).put(TestContext.o, EndpointTestBase.B_NAME2).build()));
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testQueryDistinctPredicates(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "rdf-1.nt", new Triple(TestContext.s, TestContext.p, TestContext.o),
                newHashSet(MapSolution.build(TestContext.p, TestContext.knows),
                        MapSolution.build(TestContext.p, TestContext.type),
                        MapSolution.build(TestContext.p, TestContext.age),
                        MapSolution.build(TestContext.p, TestContext.name)
                        ),
                Distinct.INSTANCE, Projection.of("p"));
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testQueryDistinctPredicatesPoll(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "rdf-1.nt", new Triple(TestContext.s, TestContext.p, TestContext.o),
                newHashSet(MapSolution.build(TestContext.p, TestContext.knows),
                        MapSolution.build(TestContext.p, TestContext.type),
                        MapSolution.build(TestContext.p, TestContext.age),
                        MapSolution.build(TestContext.p, TestContext.name)
                ),
                true,
                Distinct.INSTANCE, Projection.of("p"));
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testLimit(Function<InputStream, Fixture<TPEndpoint>> f) {
        try (Fixture<TPEndpoint> fixture = f.apply(open("rdf-2.nt"))) {
            if (!fixture.endpoint.hasCapability(LIMIT)) return; //silently skip
            CQuery qry = createQuery(TestContext.x, TestContext.knows, TestContext.y, Limit.of(1));
            List<Solution> list = new ArrayList<>();
            fixture.endpoint.query(qry).forEachRemainingThenClose(list::add);
            assertEquals(list.size(), 1);
            Solution s = list.get(0);
            Assert.assertEquals(s.get(TestContext.y), TestContext.Bob);
            assertTrue(Sets.newHashSet(TestContext.Alice, TestContext.Dave).contains(requireNonNull(s.get(TestContext.x)).asURI()));
        }
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testForceAskWithVars(Function<InputStream, Fixture<TPEndpoint>> f) {
        InputStream inputStream = open("rdf-1.nt");
        try (Fixture<TPEndpoint> fix = f.apply(inputStream)) {
            CQuery cQuery = createQuery(TestContext.s, TestContext.p, TestContext.o, Ask.INSTANCE);
            try (Results results = fix.endpoint.query(cQuery)) {
                assertTrue(results.hasNext());
                assertFalse(results.next().has(TestContext.p.getName()));
            }
        }
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testForceAskWithVarsNegative(Function<InputStream, Fixture<TPEndpoint>> f) {
        InputStream inputStream = open("rdf-1.nt");
        try (Fixture<TPEndpoint> fix = f.apply(inputStream)) {
            CQuery cQuery = createQuery(TestContext.s, TestContext.primaryTopic, TestContext.o, Ask.INSTANCE);
            try (Results results = fix.endpoint.query(cQuery)) {
                assertFalse(results.hasNext());
            }
        }
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testForceAskWithVarsNegativePoll(Function<InputStream, Fixture<TPEndpoint>> f) {
        InputStream inputStream = open("rdf-1.nt");
        try (Fixture<TPEndpoint> fix = f.apply(inputStream)) {
            CQuery cQuery = createQuery(TestContext.s, TestContext.primaryTopic, TestContext.o, Ask.INSTANCE);
            try (Results results = fix.endpoint.query(cQuery)) {
                assertFalse(results.hasNext(0)); //timeout or exhausted
                assertFalse(results.hasNext()); //exhausted
            }
        }
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testAlternatives(Function<InputStream, Fixture<TPEndpoint>> f) {
        InputStream inputStream = open("rdf-1.nt");
        try (Fixture<TPEndpoint> fix = f.apply(inputStream)) {
            EmptyEndpoint a1 = new EmptyEndpoint(), a2 = new EmptyEndpoint();
            fix.endpoint.addAlternative(a1);
            Assert.assertEquals(fix.endpoint.getAlternatives(), singleton(a1));
            fix.endpoint.addAlternatives(Arrays.asList(a1, a2));
            Assert.assertEquals(fix.endpoint.getAlternatives(), Sets.newHashSet(a1, a2));
        }
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testTransitiveAlternative(Function<InputStream, Fixture<TPEndpoint>> f) {
        InputStream inputStream = open("rdf-1.nt");
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

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testReflexiveAlternatives(Function<InputStream, Fixture<TPEndpoint>> f) {
        InputStream inputStream = open("rdf-1.nt");
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

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testNotAlternative(Function<InputStream, Fixture<TPEndpoint>> f) {
        InputStream inputStream = open("rdf-1.nt");
        try (Fixture<TPEndpoint> fix = f.apply(inputStream)) {
            EmptyEndpoint e1 = new EmptyEndpoint(), e2 = new EmptyEndpoint();
            assertFalse(fix.endpoint.isAlternative(e1));
            assertFalse(fix.endpoint.isAlternative(e2));
            Assert.assertEquals(fix.endpoint.getAlternatives(), emptySet());

            fix.endpoint.addAlternative(e1);
            assertTrue(fix.endpoint.isAlternative(e1));
            assertFalse(fix.endpoint.isAlternative(e2));
            Assert.assertEquals(fix.endpoint.getAlternatives(), singleton(e1));
        }
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testConcurrentAddAlternative(Function<InputStream, Fixture<TPEndpoint>> f)
    throws InterruptedException, ExecutionException {
        InputStream inputStream = open("rdf-1.nt");
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
            assertTrue(service.awaitTermination(2, TimeUnit.SECONDS));

            assertTrue(fix.endpoint.getAlternatives().stream()
                    .allMatch(fix.endpoint::isAlternative));
        }
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testFilter(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "rdf-1.nt", new Triple(TestContext.s, TestContext.age, TestContext.x),
                newHashSet(MapSolution.builder().put(TestContext.s, TestContext.Alice).put(TestContext.x, lit(23)).build()));
        queryResourceTest(f, "rdf-1.nt", new Triple(TestContext.s, TestContext.age, TestContext.x),
                Collections.emptySet(), SPARQLFilterFactory.parseFilter("?x > 23"));
    }

    @Test(dataProvider = "fixtureFactories", groups = {"endpointTest"})
    public void testFilterPoll(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "rdf-1.nt", new Triple(TestContext.s, TestContext.age, TestContext.x),
                newHashSet(MapSolution.builder().put(TestContext.s, TestContext.Alice).put(TestContext.x, lit(23)).build()),
                true);
        queryResourceTest(f, "rdf-1.nt", new Triple(TestContext.s, TestContext.age, TestContext.x),
                Collections.emptySet(), true,
                SPARQLFilterFactory.parseFilter("?x > 23"));
    }
}