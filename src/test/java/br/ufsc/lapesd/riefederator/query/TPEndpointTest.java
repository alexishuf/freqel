package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.NamedFunction;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.Sets.newHashSet;
import static java.net.InetAddress.getLocalHost;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

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
    protected void queryResourceTest(Function<InputStream, Fixture<TPEndpoint>> f, @Nonnull String filename,
                                     @Nonnull Triple query, @Nonnull Set<Solution> ex) {
        try (Fixture<TPEndpoint> fixture = f.apply(getClass().getResourceAsStream(filename))) {
            Set<Solution> ac = new HashSet<>();
            try (Results results = fixture.endpoint.query(query)) {
                results.forEachRemaining(ac::add);
            }
            assertEquals(ac.stream().filter(s -> !ex.contains(s)).collect(toList()), emptyList());
            assertEquals(ex.stream().filter(s -> !ac.contains(s)).collect(toList()), emptyList());
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testEmpty(@Nonnull Function<InputStream, Fixture<TPEndpoint>> f) {
        queryEmptyTest(f, new Triple(ALICE, KNOWS, X));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testAllOnEmpty(@Nonnull Function<InputStream, Fixture<TPEndpoint>> f) {
        queryEmptyTest(f, new Triple(S, P, O));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testAskOnEmpty(@Nonnull Function<InputStream, Fixture<TPEndpoint>> f) {
        queryEmptyTest(f, new Triple(ALICE, KNOWS, BOB));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testSingleObject(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(ALICE, KNOWS, X),
                singleton(MapSolution.build("X", BOB)));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testQueryTwoObjects(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(BOB, NAME, X),
                newHashSet(MapSolution.build("X", B_NAME1),
                           MapSolution.build("X", B_NAME2)));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testQueryTwoSubjects(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(X, TYPE, PERSON),
                newHashSet(MapSolution.build("X", ALICE),
                           MapSolution.build("X", BOB)));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testQueryObjectWithVarPredicate(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(ALICE, P, O),
                newHashSet(MapSolution.builder().put("P", KNOWS).put("O", BOB).build(),
                           MapSolution.builder().put("P", TYPE).put("O", PERSON).build(),
                           MapSolution.builder().put("P", AGE).put("O", A_AGE).build(),
                           MapSolution.builder().put("P", NAME).put("O", A_NAME).build()));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testQuerySubjectFromLiteral(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(X, NAME, B_NAME1),
                singleton(MapSolution.build("X", BOB)));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testQuerySUbjectObject(Function<InputStream, Fixture<TPEndpoint>> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(S, NAME, O),
                newHashSet(MapSolution.builder().put("S", ALICE).put("O", A_NAME).build(),
                           MapSolution.builder().put("S",   BOB).put("O", B_NAME1).build(),
                           MapSolution.builder().put("S",   BOB).put("O", B_NAME2).build()));
    }
}