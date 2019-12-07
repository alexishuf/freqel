package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.NamedFunction;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;
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

import static br.ufsc.lapesd.riefederator.model.term.std.StdLit.fromUnescaped;
import static com.google.common.collect.Sets.newHashSet;
import static java.net.InetAddress.getLocalHost;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TPEndpointTest {
    public static class Fixture implements AutoCloseable {
        private final  @Nonnull TPEndpoint endpoint;

        public Fixture(@Nonnull TPEndpoint endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void close() { }
    }

    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    public static final @Nonnull StdURI TYPE = new StdURI(RDF.type.getURI());
    public static final @Nonnull StdURI KNOWS = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI AGE = new StdURI(FOAF.age.getURI());
    public static final @Nonnull StdURI NAME = new StdURI(FOAF.name.getURI());
    public static final @Nonnull StdURI PERSON = new StdURI(FOAF.Person.getURI());
    public static final @Nonnull StdURI INT = new StdURI(XSDDatatype.XSDint.getURI());
    public static final @Nonnull StdLit A_AGE = fromUnescaped("23", INT);
    public static final @Nonnull StdLit A_NAME = fromUnescaped("alice", "en");
    public static final @Nonnull StdLit B_NAME1 = fromUnescaped("bob", "en");
    public static final @Nonnull StdLit B_NAME2 = fromUnescaped("beto", "pt");
    public static final @Nonnull StdVar X = new StdVar("X");
    public static final @Nonnull StdVar S = new StdVar("S");
    public static final @Nonnull StdVar P = new StdVar("P");
    public static final @Nonnull StdVar O = new StdVar("O");

    public static final @Nonnull List<NamedFunction<InputStream, Fixture>> endpoints;

    static {
        endpoints = new ArrayList<>();
        endpoints.add(new NamedFunction<>("ARQEndpoint.forModel", stream -> {
            Model model = ModelFactory.createDefaultModel();
            RDFDataMgr.read(model, stream, "", Lang.TTL);
            return new Fixture(ARQEndpoint.forModel(model));
        }));
        endpoints.add(new NamedFunction<>("ARQEndpoint.forDataset", stream -> {
            Dataset ds = DatasetFactory.create();
            RDFDataMgr.read(ds, stream, "", Lang.TTL);
            return new Fixture(ARQEndpoint.forDataset(ds));
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
            return new Fixture(ep) {
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

    private void queryEmptyTest(@Nonnull Function<InputStream, Fixture> f, Triple query) {
        Set<String> varNames = new HashSet<>();
        query.forEach(t -> {if (t.isVar()) varNames.add(t.asVar().getName());});

        try (Fixture fixture = f.apply(getClass().getResourceAsStream("../empty.nt"))) {
            try (Results results = fixture.endpoint.query(query)) {
                assertEquals(results.getVarNames(), varNames);
                assertFalse(results.hasNext());
                assertEquals(results.getCardinality(), Cardinality.EMPTY);
                assertEquals(results.getReadyCount(), 0);
            }
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testEmpty(@Nonnull Function<InputStream, Fixture> f) {
        queryEmptyTest(f, new Triple(ALICE, KNOWS, X));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testAllOnEmpty(@Nonnull Function<InputStream, Fixture> f) {
        queryEmptyTest(f, new Triple(S, P, O));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testAskOnEmpty(@Nonnull Function<InputStream, Fixture> f) {
        queryEmptyTest(f, new Triple(ALICE, KNOWS, BOB));
    }

    @SuppressWarnings("SameParameterValue")
    private void queryResourceTest(Function<InputStream, Fixture> f, @Nonnull String filename,
                                   @Nonnull Triple query, @Nonnull Set<Solution> ex) {
        try (Fixture fixture = f.apply(getClass().getResourceAsStream(filename))) {
            Set<Solution> ac = new HashSet<>();
            try (Results results = fixture.endpoint.query(query)) {
                results.forEachRemaining(ac::add);
            }
            assertEquals(ac.stream().filter(s -> !ex.contains(s)).collect(toList()), emptyList());
            assertEquals(ex.stream().filter(s -> !ac.contains(s)).collect(toList()), emptyList());
        }
    }

    @Test(dataProvider = "fixtureFactories")
    public void testSingleObject(Function<InputStream, Fixture> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(ALICE, KNOWS, X),
                singleton(MapSolution.build("X", BOB)));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testQueryTwoObjects(Function<InputStream, Fixture> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(BOB, NAME, X),
                newHashSet(MapSolution.build("X", B_NAME1),
                           MapSolution.build("X", B_NAME2)));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testQueryTwoSubjects(Function<InputStream, Fixture> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(X, TYPE, PERSON),
                newHashSet(MapSolution.build("X", ALICE),
                           MapSolution.build("X", BOB)));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testQueryObjectWithVarPredicate(Function<InputStream, Fixture> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(ALICE, P, O),
                newHashSet(MapSolution.builder().put("P", KNOWS).put("O", BOB).build(),
                           MapSolution.builder().put("P", TYPE).put("O", PERSON).build(),
                           MapSolution.builder().put("P", AGE).put("O", A_AGE).build(),
                           MapSolution.builder().put("P", NAME).put("O", A_NAME).build()));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testQuerySubjectFromLiteral(Function<InputStream, Fixture> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(X, NAME, B_NAME1),
                singleton(MapSolution.build("X", BOB)));
    }

    @Test(dataProvider = "fixtureFactories")
    public void testQuerySUbjectObject(Function<InputStream, Fixture> f) {
        queryResourceTest(f, "../rdf-1.nt", new Triple(S, NAME, O),
                newHashSet(MapSolution.builder().put("S", ALICE).put("O", A_NAME).build(),
                           MapSolution.builder().put("S",   BOB).put("O", B_NAME1).build(),
                           MapSolution.builder().put("S",   BOB).put("O", B_NAME2).build()));
    }
}