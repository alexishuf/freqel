package br.ufsc.lapesd.freqel.server.endpoints;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.description.SelectDescription;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.Freqel;
import br.ufsc.lapesd.freqel.jena.model.vocab.SPARQLSD;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.reason.regimes.W3CEntailmentRegimes;
import br.ufsc.lapesd.freqel.server.utils.PercentEncoder;
import br.ufsc.lapesd.freqel.util.DictTree;
import com.google.common.collect.Sets;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import reactor.netty.DisposableServer;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.http.server.HttpServer;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.*;

public class SPARQLEndpointTest implements TestContext {

    private Federation federation;
    private DisposableServer server;
    private final String query1 = "PREFIX ex: <"+EX+">\n" +
            "PREFIX foaf: <"+ FOAF.NS +">\n" +
            "SELECT ?x ?name WHERE {\n" +
            "  ?x foaf:knows ?y;\n" +
            "     foaf:age ?age\n" +
            "     FILTER(?age > 20).\n" +
            "  ?y foaf:name ?name.\n" +
            "}\n";
    private Set<Map<String, String>> results1;
    private Set<Map<String, String>> results1TSV;


    @SuppressWarnings("SameParameterValue")
    static @Nonnull TPEndpoint createSource(@Nonnull String resourceRelativePath,
                                            @Nonnull Lang lang) {
        try (InputStream in = new TestContext(){}.open(resourceRelativePath)) {
            Model model = ModelFactory.createDefaultModel();
            RDFDataMgr.read(model, in, lang);
            ARQEndpoint ep = ARQEndpoint.forModel(model, resourceRelativePath);
            return ep.setDescription(new SelectDescription(ep));
        } catch (IOException e) {
            fail("Unexpected exception", e);
        }
        return null;
    }

    @BeforeClass(timeOut = 60000)
    public void setUp() {
        federation = Freqel.createFederation();
        federation.addSource(createSource("rdf-1.nt", Lang.NT));
        SPARQLEndpoint endpoint = new SPARQLEndpoint(federation);
        server = HttpServer.create().host("127.0.0.1").route(routes ->
                routes.get("/sparql", endpoint::handle)
                        .post("/sparql", endpoint::handle)
        ).bindNow();

        results1 = new HashSet<>();
        Map<String, String> map = new HashMap<>();
        map.put("x", Alice.getURI());
        map.put("name", "bob");
        results1.add(new HashMap<>(map));
        map.put("name", "beto");
        results1.add(new HashMap<>(map));

        results1TSV = new HashSet<>();
        map.clear();
        map.put("?x", "<"+Alice.getURI()+">");
        map.put("?name", "\"bob\"@en");
        results1TSV.add(new HashMap<>(map));
        map.put("?name", "\"beto\"@pt");
        results1TSV.add(new HashMap<>(map));
    }

    @AfterClass(timeOut = 30000)
    public void tearDown() {
        server.disposeNow();
        if (federation != null)
            federation.close();
    }

    private @Nonnull String uri(@Nonnull String... keysAndValues) {
        if (keysAndValues.length % 2 > 0)
            throw new IllegalArgumentException("keysAndValues.length is not even");
        StringBuilder builder = new StringBuilder("http://127.0.0.1:")
                .append(server.port()).append("/sparql?");
        for (int i = 0; i < keysAndValues.length; i += 2) {
            String value = PercentEncoder.encode(keysAndValues[i + 1]);
            builder.append(keysAndValues[i]).append('=').append(value).append('&');
        }
        builder.setLength(builder.length()-1);
        return builder.toString();
    }

    @SuppressWarnings("SameParameterValue")
    private @Nonnull String get(@Nonnull String uri, @Nonnull String accept) {
        String string = HttpClient.create()
                .headers(b -> b.set(HttpHeaderNames.ACCEPT, accept)).get()
                .uri(uri)
                .responseContent().aggregate().asString(UTF_8).block();
        assertNotNull(string);
        assertFalse(string.isEmpty());
        return string;
    }


    @Test
    public void testQueryGetJsonResults() throws IOException {
        String json = get(uri("query", query1),
                          "application/sparql-results+json");
        DictTree tree = DictTree.load().fromJsonString(json);

        HashSet<String> vars = Sets.newHashSet("x", "name");
        assertEquals(tree.getSetNN("head/vars"), vars);

        Set<Map<String, String>> solutions = new HashSet<>();
        for (Object bindingObj : tree.getListNN("results/bindings")) {
            assertTrue(bindingObj instanceof DictTree);
            DictTree binding = (DictTree) bindingObj;
            Map<String, String> solution = new HashMap<>();
            for (String var : vars)
                solution.put(var, binding.getString(var + "/value"));
            solutions.add(new HashMap<>(solution));
        }
        assertEquals(solutions, results1);
    }

    @Test
    public void testQueryGetTSVResultsViaParam() throws IOException {
        String tsv =  get(uri("query", query1, "output", "tsv"), "text/html");
        HashSet<String> vars = Sets.newHashSet("?x", "?name");

        Set<Map<String, String>> solutions = new HashSet<>();
        CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter('\t')
                .withQuote('\'').withRecordSeparator('\n');
        try (StringReader reader = new StringReader(tsv);
             CSVParser parser = new CSVParser(reader, format)) {
            assertEquals(new HashSet<>(parser.getHeaderNames()), vars);
            for (CSVRecord record : parser) {
                Map<String, String> solution = new HashMap<>();
                for (String var : vars)
                    solution.put(var, record.get(var));
                solutions.add(solution);
            }
        }

        assertEquals(solutions, results1TSV);
    }

    @Test
    public void testServiceDescriptionNoReasoning() {
        HttpClientResponse response = HttpClient.create()
                .headers(h -> h.set(HttpHeaderNames.ACCEPT, "*/*")).get().uri(uri())
                .response().block();
        String body = HttpClient.create()
                .headers(h -> h.set(HttpHeaderNames.ACCEPT, "*/*")).get().uri(uri())
                .responseContent().aggregate().asString(UTF_8).block();
        assertNotNull(response);
        assertEquals(response.status().code(), 200);
        assertEquals(response.responseHeaders().get(HttpHeaderNames.CONTENT_TYPE),
                     "text/turtle");

        assertNotNull(body);
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, new ByteArrayInputStream(body.getBytes(UTF_8)), Lang.TTL);
        assertFalse(model.isEmpty());

        String prolog = "PREFIX sd: <" + SPARQLSD.NS + ">\n" +
                        "PREFIX ent: <" + W3CEntailmentRegimes.NS + ">\n" +
                        "SELECT * WHERE {\n";
        // single service in description
        try (QueryExecution ex = QueryExecutionFactory.create(prolog+"?s a sd:Service}", model)) {
            ResultSet rs = ex.execSelect();
            assertTrue(rs.hasNext());
            assertEquals(rs.next().get("s"), model.createResource(uri()));
            assertFalse(rs.hasNext(), "Single");
        }
        // test simple entailment regime  by default and on a named graph
        try (QueryExecution ex = QueryExecutionFactory.create(prolog +
                "  <"+uri()+"> a sd:Service;\n" +
                "    sd:defaultEntailmentRegime <"+W3CEntailmentRegimes.SIMPLE.iri()+">;\n" +
                "    sd:defaultDataset ?ds.\n" +
                "  ?ds a sd:Dataset;\n" +
                "    sd:defaultGraph ?defGraph;\n" +
                "    sd:namedGraph/sd:entailmentRegime ent:Simple.}", model)) {
            ResultSet rs = ex.execSelect();
            assertTrue(rs.hasNext());
            rs.next();
            assertFalse(rs.hasNext(), "Expected a single solution (federation has no reasoning enabled");
        }
    }
}