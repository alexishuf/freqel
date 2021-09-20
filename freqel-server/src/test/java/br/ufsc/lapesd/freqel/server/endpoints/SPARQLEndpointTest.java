package br.ufsc.lapesd.freqel.server.endpoints;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.description.SelectDescription;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.Freqel;
import br.ufsc.lapesd.freqel.jena.model.vocab.SPARQLSD;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.jena.rs.ModelMessageBodyWriter;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.reason.regimes.W3CEntailmentRegimes;
import br.ufsc.lapesd.freqel.server.utils.PercentEncoder;
import br.ufsc.lapesd.freqel.util.DictTree;
import com.google.common.collect.Sets;
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
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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

public class SPARQLEndpointTest extends JerseyTestNg.ContainerPerClassTest implements TestContext {

    private Federation federation;
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

    @Override
    protected Application configure() {
        federation = Freqel.createFederation();
        federation.addSource(createSource("rdf-1.nt", Lang.NT));
        return new ResourceConfig()
                .property(Federation.class.getName(), federation)
                .register(ModelMessageBodyWriter.class)
                .register(SPARQLEndpoint.class);
    }

    @BeforeClass
    @Override
    public void setUp() throws Exception {
        super.setUp();
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

    @AfterClass
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (federation != null)
            federation.close();
    }

    @Test
    public void testQueryGetJsonResults() throws IOException {
        String json = target("sparql/query")
                .queryParam("query", PercentEncoder.encode(query1))
                .request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
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
        String tsv = target("sparql/query")
                .queryParam("query", PercentEncoder.encode(query1))
                .queryParam("output", "tsv")
                .request(MediaType.TEXT_HTML_TYPE) /* bogus accept is overridden */
                .get(String.class);

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
        Response response = target("/sparql").request().accept("*/*").get();
        assertEquals(response.getStatus(), 200);
        assertEquals(response.getMediaType(), MediaType.valueOf("text/turtle"));
        String ttl = response.readEntity(String.class);
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, new ByteArrayInputStream(ttl.getBytes(UTF_8)), Lang.TTL);
        assertFalse(model.isEmpty());

        String prolog = "PREFIX sd: <" + SPARQLSD.NS + ">\n" +
                        "PREFIX ent: <" + W3CEntailmentRegimes.NS + ">\n" +
                        "SELECT * WHERE {\n";
        // single service in description
        try (QueryExecution ex = QueryExecutionFactory.create(prolog+"?s a sd:Service}", model)) {
            ResultSet rs = ex.execSelect();
            assertTrue(rs.hasNext());
            assertEquals(rs.next().get("s"), model.createResource(target("/sparql").getUri().toString()));
            assertFalse(rs.hasNext(), "Single");
        }
        // test simple entailment regime  by default and on a named graph
        try (QueryExecution ex = QueryExecutionFactory.create(prolog +
                "  <" + target("/sparql").getUri().toString() + "> a sd:Service;\n" +
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