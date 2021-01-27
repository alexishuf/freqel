package br.ufsc.lapesd.freqel.server.endpoints;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.description.SelectDescription;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.Source;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.server.utils.PercentEncoder;
import br.ufsc.lapesd.freqel.util.DictTree;
import com.google.common.collect.Sets;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
    private @Nonnull Source getSource(@Nonnull String resourceRelativePath) {
        try (InputStream in = getClass().getResourceAsStream(resourceRelativePath)) {
            if (in == null)
                fail("Resource "+resourceRelativePath+" not found");
            Model model = ModelFactory.createDefaultModel();
            RDFDataMgr.read(model, in, Lang.NT);
            ARQEndpoint ep = ARQEndpoint.forModel(model, "rdf-1.nt");
            return new Source(new SelectDescription(ep), ep);
        } catch (IOException e) {
            fail("Unexpected exception", e);
        }
        return null;
    }

    @Override
    protected Application configure() {
        federation = Federation.createDefault();
        federation.addSource(getSource("../../rdf-1.nt"));
        return new ResourceConfig()
                .property(Federation.class.getName(), federation)
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
        map.put("x", "<"+Alice.getURI()+">");
        map.put("name", "\"bob\"@en");
        results1TSV.add(new HashMap<>(map));
        map.put("name", "\"beto\"@pt");
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
        String json = target("sparql/query")
                .queryParam("query", PercentEncoder.encode(query1))
                .queryParam("output", "tsv")
                .request(MediaType.TEXT_HTML_TYPE) /* bogus accept is overridden */
                .get(String.class);

        HashSet<String> vars = Sets.newHashSet("x", "name");

        Set<Map<String, String>> solutions = new HashSet<>();
        CSVFormat format = CSVFormat.RFC4180.withDelimiter('\t').withFirstRecordAsHeader();
        try (StringReader reader = new StringReader(json);
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
}