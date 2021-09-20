package br.ufsc.lapesd.freqel.server.endpoints;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.V;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.Freqel;
import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import br.ufsc.lapesd.freqel.jena.rs.ModelMessageBodyWriter;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.reason.regimes.W3CEntailmentRegimes;
import br.ufsc.lapesd.freqel.reason.tbox.endpoint.HeuristicEndpointReasoner;
import com.github.lapesd.rdfit.util.Utils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Application;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.jena.query.QueryExecutionFactory.createServiceRequest;
import static org.testng.Assert.assertEquals;

public class QueryReasoningGraphTest extends JerseyTestNg.ContainerPerClassTest implements TestContext {

    private static final String PREFIXES = "PREFIX rdfs: <"+V.RDFS.NS+">\n" +
            "PREFIX xsd: <"+ V.XSD.NS +">\n" +
            "PREFIX foaf: <"+ V.FOAF.NS +">\n" +
            "PREFIX ex: <"+TestContext.EX+">\n" +
            "PREFIX : <"+TestContext.EX+">\n";
    private final String RDFS_IRI = V.Freqel.Entailment.Graph.RDFS.getURI();
    private final String SIMPLE_IRI = V.Freqel.Entailment.Graph.Simple.getURI();
    private Federation federation;
    private File tBoxFile;
    private String endpointURI;

    @AfterClass @Override public void tearDown() throws Exception {
        super.tearDown();
        if (federation != null)
            federation.close();
        if (tBoxFile != null && tBoxFile.exists() && !tBoxFile.delete())
            Assert.fail("Failed to delete tBoxFile at " + tBoxFile);
        tBoxFile = null;
        federation = null;
    }

    @Override protected Application configure() {
        String tBoxResource = "reason/tbox/replacements/generators/subterm-onto.ttl";
        String aBoxResource = "reason/tbox/replacements/generators/subterm-1.ttl";

        try {
            tBoxFile = Files.createTempFile("onto", ".ttl").toFile();
            tBoxFile.deleteOnExit();
            Utils.extractResource(tBoxFile, Freqel.class, "../"+tBoxResource);
        } catch (IOException e) {
            Assert.fail("Could not extract resource to temp file", e);
        }

        TPEndpoint source = SPARQLEndpointTest.createSource(aBoxResource, Lang.TTL);
        FreqelConfig cfg = FreqelConfig.fromHardCodedDefaults()
                .set(FreqelConfig.Key.ADVERTISED_REASONING,
                        W3CEntailmentRegimes.RDFS.fromSingleSourceABox())
                .set(FreqelConfig.Key.TBOX_RDF, tBoxFile.getAbsolutePath())
                .set(FreqelConfig.Key.ENDPOINT_REASONER, HeuristicEndpointReasoner.class.getName());
        federation = Freqel.createFederation(cfg, source);

        return new ResourceConfig()
                .property(Federation.class.getName(), federation)
                .register(SPARQLEndpoint.class)
                .register(ModelMessageBodyWriter.class);
    }

    @BeforeMethod
    public void beforeMethod() {
        endpointURI = target("/sparql").getUri().toString();
    }

    @SuppressWarnings("SameParameterValue")
    private void queryList(@Nonnull String sparql, @Nonnull Collection<String> expected,
                           @Nullable Consumer<QueryEngineHTTP> setup) {
        Set<String> actual = new HashSet<>();
        Query query = QueryFactory.create(PREFIXES + sparql);
        try (QueryEngineHTTP ex = createServiceRequest(endpointURI, query)) {
            if (setup != null)
                setup.accept(ex);
            ResultSet rs = ex.execSelect();
            while(rs.hasNext())
                actual.add(rs.next().get("x").toString());
        }
        assertEquals(actual, new HashSet<>(expected));
    }

    @Test
    public void testQueryDefaultGraph() {
        List<String> expected = singletonList(EX + "Charlie");
        queryList("SELECT * WHERE {ex:Alice :p ?x}", expected, null);
    }

    @Test
    public void testQueryNamedGraph() {
        List<String> expected = asList(EX+"Charlie", EX+"Dave", EX+"Eric");
        queryList("SELECT * WHERE {ex:Alice :p ?x}", expected,
                ex -> ex.addNamedGraph(W3CEntailmentRegimes.RDFS.getGraphIRI()));
    }


    @Test
    public void testQueryNamedGraphWithFromClause() {
        List<String> expected = asList(EX+"Charlie", EX+"Dave", EX+"Eric");
        queryList("SELECT * FROM <"+RDFS_IRI+">\n" +
                "WHERE {ex:Alice :p ?x}", expected, null);
    }

    @Test
    public void testQueryNamedGraphWithFromNamedClause() {
        List<String> expected = asList(EX+"Charlie", EX+"Dave", EX+"Eric");
        queryList("SELECT * FROM NAMED <"+RDFS_IRI+">\n" +
                "WHERE {ex:Alice :p ?x}",
                expected, null);
    }

    @Test
    public void testQueryNamedGraphWithFromAndFromNamedClause() {
        List<String> expected = asList(EX+"Charlie", EX+"Dave", EX+"Eric");
        queryList("SELECT * \n" +
                "FROM <"+SIMPLE_IRI+">\n" +
                "FROM NAMED <"+RDFS_IRI+">\n" +
                "WHERE {ex:Alice :p ?x}",
                expected, null);
    }
}
