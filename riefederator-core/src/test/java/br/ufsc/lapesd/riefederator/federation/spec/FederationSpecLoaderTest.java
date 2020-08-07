package br.ufsc.lapesd.riefederator.federation.spec;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.Federation;
import br.ufsc.lapesd.riefederator.federation.planner.PlannerTest;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.TPEndpointTest;
import br.ufsc.lapesd.riefederator.query.results.Results;
import com.google.common.base.Stopwatch;
import org.apache.commons.io.FileUtils;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.*;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class FederationSpecLoaderTest implements TestContext {
    private Model rdf1;
    private File dir;
    private TPEndpointTest.FusekiEndpoint fusekiEndpoint;

    @BeforeClass
    public void classSetUp() throws Exception {
        rdf1 = ModelFactory.createDefaultModel();
        try (InputStream stream = getClass().getResourceAsStream("../../rdf-1.nt")) {
            assertNotNull(stream);
            RDFDataMgr.read(rdf1, stream, Lang.TTL);
        }
    }

    @BeforeMethod
    public void setUp() throws IOException {
        dir = Files.createTempDirectory("riefederator").toFile();
    }

    @AfterMethod
    public void tearDown() throws IOException {
        if (dir != null)
            FileUtils.deleteDirectory(dir);
        if (fusekiEndpoint != null)
            fusekiEndpoint.close();
    }

    @Test
    public void testUseCachedDescriptionForSPARQLService() throws Exception {
        Dataset ds = DatasetFactory.createTxnMem();
        ds.getDefaultModel().add(rdf1);
        fusekiEndpoint = new TPEndpointTest.FusekiEndpoint(ds);

        File cacheDir = new File(dir, "cache");
        assertTrue(cacheDir.mkdirs());
        File config = new File(dir, "config.yaml");
        try (PrintStream out = new PrintStream(new FileOutputStream(config))) {
            out.printf(
                    "sources:\n" +
                    "  - loader: sparql\n" +
                    "    uri: %s\n" +
                    "sources-cache: cache\n", fusekiEndpoint.uri);
        }

        try (Federation federation = new FederationSpecLoader().load(config)) {
            Stopwatch sw = Stopwatch.createStarted();
            assertTrue(federation.initAllSources(20, TimeUnit.SECONDS));
            assertTrue(sw.elapsed(TimeUnit.SECONDS) < 15); //init should be fast!
            Set<Term> actual = new HashSet<>();
            try (Results results = federation.query(createQuery(x, knows, Bob))) {
                results.forEachRemainingThenClose(s -> actual.add(s.get(x)));
            }
            assertEquals(actual, singleton(Alice));
        }

        fusekiEndpoint.close(); //Fuseki is now unresponsive
        try (Federation federation = new FederationSpecLoader().load(config)) {
            //the only source is already initialized:
            assertTrue(federation.initAllSources(0, TimeUnit.SECONDS));
            CQuery query = createQuery(x, knows, Bob);
            Op plan = federation.plan(query); // no exception
            PlannerTest.assertPlanAnswers(plan, query); //not an EmptyNode
        }
    }

}