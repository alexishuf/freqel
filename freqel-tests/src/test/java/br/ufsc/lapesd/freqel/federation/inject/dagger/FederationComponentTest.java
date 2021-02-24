package br.ufsc.lapesd.freqel.federation.inject.dagger;

import br.ufsc.lapesd.freqel.BSBMSelfTest;
import br.ufsc.lapesd.freqel.LargeRDFBenchSelfTest;
import br.ufsc.lapesd.freqel.PlanAssert;
import br.ufsc.lapesd.freqel.ResultsAssert;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.description.SelectDescription;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.Freqel;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import com.google.common.base.Stopwatch;
import org.apache.jena.rdf.model.Model;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class FederationComponentTest {

    private Map<String, Model> lrbModels;
    private Map<String, Model> bsbmModels;
    private Map<String, Op> queries;
    private Map<String, Set<Solution>> expected;

    @BeforeClass
    public void beforeClass() throws IOException, SPARQLParseException {
        lrbModels = new HashMap<>();
        for (String filename : LargeRDFBenchSelfTest.DATA_FILENAMES)
            lrbModels.put(filename, LargeRDFBenchSelfTest.loadData(filename));
        bsbmModels = new HashMap<>();
        for (String filename : BSBMSelfTest.DATA_FILENAMES)
            bsbmModels.put(filename, BSBMSelfTest.loadData(filename));
        queries = new HashMap<>();
        expected = new HashMap<>();
        for (String n : LargeRDFBenchSelfTest.QUERY_FILENAMES) {
            queries.put(n, LargeRDFBenchSelfTest.loadQuery(n));
            expected.put(n, new HashSet<>(LargeRDFBenchSelfTest.loadResults(n).getCollection()));
        }
        for (String n : BSBMSelfTest.QUERY_FILENAMES) {
            queries.put(n, BSBMSelfTest.loadQuery(n));
            expected.put(n, new HashSet<>(BSBMSelfTest.loadResults(n).getCollection()));
        }
    }

    @DataProvider public @Nonnull Object[][] queryData() {
        List<Object[]> rows = new ArrayList<>(asList(LargeRDFBenchSelfTest.queryNameData()));
        rows.addAll(asList(BSBMSelfTest.queryData()));
        return rows.toArray(new Object[0][]);
    }

    public static void main(String[] args) throws IOException, SPARQLParseException {
        FederationComponentTest t = new FederationComponentTest();
        t.beforeClass();
        for (int i = 0; i < 10; i++)
            t.testCreateDefault("B1");
        System.out.print("Hit ENTER to run...");
        //noinspection ResultOfMethodCallIgnored
        System.in.read();
        System.out.println("Running...");
        Stopwatch sw = Stopwatch.createStarted();
        int runs = 0;
        while (sw.elapsed(TimeUnit.MILLISECONDS) < 10000) {
            t.testCreateDefault("B1");
            ++runs;
        }
        System.out.printf("runs=%d\n", runs);
    }

    @Test(dataProvider = "queryData")
    public void testCreateDefault(@Nonnull String queryName) {
        Federation federation = DaggerFederationComponent.builder().build().federation();
        Set<Map.Entry<String, Model>> modelEntries = queryName.startsWith("query")
                                                   ? bsbmModels.entrySet() : lrbModels.entrySet();
        for (Map.Entry<String, Model> e : modelEntries)
            federation.addSource(ARQEndpoint.forModel(e.getValue(), e.getKey()));
        Op parsedQuery = queries.get(queryName);
        Op plan = federation.plan(parsedQuery);
        PlanAssert.assertPlanAnswers(plan, parsedQuery);

        Results actual = federation.query(parsedQuery);
        ResultsAssert.assertExpectedResults(actual, expected.get(queryName));
    }

    @Test
    public void testSourcesCache() throws Exception {
        try {
            File dir = Files.createTempDirectory("freqel").toFile();
            System.setProperty("sources.cache.dir", dir.getAbsolutePath());
            try (Federation federation = Freqel.createFederation()) {
                for (Map.Entry<String, Model> e : bsbmModels.entrySet()) {
                    ARQEndpoint ep = ARQEndpoint.forModel(e.getValue(), e.getKey());
                    SelectDescription description = new SelectDescription(ep);
                    description.saveWhenReady(federation.getSourceCache(), e.getKey());
                    ep.setDescription(description);
                    federation.addSource(ep);
                }
                assertTrue(federation.initAllSources(20, TimeUnit.SECONDS));
            }
            File[] files = dir.listFiles();
            assertNotNull(files);
            assertTrue(files.length > 1);
            File index = new File(dir, "index.yaml");
            assertTrue(index.exists());
            assertTrue(index.isFile());
        } finally {
            System.clearProperty("sources.cache.dir");
        }
    }
}