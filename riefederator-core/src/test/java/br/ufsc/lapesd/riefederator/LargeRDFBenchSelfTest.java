package br.ufsc.lapesd.riefederator;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.description.SelectDescription;
import br.ufsc.lapesd.riefederator.federation.Federation;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.jena.query.JenaSolution;
import br.ufsc.lapesd.riefederator.query.TPEndpointTest;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.SPARQLClient;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParseException;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParser;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import org.apache.commons.io.IOUtils;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.Var;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.federation.SingletonSourceFederation.createFederation;
import static br.ufsc.lapesd.riefederator.testgen.LargeRDFBenchTestResourcesGenerator.parseResults;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

/**
 * The LargeRDFBench test cases use data reassembled from the query results.
 *
 * This class checks data is valid and that the queries yield expected answers from the data
 * (without federating).
 */
public class LargeRDFBenchSelfTest {
    private static final Logger logger = LoggerFactory.getLogger(LargeRDFBenchSelfTest.class);
    public static List<String> DATA_FILENAMES = Arrays.asList(
            "Affymetrix.nt",
            "DBPedia-Subset.nt",
            "DrugBank.nt",
            "GeoNames.nt",
            "KEGG.nt",
            "LinkedTCGA-E.nt",
            "LinkedTCGA-M.nt",
            "LMDB.nt",
            "NYT.nt",
            "orphan.nt",
            "SWDFood.nt",
            "tcga-orphan.nt");
    public static final List<String> QUERY_FILENAMES = Arrays.asList(
            "B1", //UNION
            "B2",
            "B3", //ORDER BY
            "B4", //UNION
            "B5",
            "B6",
            "B7",
            "B8", //UNION
            // "C2", //OPTIONAL
            // "C3", //OPTIONAL + DISTINCT
            // "C6", //ORDER BY
            // "C7", //OPTIONAL + DISTINCT
            // "C8", //OPTIONAL
            "C10",
            "S1", //UNION
            "S2",
            "S3",
            "S4",
            "S5",
            "S6",
            "S7",
            "S8", //UNION
            "S9", //UNION
            "S10",
            "S11",
            "S12",
            "S13"
            // "S14"  //OPTIONAL
    );
    public static final String RESOURCE_DIR =
            "br/ufsc/lapesd/riefederator/LargeRDFBench-reassembled/";


    public static @Nonnull Op loadQuery(@Nonnull String queryName)
            throws IOException, SPARQLParseException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        String queryPath = RESOURCE_DIR + "/queries/" + queryName;
        try (InputStream stream = cl.getResourceAsStream(queryPath)) {
            assertNotNull(stream);
            return SPARQLParser.tolerant().parse(stream);
        }
    }

    public static @Nonnull Model loadData(@Nonnull String fileName) throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        String queryPath = RESOURCE_DIR + "/data/" + fileName;
        try (InputStream stream = cl.getResourceAsStream(queryPath)) {
            assertNotNull(stream);
            Model model = ModelFactory.createDefaultModel();
            RDFDataMgr.read(model, stream, Lang.TTL);
            return model;
        }
    }

    public static @Nonnull CollectionResults loadResults(@Nonnull String queryName)
            throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        String queryPath = RESOURCE_DIR + "/queries/" + queryName;
        String resultsPath = RESOURCE_DIR + "/results/" + queryName;
        Set<String> actualVars;
        try (InputStream queryStream = cl.getResourceAsStream(queryPath)) {
            assertNotNull(queryStream);
            Query query = QueryFactory.create(IOUtils.toString(queryStream, UTF_8));
            actualVars = query.getProjectVars().stream().map(Var::getVarName)
                                      .collect(toSet());
        }
        InputStream stream = cl.getResourceAsStream(resultsPath+".union");
        if (stream == null)
            stream = cl.getResourceAsStream(resultsPath);
        assertNotNull(stream);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, UTF_8))) {
            List<Solution> clean = new ArrayList<>();
            for (Solution solution : parseResults(queryName, reader).getCollection()) {
                MapSolution.Builder builder = MapSolution.builder(solution);
                solution.getVarNames().stream().filter(v -> !actualVars.contains(v))
                                      .forEach(builder::remove);
                actualVars.stream().filter(v -> !solution.getVarNames().contains(v))
                          .forEach(v -> builder.put(v, null));
                clean.add(builder.build());
            }
            return new CollectionResults(clean, actualVars);
        }
    }

    private Model allData = null;
    private @Nullable TPEndpointTest.FusekiEndpoint fuseki = null;

    @BeforeClass(groups = {"fast"})
    public void setUp() {
        Model model = ModelFactory.createDefaultModel();
        ClassLoader cl = getClass().getClassLoader();
        String dataPath = RESOURCE_DIR + "/data/";
        for (String filename : DATA_FILENAMES) {
            String path = dataPath + filename;
            try (InputStream in = cl.getResourceAsStream(path)) {
                if (in == null) {
                    logger.error("Missing resource file {}", path);
                    return;
                }
                RDFDataMgr.read(model, in, Lang.NT);
            } catch (IOException e) {
                logger.error("Failed to open resource {}", path, e);
            }
        }
        allData = model;
    }

    @AfterClass
    public void tearDown() {
        if (fuseki != null)
            fuseki.close();
    }

    private @Nonnull SPARQLClient createSPARQLClient() {
        if (fuseki == null)
            fuseki = new TPEndpointTest.FusekiEndpoint(DatasetFactory.create(allData));
        return new SPARQLClient(fuseki.uri);
    }

    @DataProvider
    public static @Nonnull Object[][] dataFilesTestData() {
        return DATA_FILENAMES.stream()
                .map(n -> new Object[]{RESOURCE_DIR+"/data/"+n})
                .toArray(Object[][]::new);
    }

    @Test(dataProvider = "dataFilesTestData")
    public void testDataReadable(String path) throws IOException {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(path)) {
            assertNotNull(in);
            String string = IOUtils.toString(in, UTF_8);
            assertFalse(string.isEmpty());
        }
    }

    @Test(dataProvider = "dataFilesTestData")
    public void testDataSyntax(String path) throws IOException {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(path)) {
            assertNotNull(in);
            Model model = ModelFactory.createDefaultModel();
            RDFDataMgr.read(model, in, Lang.NT);
            assertFalse(model.isEmpty());
        }
    }

    @DataProvider
    public static @Nonnull Object[][] queryNameData() {
        return QUERY_FILENAMES.stream().map(n -> new Object[]{n})
                                       .toArray(Object[][]::new);
    }

    @Test(dataProvider = "queryNameData", groups = {"fast"})
    public void testParseQueries(String queryName) throws Exception {
        String path = RESOURCE_DIR + "/queries/" + queryName;
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream(path)) {
            assertNotNull(stream);
            Op query = SPARQLParser.tolerant().parse(stream);
            assertFalse(query.getMatchedTriples().isEmpty());
        }
    }

    @Test(dataProvider = "queryNameData")
    public void testRunRawQueries(String queryName) throws Exception {
        assertNotNull(allData);
        String queryPath = RESOURCE_DIR + "/queries/" + queryName;
        ClassLoader classLoader = getClass().getClassLoader();
        try (InputStream stream = classLoader.getResourceAsStream(queryPath)) {
            assertNotNull(stream);
            CollectionResults expected = loadResults(queryName);

            String sparql = IOUtils.toString(stream, UTF_8);
            List<Solution> actualList = new ArrayList<>();
            try (QueryExecution exec = QueryExecutionFactory.create(sparql, allData)) {
                ResultSet rs = exec.execSelect();
                JenaSolution.Factory factory = new JenaSolution.Factory(rs.getResultVars());
                while (rs.hasNext())
                    actualList.add(factory.transform(rs.next()));
            }

            assertTrue(actualList.containsAll(expected.getCollection()), "Missing solutions");
            assertTrue(expected.getCollection().containsAll(actualList), "Unexpected solutions");
        }
    }

    @Test(dataProvider = "queryNameData", groups = {"fast"})
    public void testRunQueries(String queryName) throws Exception {
        assertNotNull(allData);
        String queryPath = RESOURCE_DIR + "/queries/" + queryName;
        ClassLoader classLoader = getClass().getClassLoader();
        try (InputStream stream = classLoader.getResourceAsStream(queryPath)) {
            assertNotNull(stream);
            CollectionResults expected = loadResults(queryName);

            Op root = SPARQLParser.tolerant().parse(stream);
            ARQEndpoint ep = ARQEndpoint.forModel(allData);
            Results actual;
            if (root instanceof QueryOp) {
                actual = ep.query(((QueryOp) root).getQuery());
            } else {
                try (Federation federation = createFederation(ep.asSource())) {
                    actual = federation.query(root);
                }
            }
            List<Solution> actualList = new ArrayList<>();
            actual.forEachRemainingThenClose(actualList::add);
            assertTrue(actualList.containsAll(expected.getCollection()), "Missing solutions");
            assertTrue(expected.getCollection().containsAll(actualList), "Unexpected solutions");
        }
    }

    @Test(dataProvider = "queryNameData")
    public void testRunQueriesWithSPARQLClient(String queryName) throws Exception {
        assertNotNull(allData);
        String queryPath = RESOURCE_DIR + "/queries/" + queryName;
        ClassLoader classLoader = getClass().getClassLoader();

        try (InputStream stream = classLoader.getResourceAsStream(queryPath);
             SPARQLClient client = createSPARQLClient()) {

            assertNotNull(stream);
            CollectionResults expected = loadResults(queryName);

            Op root = SPARQLParser.tolerant().parse(stream);
            Results actual;
            if (root instanceof QueryOp) {
                actual = client.query(((QueryOp) root).getQuery());
            } else {
                Source source = new Source(new SelectDescription(client), client);
                try (Federation federation = createFederation(source)) {
                    actual = federation.query(root);
                }
            }
            List<Solution> actualList = new ArrayList<>();
            actual.forEachRemainingThenClose(actualList::add);
            assertTrue(actualList.containsAll(expected.getCollection()), "Missing solutions");
            assertTrue(expected.getCollection().containsAll(actualList), "Unexpected solutions");
        }
    }

}
