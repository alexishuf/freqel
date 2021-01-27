package br.ufsc.lapesd.freqel;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.SingletonSourceFederation;
import br.ufsc.lapesd.freqel.federation.spec.FederationSpecException;
import br.ufsc.lapesd.freqel.federation.spec.FederationSpecLoader;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.jena.query.JenaBindingResults;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.TPEndpointTest;
import br.ufsc.lapesd.freqel.query.endpoint.impl.SPARQLClient;
import br.ufsc.lapesd.freqel.query.modifiers.Limit;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParser;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParserTest;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.CollectionResults;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static br.ufsc.lapesd.freqel.ResultsAssert.assertExpectedResults;
import static br.ufsc.lapesd.freqel.federation.planner.ConjunctivePlannerTest.assertPlanAnswers;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

public class BSBMSelfTest {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(BSBMSelfTest.class);
    private static @Nullable Model allDataModel = null;
    private @Nullable TPEndpointTest.FusekiEndpoint fuseki = null;
    private @Nullable SPARQLClient sparqlClient = null;

    public static final String RESOURCE_DIR =
            "br/ufsc/lapesd/freqel/bsbm/";
    public static final List<String> DATA_FILENAMES = asList(
            "Offer.ttl",
            "Person.ttl",
            "Producer.ttl",
            "Product.ttl",
            "ProductFeature.ttl",
            "ProductType.ttl",
            "Review.ttl",
            "Vendor.ttl"
    );
    public static final List<String> QUERY_FILENAMES = asList(
            "query1.sparql",
            "query2.sparql",
            "query3.sparql",
            "query4.sparql", /* UNION */
            "query5.sparql",
            "query6.sparql", /* REGEX */
            "query7.sparql",  /* uses Product,Offer,Vendor,Review  & Person */
            "query8.sparql",
            "query10.sparql",
            "query11.sparql" /*UNION & unbound predicate */
    );

    private static @Nonnull InputStream getStream(@Nonnull String path) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        String fullPath = RESOURCE_DIR + "/" + path;
        InputStream stream = cl.getResourceAsStream(fullPath);
        if (stream == null)
            throw new AssertionError("Missing resource file "+fullPath);
        return stream;
    }
    private static @Nonnull Reader getReader(@Nonnull String path) {
        return new InputStreamReader(getStream(path), StandardCharsets.UTF_8);
    }

    public static @Nonnull Op loadQueryWithLimit(@Nonnull String queryName) throws
            IOException, SPARQLParseException {
        try (Reader reader = getReader("queries/" + queryName)) {
            SPARQLParser tolerant = SPARQLParser.tolerant();
            return tolerant.parse(reader);
        }
    }

    public static @Nonnull Op loadQuery(@Nonnull String queryName) throws
            IOException, SPARQLParseException {
        Op query = loadQueryWithLimit(queryName);
        query.modifiers().removeIf(Limit.class::isInstance);
        return query;
    }

    public static @Nullable CQuery loadConjunctiveQuery(@Nonnull String queryName) throws
            IOException, SPARQLParseException {
        Op op = loadQuery(queryName);
        if (op instanceof QueryOp)
            return ((QueryOp) op).getQuery();
        return null;
    }

    public static @Nonnull Model loadData(@Nonnull String fileName) throws IOException {
        try (InputStream stream = getStream("data/" + fileName)) {
            Model model = ModelFactory.createDefaultModel();
            RDFDataMgr.read(model, stream, Lang.TTL);
            return model;
        }
    }

    public static synchronized @Nonnull Model allData() {
        if (allDataModel == null) {
            allDataModel = ModelFactory.createDefaultModel();
            for (String filename : DATA_FILENAMES) {
                try {
                    allDataModel.add(loadData(filename));
                } catch (IOException e) {
                    logger.error("Failed to load BSBM dataset {} from resources!", filename, e);
                    throw new AssertionError(e);
                }
            }
        }
        return allDataModel;
    }

    public static @Nonnull CollectionResults loadResults(@Nonnull String queryName)
            throws IOException {
        String path = "queries/" + queryName;
        String sparql = IOUtils.toString(getReader(path))
                .replaceAll("LIMIT \\d+", "")
                .replaceAll("GROUP BY.*\n", "")
                .replaceAll("OFFSET \\d+", "");
        QueryExecution exec = QueryExecutionFactory.create(sparql, allData());
        return CollectionResults.greedy(new JenaBindingResults(exec.execSelect(), exec));
    }

    @DataProvider
    public static @Nonnull Object[][] queryData() {
        return QUERY_FILENAMES.stream().map(f -> new Object[]{f}).toArray(Object[][]::new);
    }

    @DataProvider
    public static @Nonnull Object[][] datasetData() {
        return DATA_FILENAMES.stream().map(f -> new Object[]{f}).toArray(Object[][]::new);
    }

    @AfterClass
    public void tearDown() {
        if (fuseki != null)
            fuseki.close();
        if (sparqlClient != null)
            sparqlClient.close();
    }

    private @Nonnull SPARQLClient getSPARQLClient() {
        if (fuseki == null)
            fuseki = new TPEndpointTest.FusekiEndpoint(DatasetFactory.wrap(allData()));
        if (sparqlClient == null)
            sparqlClient = new SPARQLClient(fuseki.uri);
        return sparqlClient;
    }

    @Test(groups = {"fast"}, dataProvider = "queryData")
    public void loadAllQueries(String fn) {
        try {
            Op query = loadQueryWithLimit(fn);
            assertNotNull(query, "Got null from loadQuery("+fn+")");
            SPARQLParserTest.assertVarsUniverse(query);
            SPARQLParserTest.assertTripleUniverse(query);
        } catch (IOException | SPARQLParseException e) {
            fail("Failed to load query "+fn, e);
        }
    }

    @Test(groups = {"fast"}, dataProvider = "datasetData")
    public void loadAllDatasets(String filename) {
        try {
            assertNotNull(loadData(filename), "Got null from loadQuery("+filename+")");
        } catch (IOException e) {
            fail("Failed to load dataset "+filename, e);
        }
    }

    @Test(groups = {"fast"}, dataProvider = "queryData")
    public void testRunQueriesWithARQEndpoint(String queryName) throws Exception {
        CQuery query = loadConjunctiveQuery(queryName);
        if (query == null)
            return; //silently skip
        assertExpectedResults(ARQEndpoint.forModel(allData()).query(query), loadResults(queryName));
    }

    @Test(dataProvider = "queryData")
    public void testRunQueriesWithSPARQLClient(String queryName) throws Exception {
        CQuery query = loadConjunctiveQuery(queryName);
        if (query == null)
            return; //silently skip
        assertExpectedResults(getSPARQLClient().query(query), loadResults(queryName));
    }

    @Test(dataProvider = "queryData")
    public void testRunOnSingletonFederation(String queryName) throws Exception {
        Op query = loadQuery(queryName);
        Set<Solution> ac = new HashSet<>(), ex = new HashSet<>();
        ARQEndpoint ep = ARQEndpoint.forModel(allData());
        try (Federation federation = SingletonSourceFederation.createFederation(ep.asSource())) {
            Op plan = federation.plan(query);
            assertPlanAnswers(plan, query);
            federation.execute(plan).forEachRemainingThenClose(ac::add);
            loadResults(queryName).forEachRemainingThenClose(ex::add);
            Set<String> actualVars, expectedVars;
            actualVars = ac.stream().flatMap(s -> s.getVarNames().stream()).collect(toSet());
            expectedVars = ex.stream().flatMap(s -> s.getVarNames().stream()).collect(toSet());
            assertEquals(actualVars, expectedVars, "Observed vars differ");
            assertTrue(ac.stream().allMatch(s -> actualVars.equals(s.getVarNames())),
                       "Not all solutions have the same varNames");

            Map<String, Long> exHist = new TreeMap<>(), acHist = new TreeMap<>();
            ex.stream().flatMap(s -> s.getVarNames().stream()).distinct()
                    .forEach(n -> exHist.put(n, ex.stream().filter(s -> s.get(n) != null).count()));
            ex.stream().flatMap(s -> s.getVarNames().stream()).distinct()
                    .forEach(n -> acHist.put(n, ac.stream().filter(s -> s.get(n) != null).count()));
            assertEquals(acHist, exHist);

            assertEquals(ex.stream().filter(s -> !ac.contains(s)).collect(toSet()),
                         emptySet(), "Missing solutions!");
            assertEquals(ac.stream().filter(s -> !ex.contains(s)).collect(toSet()),
                         emptySet(), "Unexpected solutions!");
            assertEquals(ac, ex);
        }
    }

    public static class CreateBSBMZip {

        private static void mkdir(@Nonnull File dir) throws IOException {
            if (dir.exists())
                FileUtils.deleteDirectory(dir);
            if (!dir.mkdirs()) throw new IOException("Failed to mkdir -p " + dir);
        }

        private static @Nonnull File fillDir() throws IOException, FederationSpecException {
            File dir = Files.createTempDirectory("freqel").toFile();
            File queriesDir = new File(dir, "queries");
            mkdir(dir);
            mkdir(new File(dir, "cache"));
            mkdir(queriesDir);

            List<TPEndpointTest.FusekiEndpoint> eps = new ArrayList<>();
            try {
                for (String filename : DATA_FILENAMES) {
                    Dataset ds = DatasetFactory.create(loadData(filename));
                    eps.add(new TPEndpointTest.FusekiEndpoint(ds));
                }
                File config = new File(dir, "config.yaml");
                try (PrintStream out = new PrintStream(new FileOutputStream(config))) {
                    out.println("sources-cache: cache");
                    out.println("sources:");
                    for (TPEndpointTest.FusekiEndpoint ep : eps) {
                        out.println("  - loader: sparql");
                        out.println("    uri: " + ep.uri);
                    }
                }
                for (String filename : QUERY_FILENAMES) {
                    try (Reader in = getReader("queries/" + filename);
                         FileOutputStream out = new FileOutputStream(new File(queriesDir, filename))) {
                        IOUtils.copy(in, out, StandardCharsets.UTF_8);
                    }
                }
                try (Federation federation = new FederationSpecLoader().load(config)) {
                    if (!federation.initAllSources(5, TimeUnit.MINUTES))
                        throw new RuntimeException("Timeout on initAllSources()");
                }
            } finally {
                for (TPEndpointTest.FusekiEndpoint ep : eps)
                    ep.close();
            }
            return dir;
        }

        public static void main(String[] args) throws IOException, FederationSpecException {
            File dir = fillDir();
            File dst = new File(args.length > 0 ? args[0] : "/tmp/bsbm.zip");
            try (ZipOutputStream out = new ZipOutputStream(new FileOutputStream(dst))) {
                Path dirPath = dir.toPath();
                for (Iterator<Path> it = Files.walk(dirPath).iterator(); it.hasNext(); ) {
                    Path p = it.next();
                    String relativePath = dirPath.relativize(p).toString();
                    File inFile = dirPath.resolve(p).toAbsolutePath().toFile();
                    if (inFile.isFile()) {
                        out.putNextEntry(new ZipEntry(relativePath));
                        try (FileInputStream in = new FileInputStream(inFile)) {
                            IOUtils.copy(in, out);
                        }
                    }
                }
            }
            System.out.println("Wrote to "+dst.getAbsolutePath());
        }
    }
}
