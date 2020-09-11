package br.ufsc.lapesd.riefederator;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.federation.Federation;
import br.ufsc.lapesd.riefederator.federation.SingletonSourceFederation;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.jena.query.JenaBindingResults;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.TPEndpointTest;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.SPARQLClient;
import br.ufsc.lapesd.riefederator.query.modifiers.Limit;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParseException;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParser;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import org.apache.commons.io.IOUtils;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static br.ufsc.lapesd.riefederator.federation.planner.ConjunctivePlannerTest.assertPlanAnswers;
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
            "br/ufsc/lapesd/riefederator/bsbm/";
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
            assertNotNull(loadQueryWithLimit(fn), "Got null from loadQuery("+fn+")");
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
        Set<Solution> actual = new HashSet<>(), expected = new HashSet<>();
        ARQEndpoint.forModel(allData()).query(query).forEachRemainingThenClose(actual::add);
        loadResults(queryName).forEachRemainingThenClose(expected::add);
        assertEquals(actual, expected);
    }

    @Test(dataProvider = "queryData")
    public void testRunQueriesWithSPARQLClient(String queryName) throws Exception {
        CQuery query = loadConjunctiveQuery(queryName);
        if (query == null)
            return; //silently skip
        Set<Solution> actual = new HashSet<>(), expected = new HashSet<>();
        getSPARQLClient().query(query).forEachRemainingThenClose(actual::add);
        loadResults(queryName).forEachRemainingThenClose(expected::add);
        assertEquals(actual, expected);
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
}
