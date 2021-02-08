package br.ufsc.lapesd.freqel.description;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.description.semantic.SemanticSelectDescription;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceCache;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.jena.query.modifiers.filter.JenaSPARQLFilter;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.reason.tbox.EmptyTBox;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.Transactional;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static br.ufsc.lapesd.freqel.description.MatchReasoning.NONE;
import static br.ufsc.lapesd.freqel.model.term.std.StdLit.fromUnescaped;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.testng.Assert.*;

public class SelectDescriptionTest implements TestContext {
    public static final @Nonnull StdLit A_AGE = fromUnescaped("23", xsdInt);

    private static class CountedARQEndpoint extends ARQEndpoint {
        public int queries = 0;

        protected CountedARQEndpoint(@Nullable String name,
                                     @Nonnull Function<Query, QueryExecution> executionFactory,
                                     @Nullable Transactional transactional,
                                     @Nonnull Runnable closer, boolean local) {
            super(name, executionFactory, transactional, closer, local);
        }

        @Override
        public @Nonnull  Results doQuery(@Nonnull Query query, boolean isAsk,
                                         @Nonnull Set<String> vars) {
            ++queries;
            return super.doQuery(query, isAsk, vars);
        }
    }

    CountedARQEndpoint rdf1;

    /* ~~~  data methods ~~~ */

    @DataProvider
    public static Object[][] matchData() {
        List<List<Object>> baseRows = Arrays.asList(
                Arrays.asList(true, singletonList(new Triple(s, name, o)),
                                   singletonList(new Triple(s, name, o))),
                Arrays.asList(true, singletonList(new Triple(s, type, Person)),
                                   singletonList(new Triple(s, type, Person))),
                Arrays.asList(true, singletonList(new Triple(Alice, knows, Bob)),
                                   singletonList(new Triple(Alice, knows, Bob))),
                // ok: not in rdf-1.nt, but matches predicate
                Arrays.asList(true, singletonList(new Triple(Alice, knows, Alice)),
                                   singletonList(new Triple(Alice, knows, Alice))),
                Arrays.asList(true, singletonList(new Triple(Alice, age, A_AGE)),
                                   singletonList(new Triple(Alice, age, A_AGE))),
                Arrays.asList(true, singletonList(new Triple(Alice, age, A_AGE)),
                                   singletonList(new Triple(Alice, age, A_AGE))),
                // fail: bad predicate
                Arrays.asList(true, singletonList(new Triple(s, primaryTopic, Alice)), emptyList()),
                // fail: bad predicate
                Arrays.asList(true, singletonList(new Triple(s, primaryTopic, o)), emptyList()),
                // fail: still bad predicate without classes
                Arrays.asList(false, singletonList(new Triple(s, primaryTopic, o)), emptyList()),
                // ok: predicate is a variable
                Arrays.asList(true, singletonList(new Triple(s, p, o)),
                                   singletonList(new Triple(s, p, o))),
                // still ok without classes
                Arrays.asList(false, singletonList(new Triple(s, p, o)),
                                    singletonList(new Triple(s, p, o))),
                // ok: predicate is a variable
                Arrays.asList(true, singletonList(new Triple(Alice, p, Alice)),
                                   singletonList(new Triple(Alice, p, Alice))),
                // ok: predicate is a variable
                Arrays.asList(true, singletonList(new Triple(primaryTopic, p, Alice)),
                                   singletonList(new Triple(primaryTopic, p, Alice))),
                // fail bcs classes were collected and there is no Document instance
                Arrays.asList(true, singletonList(new Triple(s, type, Document)), emptyList()),
                // ok without classes data
                Arrays.asList(false, singletonList(new Triple(s, type, Document)),
                                    singletonList(new Triple(s, type, Document))),
                // both match (var predicate and known predicate)
                Arrays.asList(true, asList(new Triple(s, p, o), new Triple(Alice, knows, o)),
                                   asList(new Triple(s, p, o), new Triple(Alice, knows, o))),
                // partial match bcs PRIMARY_TOPIC is not matched
                Arrays.asList(true, asList(new Triple(s, primaryTopic, o), new Triple(Alice, knows, o)),
                                   singletonList(new Triple(Alice, knows, o))),
                // partial match bcs PRIMARY_TOPIC is not matched (reverse query order)
                Arrays.asList(true, asList(new Triple(Alice, knows, o), new Triple(s, primaryTopic, o)),
                                   singletonList(new Triple(Alice, knows, o))),
                // partial match bcs class-elimination
                Arrays.asList(true, asList(new Triple(s, knows, o), new Triple(s, type, Document)),
                                   singletonList(new Triple(s, knows, o))),
                // no evidence for class-elimination
                Arrays.asList(true, asList(new Triple(s, knows, o), new Triple(s, knows, Document)),
                                   asList(new Triple(s, knows, o), new Triple(s, knows, Document))),
                // full match without classes data
                Arrays.asList(false, asList(new Triple(s, knows, o), new Triple(s, type, Document)),
                                    asList(new Triple(s, knows, o), new Triple(s, type, Document)))
        );
        List<List<Object>> rows = new ArrayList<>(baseRows.size()*2);
        for (List<Object> baseRow : baseRows) {
            ArrayList<Object> row = new ArrayList<>(baseRow);
            row.add(0, false);
            rows.add(new ArrayList<>(row));
            row.set(0, true);
            rows.add(row);
        }
        return rows.stream().map(List::toArray).toArray(Object[][]::new);
    }

    /* ~~~  setUp/tearDown ~~~ */

    @BeforeClass(groups = {"fast"})
    public void setUp() {
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, getClass().getResourceAsStream("../rdf-1.nt"), Lang.TTL);
        rdf1 = new CountedARQEndpoint("rdf-1.nt",
                q -> QueryExecutionFactory.create(q, model), null,
                () -> {}, true);
    }

    /* ~~~  test methods ~~~ */

    @Test(dataProvider = "matchData", groups = {"fast"})
    public void testMatch(boolean semantic, boolean fetchClasses, @Nonnull List<Triple> query,
                          @Nonnull List<Triple> expected) {
        SelectDescription description = createDescription(semantic, fetchClasses);

        CQueryMatch match = description.match(CQuery.from(query), NONE);
        assertEquals(new HashSet<>(match.getNonExclusiveRelevant()), new HashSet<>(expected));
        assertEquals(match.getKnownExclusiveGroups(), Collections.emptySet());
    }

    @Nonnull
    private SelectDescription createDescription(boolean semantic, boolean fetchClasses) {
        return semantic
                ? new SemanticSelectDescription(rdf1, fetchClasses, new EmptyTBox())
                : new SelectDescription(rdf1, fetchClasses);
    }

    @DataProvider
    public static @Nonnull Object[][] constructorData() {
        return Lists.cartesianProduct(asList(false, true), asList(false, true))
                    .stream().map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "constructorData", groups = {"fast"})
    public void testMatchWithFilter(boolean fetchClasses, boolean semantic) {
        CQuery query = createQuery(
                Alice, knows, x,
                x, age, TestContext.y,
                JenaSPARQLFilter.build("?y >= 23"));
        SelectDescription description = createDescription(semantic, fetchClasses);
        CQueryMatch match = description.match(query, NONE);
        assertEquals(match.getKnownExclusiveGroups(), emptyList());
        assertEquals(new HashSet<>(match.getNonExclusiveRelevant()), query.attr().getSet());
        assertEquals(match.getUnknown(), emptySet());

        CQueryMatch localMatch = description.localMatch(query, NONE);
        assertNotNull(localMatch);
        assertEquals(localMatch.getAllRelevant(), match.getAllRelevant());
        assertEquals(localMatch.getNonExclusiveRelevant(), match.getNonExclusiveRelevant());
        assertEquals(localMatch.getKnownExclusiveGroups(), match.getKnownExclusiveGroups());
        assertEquals(localMatch.getUnknown(), emptySet());
    }

    @Test(dataProvider = "constructorData", groups = {"fast"})
    public void testSaveAndLoadDescription(boolean fetchClasses,
                                           boolean semantic) throws IOException {
        File dir = Files.createTempDirectory("freqel").toFile();
        try {
            String uri = "http://rdf1.example.org/sparql";
            SourceCache cache = new SourceCache(dir);
            rdf1.queries = 0;
            SelectDescription d1 = createDescription(semantic, fetchClasses);

            d1.saveWhenReady(cache, uri);
            rdf1.queries = 0;
            d1.init();
            assertTrue(d1.waitForInit(10000));
            assertEquals(rdf1.queries, fetchClasses ? 2 : 1);

            rdf1.queries = 0;
            SelectDescription d2 = SelectDescription.fromCache(rdf1, cache, uri);
            assertNotNull(d2);
            assertTrue(d2.waitForInit(0)); //already initialized
            assertEquals(rdf1.queries, 0); //no querying

            // both answer match() correctly
            CQuery q = createQuery(s, name, o);
            assertEquals(d2.match(q, NONE).getNonExclusiveRelevant(), q.asList());
            assertEquals(d1.match(q, NONE).getNonExclusiveRelevant(), q.asList());

            CQueryMatch localMatch = d2.localMatch(q, NONE);
            assertNotNull(localMatch);
            assertEquals(localMatch.getNonExclusiveRelevant(), q.asList());
            assertEquals(localMatch.getUnknown(), emptySet());
            localMatch = d1.localMatch(q, NONE);
            assertNotNull(localMatch);
            assertEquals(localMatch.getNonExclusiveRelevant(), q.asList());
            assertEquals(localMatch.getUnknown(), emptySet());
        } finally {
            FileUtils.deleteDirectory(dir);
        }
    }

    @Test(dataProvider = "constructorData")
    public void testParallelSaveAndLoadDescription(boolean fetchClasses,
                                                   boolean semantic) throws Exception {
        for (int run = 0; run < 10; run++) {
            ExecutorService outer = Executors.newCachedThreadPool();
            File dir = Files.createTempDirectory("freqel").toFile();
            SourceCache cache = new SourceCache(dir);
            List<Future<?>> futures = new ArrayList<>();
            try {
                for (int i = 0; i < 80; i++) {
                    final int taskId = i;
                    futures.add(outer.submit(() -> {
                        String uri = "http://rdf-" + taskId + ".example.org/sparql";
                        SelectDescription d1 = createDescription(semantic, fetchClasses);

                        d1.saveWhenReady(cache, uri);
                        d1.init();
                        assertTrue(d1.waitForInit(60000));

                        SelectDescription d2 = SelectDescription.fromCache(rdf1, cache, uri);
                        assertNotNull(d2);
                        assertTrue(d2.waitForInit(0)); //already initialized

                        // both answer match() correctly
                        CQuery q = createQuery(s, name, o);
                        assertEquals(d2.match(q, NONE).getNonExclusiveRelevant(), q.asList());
                        assertEquals(d1.match(q, NONE).getNonExclusiveRelevant(), q.asList());
                        return null;
                    }));
                }
                for (Future<?> f : futures)
                    f.get(); //re-throws Exceptions and AssertionErrors
                outer.shutdown();
                assertTrue(outer.awaitTermination(10, TimeUnit.SECONDS));
            } finally {
                FileUtils.deleteDirectory(dir);
            }
        }
    }
}