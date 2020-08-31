package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.federation.spec.source.SourceCache;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Results;
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

import static br.ufsc.lapesd.riefederator.model.term.std.StdLit.fromUnescaped;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
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
        return new Object[][] {
                new Object[]{true, singletonList(new Triple(s, name, o)),
                                   singletonList(new Triple(s, name, o))},
                new Object[]{true, singletonList(new Triple(s, type, Person)),
                                   singletonList(new Triple(s, type, Person))},
                new Object[]{true, singletonList(new Triple(Alice, knows, Bob)),
                                   singletonList(new Triple(Alice, knows, Bob))},
                // ok: not in rdf-1.nt, but matches predicate
                new Object[]{true, singletonList(new Triple(Alice, knows, Alice)),
                                   singletonList(new Triple(Alice, knows, Alice))},
                new Object[]{true, singletonList(new Triple(Alice, age, A_AGE)),
                                   singletonList(new Triple(Alice, age, A_AGE))},
                new Object[]{true, singletonList(new Triple(Alice, age, A_AGE)),
                                   singletonList(new Triple(Alice, age, A_AGE))},
                // fail: bad predicate
                new Object[]{true, singletonList(new Triple(s, primaryTopic, Alice)), emptyList()},
                // fail: bad predicate
                new Object[]{true, singletonList(new Triple(s, primaryTopic, o)), emptyList()},
                // fail: still bad predicate without classes
                new Object[]{false, singletonList(new Triple(s, primaryTopic, o)), emptyList()},
                // ok: predicate is a variable
                new Object[]{true, singletonList(new Triple(s, p, o)),
                                   singletonList(new Triple(s, p, o))},
                // still ok without classes
                new Object[]{false, singletonList(new Triple(s, p, o)),
                                    singletonList(new Triple(s, p, o))},
                // ok: predicate is a variable
                new Object[]{true, singletonList(new Triple(Alice, p, Alice)),
                                   singletonList(new Triple(Alice, p, Alice))},
                // ok: predicate is a variable
                new Object[]{true, singletonList(new Triple(primaryTopic, p, Alice)),
                                   singletonList(new Triple(primaryTopic, p, Alice))},
                // fail bcs classes were collected and there is no Document instance
                new Object[]{true, singletonList(new Triple(s, type, Document)), emptyList()},
                // ok without classes data
                new Object[]{false, singletonList(new Triple(s, type, Document)),
                                    singletonList(new Triple(s, type, Document))},
                // both match (var predicate and known predicate)
                new Object[]{true, asList(new Triple(s, p, o), new Triple(Alice, knows, o)),
                                   asList(new Triple(s, p, o), new Triple(Alice, knows, o))},
                // partial match bcs PRIMARY_TOPIC is not matched
                new Object[]{true, asList(new Triple(s, primaryTopic, o), new Triple(Alice, knows, o)),
                                   singletonList(new Triple(Alice, knows, o))},
                // partial match bcs PRIMARY_TOPIC is not matched (reverse query order)
                new Object[]{true, asList(new Triple(Alice, knows, o), new Triple(s, primaryTopic, o)),
                                   singletonList(new Triple(Alice, knows, o))},
                // partial match bcs class-elimination
                new Object[]{true, asList(new Triple(s, knows, o), new Triple(s, type, Document)),
                                   singletonList(new Triple(s, knows, o))},
                // no evidence for class-elimination
                new Object[]{true, asList(new Triple(s, knows, o), new Triple(s, knows, Document)),
                                   asList(new Triple(s, knows, o), new Triple(s, knows, Document))},
                // full match without classes data
                new Object[]{false, asList(new Triple(s, knows, o), new Triple(s, type, Document)),
                                    asList(new Triple(s, knows, o), new Triple(s, type, Document))},
        };
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
    public void testMatch(boolean fetchClasses, @Nonnull List<Triple> query,
                          @Nonnull List<Triple> expected) {
        SelectDescription description = new SelectDescription(rdf1, fetchClasses);

        CQueryMatch match = description.match(CQuery.from(query));
        assertEquals(match.getQuery(), CQuery.from(query));
        assertEquals(new HashSet<>(match.getNonExclusiveRelevant()), new HashSet<>(expected));
        assertEquals(match.getKnownExclusiveGroups(), Collections.emptySet());
    }

    @DataProvider
    public static @Nonnull Object[][] fetchClassesData() {
        return new Object[][] { new Object[]{true}, new Object[]{false} };
    }

    @Test(dataProvider = "fetchClassesData", groups = {"fast"})
    public void testMatchWithFilter(boolean fetchClasses) {
        CQuery query = createQuery(
                Alice, knows, x,
                x, age, TestContext.y,
                SPARQLFilter.build("?y >= 23"));
        SelectDescription description = new SelectDescription(rdf1, fetchClasses);
        CQueryMatch match = description.match(query);
        assertEquals(match.getKnownExclusiveGroups(), emptyList());
        assertEquals(new HashSet<>(match.getNonExclusiveRelevant()), query.attr().getSet());
    }

    @Test(dataProvider = "fetchClassesData", groups = {"fast"})
    public void testSaveAndLoadDescription(boolean fetchClasses) throws IOException {
        File dir = Files.createTempDirectory("riefederator").toFile();
        try {
            String uri = "http://rdf1.example.org/sparql";
            SourceCache cache = new SourceCache(dir);
            rdf1.queries = 0;
            SelectDescription d1 = new SelectDescription(rdf1, fetchClasses);

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
            assertEquals(d2.match(q).getNonExclusiveRelevant(), q.asList());
            assertEquals(d1.match(q).getNonExclusiveRelevant(), q.asList());
        } finally {
            FileUtils.deleteDirectory(dir);
        }
    }

    @Test(dataProvider = "fetchClassesData")
    public void testParallelSaveAndLoadDescription(boolean fetchClasses) throws Exception {
        for (int run = 0; run < 10; run++) {
            ExecutorService outer = Executors.newCachedThreadPool();
            File dir = Files.createTempDirectory("riefederator").toFile();
            SourceCache cache = new SourceCache(dir);
            List<Future<?>> futures = new ArrayList<>();
            try {
                for (int i = 0; i < 80; i++) {
                    final int taskId = i;
                    futures.add(outer.submit(() -> {
                        String uri = "http://rdf-" + taskId + ".example.org/sparql";
                        SelectDescription d1 = new SelectDescription(rdf1, fetchClasses);

                        d1.saveWhenReady(cache, uri);
                        d1.init();
                        assertTrue(d1.waitForInit(60000));

                        SelectDescription d2 = SelectDescription.fromCache(rdf1, cache, uri);
                        assertNotNull(d2);
                        assertTrue(d2.waitForInit(0)); //already initialized

                        // both answer match() correctly
                        CQuery q = createQuery(s, name, o);
                        assertEquals(d2.match(q).getNonExclusiveRelevant(), q.asList());
                        assertEquals(d1.match(q).getNonExclusiveRelevant(), q.asList());
                        return null;
                    }));
                }
                for (Future<?> f : futures)
                    f.get(); //re-throws Exceptions and AssertionErrors
                outer.shutdown();
                outer.awaitTermination(10, TimeUnit.SECONDS);
            } finally {
                FileUtils.deleteDirectory(dir);
            }
        }
    }
}