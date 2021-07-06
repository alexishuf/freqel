package br.ufsc.lapesd.freqel.description;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.jena.query.modifiers.filter.JenaSPARQLFilter;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.apache.jena.query.QueryExecutionFactory.create;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class AskDescriptionTest implements TestContext {
    private Model rdf1;

    @SuppressWarnings("Immutable") //testing purposes
    private static class CountingARQEndpoint extends ARQEndpoint {
        public int calls = 0;

        public CountingARQEndpoint(@Nullable String name,
                                   @Nonnull Function<Query, QueryExecution> executionFactory,
                                   @Nonnull Runnable closer, boolean local) {
            super(name, executionFactory, null, closer, local);
        }

        @Override
        public @Nonnull Results query(@Nonnull CQuery query) {
            ++calls;
            return super.query(query);
        }
    }

    private @Nonnull CountingARQEndpoint createEndpoint() {
        return new CountingARQEndpoint("rdf", sparql -> create(sparql, rdf1),
                                       () -> {}, true);
    }

    @DataProvider
    public static Object[][] matchData() {
        return new Object[][] {
                new Object[] {singletonList(new Triple(Alice, knows, Bob)),
                              singletonList(new Triple(Alice, knows, Bob))},
                new Object[] {singletonList(new Triple(s, p, o)),
                              singletonList(new Triple(s, p, o))},
                new Object[] {singletonList(new Triple(Alice, knows, o)),
                              singletonList(new Triple(Alice, knows, o))},
                new Object[] {singletonList(new Triple(s, knows, Bob)),
                              singletonList(new Triple(s, knows, Bob))},
                new Object[] {singletonList(new Triple(s, knows, o)),
                              singletonList(new Triple(s, knows, o))},
                new Object[] {singletonList(new Triple(Alice, knows, Bob)),
                              singletonList(new Triple(Alice, knows, Bob))},
                // generalized to S KNOWS CHARLIE, but still fails
                new Object[] {singletonList(new Triple(Alice, knows, Charlie)),
                              emptyList()},
                // bad predicate
                new Object[] {singletonList(new Triple(s, primaryTopic, o)), emptyList()},
                // bad predicate
                new Object[] {singletonList(new Triple(Bob, primaryTopic, o)), emptyList()},
                // generalized to S PRIMARY_TOPIC CHARLIE, which then fails the ASK
                new Object[] {singletonList(new Triple(Alice, primaryTopic, Charlie)),
                              emptyList()},
                new Object[] {singletonList(new Triple(Alice, primaryTopic, Charlie)),
                              emptyList()},
                new Object[] {singletonList(new Triple(s, type, Person)),
                              singletonList(new Triple(s, type, Person))},
                // fails bcs query is not generalized since predicate is rdf:type
                new Object[] {singletonList(new Triple(s, type, Document)), emptyList()},

                /* ~~~ try partial matches ~~~ */
                new Object[] {asList(new Triple(Bob, primaryTopic, o), new Triple(s, p, o)),
                              singletonList(new Triple(s, p, o))},
                new Object[] {asList(new Triple(s, p, o), new Triple(Bob, primaryTopic, o)),
                              singletonList(new Triple(s, p, o))},
                new Object[] {asList(new Triple(s, primaryTopic, o), new Triple(s, knows, o)),
                              singletonList(new Triple(s, knows, o))},
                new Object[] {asList(new Triple(s, primaryTopic, o), new Triple(s, knows, o),
                                     new Triple(s, name, Charlie)),
                              singletonList(new Triple(s, knows, o))},
        };
    }

    @BeforeClass
    public void setUp() {
        rdf1 = ModelFactory.createDefaultModel();
        RDFDataMgr.read(rdf1, open("rdf-1.nt"), Lang.TTL);
    }

    @AfterClass
    public void tearDown() {
        rdf1.close();
        rdf1 = null;
    }

    @Test(dataProvider = "matchData")
    public void testMatch(@Nonnull List<Triple> query, @Nonnull List<Triple> expected) {
        AskDescription description = new AskDescription(ARQEndpoint.forModel(rdf1));
        CQueryMatch match = description.match(CQuery.from(query), MatchReasoning.NONE);

        assertEquals(match.getKnownExclusiveGroups(), Collections.emptySet());
        assertEquals(new HashSet<>(match.getNonExclusiveRelevant()), new HashSet<>(expected));
    }

    @Test
    public void testCache() {
        CountingARQEndpoint ep = createEndpoint();
        AskDescription d = new AskDescription(ep);
        CQueryMatch match = d.match(createQuery(Alice, knows, o), MatchReasoning.NONE);
        assertEquals(ep.calls, 1);
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(Alice, knows, o)));

        match = d.match(createQuery(Alice, knows, o), MatchReasoning.NONE);
        assertEquals(ep.calls, 1);
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(Alice, knows, o)));

        match = d.match(createQuery(Alice, knows, x), MatchReasoning.NONE);
        assertEquals(ep.calls, 1); //different var name should not matter
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(Alice, knows, x)));
    }

    @Test
    public void testCacheLearnsGeneralized() {
        CountingARQEndpoint ep = createEndpoint();
        AskDescription d = new AskDescription(ep);
        CQueryMatch match = d.match(createQuery(Alice, knows, Bob), MatchReasoning.NONE);
        assertEquals(ep.calls, 1);
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(Alice, knows, Bob)));

        match = d.match(createQuery(Alice, knows, o), MatchReasoning.NONE);
        assertEquals(ep.calls, 1); //should have learned from previous query
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(Alice, knows, o)));
    }

    @Test
    public void testTriesToFailGeneralized() {
        CountingARQEndpoint ep = createEndpoint();
        AskDescription d = new AskDescription(ep);
        CQueryMatch match = d.match(createQuery(Alice, knows, Charlie), MatchReasoning.NONE);
        assertEquals(ep.calls, 2); //already tries to prove failure of subsequent
        assertTrue(match.isEmpty());

        match = d.match(createQuery(Alice, knows, o), MatchReasoning.NONE);
        assertEquals(ep.calls, 2); // no new ASK
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(Alice, knows, o)));
    }

    @Test
    public void testMatchWithFilter() {
        CountingARQEndpoint ep = createEndpoint();
        AskDescription d = new AskDescription(ep);
        CQuery query = createQuery(x, age,   y,
                                   x, knows, Bob,
                                   JenaSPARQLFilter.build("?y > 23"));
        CQueryMatch match = d.match(query, MatchReasoning.NONE);
        assertEquals(match.getKnownExclusiveGroups(), emptyList());
        assertEquals(new HashSet<>(match.getNonExclusiveRelevant()), query.attr().getSet());
    }

    public void testLocalMatch() {
        CountingARQEndpoint ep = createEndpoint();
        AskDescription d = new AskDescription(ep);

        MutableCQuery q1 = createQuery(Alice, age, lit(23));
        CQueryMatch match1 = d.match(q1, MatchReasoning.NONE);
        assertEquals(match1.getAllRelevant(), q1.attr().getSet());
        CQueryMatch actual = d.localMatch(q1, MatchReasoning.NONE);
        assertNotNull(actual);
        assertEquals(actual.getNonExclusiveRelevant(), match1.getNonExclusiveRelevant());
        assertEquals(actual.getKnownExclusiveGroups(), match1.getKnownExclusiveGroups());

        // matching q1 should have cached a generalized subject version as well
        MutableCQuery genSubjectQ1 = createQuery(x, age, lit(23));
        actual = d.localMatch(genSubjectQ1, MatchReasoning.NONE);
        assertNotNull(actual);
        assertEquals(actual.getAllRelevant(), genSubjectQ1.attr().getSet());

        // matching q1 should have cached a generalized object version as well
        MutableCQuery genObjectQ1 = createQuery(Alice, age, y);
        actual = d.localMatch(genObjectQ1, MatchReasoning.NONE);
        assertNotNull(actual);
        assertEquals(actual.getAllRelevant(), genObjectQ1.attr().getSet());

        // nothing in cache
        actual = d.localMatch(createQuery(x, name, y), MatchReasoning.NONE);
        assertNotNull(actual);
        assertEquals(actual.getAllRelevant(), emptySet());
        assertEquals(actual.getUnknown(), singleton(new Triple(x, name, y)));

        // first triple matches from cache, but second does not
        CQuery partialLocal = createQuery(x, age, lit(23),
                                          x, name, y);
        actual = d.localMatch(partialLocal, MatchReasoning.NONE);
        IndexSubset<Triple> matched, unknown;
        matched = partialLocal.attr().getSet().subset(new Triple(x, age, lit(23)));
        unknown = partialLocal.attr().getSet().subset(new Triple(x, name, y));
        assertEquals(actual.getAllRelevant(), matched);
        assertEquals(actual.getUnknown(), unknown);
    }
}