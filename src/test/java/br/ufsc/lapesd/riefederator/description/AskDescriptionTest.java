package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Results;
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

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.jena.query.QueryExecutionFactory.create;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AskDescriptionTest implements TestContext {
    private Model rdf1;

    @SuppressWarnings("Immutable") //testing purposes
    private static class CountingARQEndpoint extends ARQEndpoint {
        public int calls = 0;

        public CountingARQEndpoint(@Nullable String name,
                                   @Nonnull Function<String, QueryExecution> executionFactory,
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
        RDFDataMgr.read(rdf1, getClass().getResourceAsStream("../rdf-1.nt"), Lang.TTL);
    }

    @AfterClass
    public void tearDown() {
        rdf1.close();
        rdf1 = null;
    }

    @Test(dataProvider = "matchData")
    public void testMatch(@Nonnull List<Triple> query, @Nonnull List<Triple> expected) {
        AskDescription description = new AskDescription(ARQEndpoint.forModel(rdf1));
        CQueryMatch match = description.match(CQuery.from(query));

        assertEquals(match.getKnownExclusiveGroups(), Collections.emptySet());
        assertEquals(new HashSet<>(match.getNonExclusiveRelevant()), new HashSet<>(expected));
    }

    @Test
    public void testCache() {
        CountingARQEndpoint ep = createEndpoint();
        AskDescription d = new AskDescription(ep);
        CQueryMatch match = d.match(CQuery.from(singletonList(new Triple(Alice, knows, o))));
        assertEquals(ep.calls, 1);
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(Alice, knows, o)));

        match = d.match(CQuery.from(singletonList(new Triple(Alice, knows, o))));
        assertEquals(ep.calls, 1);
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(Alice, knows, o)));

        match = d.match(CQuery.from(singletonList(new Triple(Alice, knows, x))));
        assertEquals(ep.calls, 1); //different var name should not matter
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(Alice, knows, x)));
    }

    @Test
    public void testCacheLearnsGeneralized() {
        CountingARQEndpoint ep = createEndpoint();
        AskDescription d = new AskDescription(ep);
        CQueryMatch match = d.match(CQuery.from(singletonList(new Triple(Alice, knows, Bob))));
        assertEquals(ep.calls, 1);
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(Alice, knows, Bob)));

        match = d.match(CQuery.from(singletonList(new Triple(Alice, knows, o))));
        assertEquals(ep.calls, 1); //should have learned from previous query
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(Alice, knows, o)));
    }

    @Test
    public void testTriesToFailGeneralized() {
        CountingARQEndpoint ep = createEndpoint();
        AskDescription d = new AskDescription(ep);
        CQueryMatch match = d.match(CQuery.from(singletonList(new Triple(Alice, knows, Charlie))));
        assertEquals(ep.calls, 2); //already tries to prove failure of subsequent
        assertTrue(match.isEmpty());

        match = d.match(CQuery.from(singletonList(new Triple(Alice, knows, o))));
        assertEquals(ep.calls, 2); // no new ASK
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(Alice, knows, o)));
    }

    @Test
    public void testMatchWithFilter() {
        CountingARQEndpoint ep = createEndpoint();
        AskDescription d = new AskDescription(ep);
        CQuery query = createQuery(x, age,   y,
                                   x, knows, Bob,
                                   SPARQLFilter.build("?y > 23"));
        CQueryMatch match = d.match(query);
        assertEquals(match.getKnownExclusiveGroups(), emptyList());
        assertEquals(new HashSet<>(match.getNonExclusiveRelevant()), query.getSet());
    }
}