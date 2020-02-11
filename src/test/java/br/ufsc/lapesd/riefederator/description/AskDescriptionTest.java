package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Results;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;
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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.jena.query.QueryExecutionFactory.create;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AskDescriptionTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    public static final @Nonnull StdURI CHARLIE = new StdURI("http://example.org/Charlie");
    public static final @Nonnull StdURI TYPE = new StdURI(RDF.type.getURI());
    public static final @Nonnull StdURI KNOWS = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI NAME = new StdURI(FOAF.name.getURI());
    public static final @Nonnull StdURI PRIMARY_TOPIC = new StdURI(FOAF.primaryTopic.getURI());
    public static final @Nonnull StdURI PERSON = new StdURI(FOAF.Person.getURI());
    public static final @Nonnull StdURI DOCUMENT = new StdURI(FOAF.Document.getURI());
    public static final @Nonnull StdURI INT = new StdURI(XSDDatatype.XSDint.getURI());
    public static final @Nonnull StdVar X = new StdVar("X");
    public static final @Nonnull StdVar S = new StdVar("S");
    public static final @Nonnull StdVar P = new StdVar("P");
    public static final @Nonnull StdVar O = new StdVar("O");

    private Model rdf1;

    @SuppressWarnings("Immutable") //testing purposes
    private static class CountingARQEndpoint extends ARQEndpoint {
        public int calls = 0;

        public CountingARQEndpoint(@Nullable String name,
                                   @Nonnull Function<String, QueryExecution> executionFactory,
                                   @Nonnull Runnable closer, boolean local) {
            super(name, executionFactory, closer, local);
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
                new Object[] {singletonList(new Triple(ALICE, KNOWS, BOB)),
                              singletonList(new Triple(ALICE, KNOWS, BOB))},
                new Object[] {singletonList(new Triple(S, P, O)),
                              singletonList(new Triple(S, P, O))},
                new Object[] {singletonList(new Triple(ALICE, KNOWS, O)),
                              singletonList(new Triple(ALICE, KNOWS, O))},
                new Object[] {singletonList(new Triple(S, KNOWS, BOB)),
                              singletonList(new Triple(S, KNOWS, BOB))},
                new Object[] {singletonList(new Triple(S, KNOWS, O)),
                              singletonList(new Triple(S, KNOWS, O))},
                new Object[] {singletonList(new Triple(ALICE, KNOWS, BOB)),
                              singletonList(new Triple(ALICE, KNOWS, BOB))},
                // generalized to S KNOWS CHARLIE, but still fails
                new Object[] {singletonList(new Triple(ALICE, KNOWS, CHARLIE)),
                              emptyList()},
                // bad predicate
                new Object[] {singletonList(new Triple(S, PRIMARY_TOPIC, O)), emptyList()},
                // bad predicate
                new Object[] {singletonList(new Triple(BOB, PRIMARY_TOPIC, O)), emptyList()},
                // generalized to S PRIMARY_TOPIC CHARLIE, which then fails the ASK
                new Object[] {singletonList(new Triple(ALICE, PRIMARY_TOPIC, CHARLIE)),
                              emptyList()},
                new Object[] {singletonList(new Triple(ALICE, PRIMARY_TOPIC, CHARLIE)),
                              emptyList()},
                new Object[] {singletonList(new Triple(S, TYPE, PERSON)),
                              singletonList(new Triple(S, TYPE, PERSON))},
                // fails bcs query is not generalized since predicate is rdf:type
                new Object[] {singletonList(new Triple(S, TYPE, DOCUMENT)), emptyList()},

                /* ~~~ try partial matches ~~~ */
                new Object[] {asList(new Triple(BOB, PRIMARY_TOPIC, O), new Triple(S, P, O)),
                              singletonList(new Triple(S, P, O))},
                new Object[] {asList(new Triple(S, P, O), new Triple(BOB, PRIMARY_TOPIC, O)),
                              singletonList(new Triple(S, P, O))},
                new Object[] {asList(new Triple(S, PRIMARY_TOPIC, O), new Triple(S, KNOWS, O)),
                              singletonList(new Triple(S, KNOWS, O))},
                new Object[] {asList(new Triple(S, PRIMARY_TOPIC, O), new Triple(S, KNOWS, O),
                                     new Triple(S, NAME, CHARLIE)),
                              singletonList(new Triple(S, KNOWS, O))},
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
        CQueryMatch match = d.match(CQuery.from(singletonList(new Triple(ALICE, KNOWS, O))));
        assertEquals(ep.calls, 1);
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(ALICE, KNOWS, O)));

        match = d.match(CQuery.from(singletonList(new Triple(ALICE, KNOWS, O))));
        assertEquals(ep.calls, 1);
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(ALICE, KNOWS, O)));

        match = d.match(CQuery.from(singletonList(new Triple(ALICE, KNOWS, X))));
        assertEquals(ep.calls, 1); //different var name should not matter
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(ALICE, KNOWS, X)));
    }

    @Test
    public void testCacheLearnsGeneralized() {
        CountingARQEndpoint ep = createEndpoint();
        AskDescription d = new AskDescription(ep);
        CQueryMatch match = d.match(CQuery.from(singletonList(new Triple(ALICE, KNOWS, BOB))));
        assertEquals(ep.calls, 1);
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(ALICE, KNOWS, BOB)));

        match = d.match(CQuery.from(singletonList(new Triple(ALICE, KNOWS, O))));
        assertEquals(ep.calls, 1); //should have learned from previous query
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(ALICE, KNOWS, O)));
    }

    @Test
    public void testTriesToFailGeneralized() {
        CountingARQEndpoint ep = createEndpoint();
        AskDescription d = new AskDescription(ep);
        CQueryMatch match = d.match(CQuery.from(singletonList(new Triple(ALICE, KNOWS, CHARLIE))));
        assertEquals(ep.calls, 2); //already tries to prove failure of subsequent
        assertTrue(match.isEmpty());

        match = d.match(CQuery.from(singletonList(new Triple(ALICE, KNOWS, O))));
        assertEquals(ep.calls, 2); // no new ASK
        assertEquals(match.getNonExclusiveRelevant(), singletonList(new Triple(ALICE, KNOWS, O)));
    }
}