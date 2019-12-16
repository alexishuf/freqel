package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static br.ufsc.lapesd.riefederator.model.term.std.StdLit.fromUnescaped;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;

public class SelectDescriptionTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    public static final @Nonnull StdURI CHARLIE = new StdURI("http://example.org/Charlie");
    public static final @Nonnull StdURI DAVE = new StdURI("http://example.org/Dave");
    public static final @Nonnull StdURI TYPE = new StdURI(RDF.type.getURI());
    public static final @Nonnull StdURI KNOWS = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI AGE = new StdURI(FOAF.age.getURI());
    public static final @Nonnull StdURI NAME = new StdURI(FOAF.name.getURI());
    public static final @Nonnull StdURI PRIMARY_TOPIC = new StdURI(FOAF.primaryTopic.getURI());
    public static final @Nonnull StdURI PERSON = new StdURI(FOAF.Person.getURI());
    public static final @Nonnull StdURI DOCUMENT = new StdURI(FOAF.Document.getURI());
    public static final @Nonnull StdURI INT = new StdURI(XSDDatatype.XSDint.getURI());
    public static final @Nonnull StdLit A_AGE = fromUnescaped("23", INT);
    public static final @Nonnull StdLit A_NAME = fromUnescaped("alice", "en");
    public static final @Nonnull StdVar X = new StdVar("X");
    public static final @Nonnull StdVar S = new StdVar("S");
    public static final @Nonnull StdVar P = new StdVar("P");
    public static final @Nonnull StdVar O = new StdVar("O");

    ARQEndpoint rdf1;

    /* ~~~  data methods ~~~ */

    @DataProvider
    public static Object[][] matchData() {
        return new Object[][] {
                new Object[]{true, singletonList(new Triple(S, NAME, O)),
                                   singletonList(new Triple(S, NAME, O))},
                new Object[]{true, singletonList(new Triple(S, TYPE, PERSON)),
                                   singletonList(new Triple(S, TYPE, PERSON))},
                new Object[]{true, singletonList(new Triple(ALICE, KNOWS, BOB)),
                                   singletonList(new Triple(ALICE, KNOWS, BOB))},
                // ok: not in rdf-1.nt, but matches predicate
                new Object[]{true, singletonList(new Triple(ALICE, KNOWS, ALICE)),
                                   singletonList(new Triple(ALICE, KNOWS, ALICE))},
                new Object[]{true, singletonList(new Triple(ALICE, AGE, A_AGE)),
                                   singletonList(new Triple(ALICE, AGE, A_AGE))},
                new Object[]{true, singletonList(new Triple(ALICE, AGE, A_AGE)),
                                   singletonList(new Triple(ALICE, AGE, A_AGE))},
                // fail: bad predicate
                new Object[]{true, singletonList(new Triple(S, PRIMARY_TOPIC, ALICE)), emptyList()},
                // fail: bad predicate
                new Object[]{true, singletonList(new Triple(S, PRIMARY_TOPIC, O)), emptyList()},
                // fail: still bad predicate without classes
                new Object[]{false, singletonList(new Triple(S, PRIMARY_TOPIC, O)), emptyList()},
                // ok: predicate is a variable
                new Object[]{true, singletonList(new Triple(S, P, O)),
                                   singletonList(new Triple(S, P, O))},
                // still ok without classes
                new Object[]{false, singletonList(new Triple(S, P, O)),
                                    singletonList(new Triple(S, P, O))},
                // ok: predicate is a variable
                new Object[]{true, singletonList(new Triple(ALICE, P, ALICE)),
                                   singletonList(new Triple(ALICE, P, ALICE))},
                // ok: predicate is a variable
                new Object[]{true, singletonList(new Triple(PRIMARY_TOPIC, P, ALICE)),
                                   singletonList(new Triple(PRIMARY_TOPIC, P, ALICE))},
                // fail bcs classes were collected and there is no Document instance
                new Object[]{true, singletonList(new Triple(S, TYPE, DOCUMENT)), emptyList()},
                // ok without classes data
                new Object[]{false, singletonList(new Triple(S, TYPE, DOCUMENT)),
                                    singletonList(new Triple(S, TYPE, DOCUMENT))},
                // both match (var predicate and known predicate)
                new Object[]{true, asList(new Triple(S, P, O), new Triple(ALICE, KNOWS, O)),
                                   asList(new Triple(S, P, O), new Triple(ALICE, KNOWS, O))},
                // partial match bcs PRIMARY_TOPIC is not matched
                new Object[]{true, asList(new Triple(S, PRIMARY_TOPIC, O), new Triple(ALICE, KNOWS, O)),
                                   singletonList(new Triple(ALICE, KNOWS, O))},
                // partial match bcs PRIMARY_TOPIC is not matched (reverse query order)
                new Object[]{true, asList(new Triple(ALICE, KNOWS, O), new Triple(S, PRIMARY_TOPIC, O)),
                                   singletonList(new Triple(ALICE, KNOWS, O))},
                // partial match bcs class-elimination
                new Object[]{true, asList(new Triple(S, KNOWS, O), new Triple(S, TYPE, DOCUMENT)),
                                   singletonList(new Triple(S, KNOWS, O))},
                // no evidence for class-elimination
                new Object[]{true, asList(new Triple(S, KNOWS, O), new Triple(S, KNOWS, DOCUMENT)),
                                   asList(new Triple(S, KNOWS, O), new Triple(S, KNOWS, DOCUMENT))},
                // full match without classes data
                new Object[]{false, asList(new Triple(S, KNOWS, O), new Triple(S, TYPE, DOCUMENT)),
                                    asList(new Triple(S, KNOWS, O), new Triple(S, TYPE, DOCUMENT))},
        };
    }

    /* ~~~  setUp/tearDown ~~~ */

    @BeforeClass
    public void setUp() {
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, getClass().getResourceAsStream("../rdf-1.nt"), Lang.TTL);
        rdf1 = ARQEndpoint.forModel(model);
    }


    /* ~~~  test methods ~~~ */

    @Test(dataProvider = "matchData")
    public void testMatch(boolean fetchClasses, @Nonnull List<Triple> query,
                          @Nonnull List<Triple> expected) {
        SelectDescription description = new SelectDescription(rdf1, fetchClasses);

        CQueryMatch match = description.match(CQuery.from(query));
        assertEquals(match.getQuery(), CQuery.from(query));
        assertEquals(new HashSet<>(match.getNonExclusiveRelevant()), new HashSet<>(expected));
        assertEquals(match.getKnownExclusiveGroups(), Collections.emptySet());
    }
}