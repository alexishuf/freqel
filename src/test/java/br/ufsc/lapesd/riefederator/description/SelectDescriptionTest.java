package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static br.ufsc.lapesd.riefederator.model.term.std.StdLit.fromUnescaped;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;

public class SelectDescriptionTest implements TestContext {
    public static final @Nonnull StdLit A_AGE = fromUnescaped("23", xsdInt);

    ARQEndpoint rdf1;

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

    @DataProvider
    public static @Nonnull Object[][] fetchClassesData() {
        return new Object[][] { new Object[]{true}, new Object[]{false} };
    }

    @Test(dataProvider = "fetchClassesData")
    public void testMatchWithFilter(boolean fetchClasses) {
        CQuery query = createQuery(
                Alice, knows, x,
                x, age, TestContext.y,
                SPARQLFilter.build("?y >= 23"));
        SelectDescription description = new SelectDescription(rdf1, fetchClasses);
        CQueryMatch match = description.match(query);
        assertEquals(match.getKnownExclusiveGroups(), emptyList());
        assertEquals(new HashSet<>(match.getNonExclusiveRelevant()), query.getSet());
    }
}