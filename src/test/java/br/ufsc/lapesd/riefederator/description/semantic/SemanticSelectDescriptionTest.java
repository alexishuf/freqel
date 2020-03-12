package br.ufsc.lapesd.riefederator.description.semantic;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.MatchAnnotation;
import br.ufsc.lapesd.riefederator.jena.TBoxLoader;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxReasoner;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxReasonerTest;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxSpec;
import com.google.common.collect.Sets;
import org.apache.jena.rdf.model.Model;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import static br.ufsc.lapesd.riefederator.query.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.testng.Assert.assertEquals;

public class SemanticSelectDescriptionTest implements TestContext {
    public static final @Nonnull URI Person = new StdURI("http://example.org/source-onto-1.ttl#Person");
    public static final @Nonnull URI Employee = new StdURI("http://example.org/source-onto-1.ttl#Employee");
    public static final @Nonnull URI Manager = new StdURI("http://example.org/source-onto-1.ttl#Manager");

    public static final @Nonnull URI knows = new StdURI("http://example.org/source-onto-1.ttl#knows");
    public static final @Nonnull URI advises = new StdURI("http://example.org/source-onto-1.ttl#advises");
    public static final @Nonnull URI mentors = new StdURI("http://example.org/source-onto-1.ttl#mentors");
    public static final @Nonnull URI manages = new StdURI("http://example.org/source-onto-1.ttl#manages");


    private ARQEndpoint ep;
    private TBoxSpec tBoxSpec;
    private TBoxReasoner reasoner;

    @BeforeMethod
    public void setUp() {
        Model model = new TBoxLoader().fetchingImports(false)
                                      .addFromResource("source-1.ttl").getModel();
        ep = ARQEndpoint.forModel(model);
        tBoxSpec = new TBoxSpec().addResource(getClass(), "../../source-onto-1.ttl");
    }

    @AfterMethod
    public void tearDown() throws Exception {
        TBoxReasoner reasoner = this.reasoner;
        if (reasoner != null) {
            reasoner.close();
            this.reasoner = null;
        }
    }
    @DataProvider
    public static Object[][] reasonerData() {
        return TBoxReasonerTest.suppliers.stream()
                .filter(n -> !n.getName().equals("StructuralReasoner"))
                .map(s -> new Object[]{s}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "reasonerData")
    public void testAskWithExactPredicate(Supplier<TBoxReasoner> supplier) {
        reasoner = supplier.get();
        reasoner.load(tBoxSpec);
        SemanticSelectDescription d = new SemanticSelectDescription(ep, reasoner);
        Triple qry = new Triple(Alice, manages, Bob);
        SemanticCQueryMatch m = d.semanticMatch(CQuery.from(qry));
        assertEquals(m.getIrrelevant(), emptyList());
        assertEquals(m.getAllRelevant(), singleton(qry));
        assertEquals(m.getKnownExclusiveGroups(), emptySet());
        assertEquals(m.getAlternatives(qry), singleton(CQuery.from(qry)));
        assertEquals(m.getAlternatives(new Triple(Alice, manages, o)), emptySet());
    }

    @Test(dataProvider = "reasonerData")
    public void testVarWithExactPredicates(Supplier<TBoxReasoner> supplier) {
        reasoner = supplier.get();
        reasoner.load(tBoxSpec);
        Triple[] ts = {new Triple(Alice, manages, o), new Triple(o, mentors, Charlie)};
        SemanticSelectDescription d = new SemanticSelectDescription(ep, reasoner);
        SemanticCQueryMatch m = d.semanticMatch(CQuery.from(asList(ts)));
        assertEquals(m.getIrrelevant(), emptyList());
        assertEquals(new HashSet<>(m.getAllRelevant()), Sets.newHashSet(ts));
        assertEquals(m.getKnownExclusiveGroups(), emptySet());

        assertEquals(m.getAlternatives(ts[0]), singleton(CQuery.from(ts[0])));
        assertEquals(m.getAlternatives(ts[1]), singleton(CQuery.from(ts[1])));
        assertEquals(m.getAlternatives(new Triple(Alice, manages, s)), emptySet());
    }

    @Test(dataProvider = "reasonerData")
    public void testSuperPredicateNotUsed(Supplier<TBoxReasoner> supplier) {
        reasoner = supplier.get();
        reasoner.load(tBoxSpec);
        SemanticSelectDescription d = new SemanticSelectDescription(ep, reasoner);
        Triple qry = new Triple(Alice, knows, o);
        SemanticCQueryMatch m = d.semanticMatch(CQuery.from(qry));
        assertEquals(m.getIrrelevant(), emptyList());
        assertEquals(m.getKnownExclusiveGroups(), emptyList());
        assertEquals(m.getAllRelevant(), singletonList(qry));

        Triple qAdvises = new Triple(Alice, advises, o);
        Triple qMentors = new Triple(Alice, mentors, o);
        Triple qManages = new Triple(Alice, manages, o);
        MatchAnnotation annotation = new MatchAnnotation(qry);
        assertEquals(m.getAlternatives(qry),
                Sets.newHashSet(CQuery.with(qAdvises).annotate(qAdvises, annotation).build(),
                                CQuery.with(qMentors).annotate(qMentors, annotation).build(),
                                CQuery.with(qManages).annotate(qManages, annotation).build()));
        assertEquals(m.getAlternatives(CQuery.from(qry)), emptySet());
    }

    @Test(dataProvider = "reasonerData")
    public void testSuperPredicateUsed(Supplier<TBoxReasoner> supplier) {
        reasoner = supplier.get();
        reasoner.load(tBoxSpec);
        SemanticSelectDescription d = new SemanticSelectDescription(ep, reasoner);
        Triple qry = new Triple(s, advises, Charlie);
        SemanticCQueryMatch m = d.semanticMatch(CQuery.from(qry));
        assertEquals(m.getIrrelevant(), emptyList());
        assertEquals(m.getKnownExclusiveGroups(), emptyList());
        assertEquals(m.getAllRelevant(), singletonList(qry));
        assertEquals(m.getNonExclusiveRelevant(), singletonList(qry));

        Triple qMentors = new Triple(s, mentors, Charlie);
        MatchAnnotation annotation = new MatchAnnotation(qry);
        assertEquals(m.getAlternatives(qry),
                Sets.newHashSet(CQuery.from(qry),
                                CQuery.with(qMentors).annotate(qMentors, annotation).build()));
    }


    @Test(dataProvider = "reasonerData")
    public void testVarWithExactClass(Supplier<TBoxReasoner> supplier) {
        reasoner = supplier.get();
        reasoner.load(tBoxSpec);
        SemanticSelectDescription d = new SemanticSelectDescription(ep, true, reasoner);
        Triple qry = new Triple(s, type, Manager);
        SemanticCQueryMatch m = d.semanticMatch(CQuery.from(qry));
        assertEquals(m.getIrrelevant(), emptyList());
        assertEquals(m.getKnownExclusiveGroups(), emptyList());
        assertEquals(m.getAllRelevant(), singletonList(qry));
        assertEquals(m.getNonExclusiveRelevant(), singletonList(qry));

        assertEquals(m.getAlternatives(qry), singleton(CQuery.from(qry)));
    }

    @Test(dataProvider = "reasonerData")
    public void testVarWithSuperClassNotUsed(Supplier<TBoxReasoner> supplier) {
        reasoner = supplier.get();
        reasoner.load(tBoxSpec);
        SemanticSelectDescription d = new SemanticSelectDescription(ep, true, reasoner);
        Triple qry = new Triple(s, type, Person);
        SemanticCQueryMatch m = d.semanticMatch(CQuery.from(qry));
        assertEquals(m.getIrrelevant(), emptyList());
        assertEquals(m.getKnownExclusiveGroups(), emptyList());
        assertEquals(m.getAllRelevant(), singletonList(qry));
        assertEquals(m.getNonExclusiveRelevant(), singletonList(qry));

        Triple qEmployee = new Triple(s, type, Employee);
        Triple qManager = new Triple(s, type, Manager);
        MatchAnnotation ann = new MatchAnnotation(qry);
        assertEquals(m.getAlternatives(qry),
                     Sets.newHashSet(CQuery.with(qEmployee).annotate(qEmployee, ann).build(),
                                     CQuery.with(qManager ).annotate(qManager,  ann).build()));
    }


    @Test(dataProvider = "reasonerData")
    public void testVarWithSuperClassUsed(Supplier<TBoxReasoner> supplier) {
        reasoner = supplier.get();
        reasoner.load(tBoxSpec);
        SemanticSelectDescription d = new SemanticSelectDescription(ep, true, reasoner);
        Triple qry = new Triple(s, type, Employee);
        SemanticCQueryMatch m = d.semanticMatch(CQuery.from(qry));
        assertEquals(m.getIrrelevant(), emptyList());
        assertEquals(m.getKnownExclusiveGroups(), emptyList());
        assertEquals(m.getAllRelevant(), singletonList(qry));
        assertEquals(m.getNonExclusiveRelevant(), singletonList(qry));

        Triple qManager = new Triple(s, type, Manager);
        assertEquals(m.getAlternatives(qry), Sets.newHashSet(
                CQuery.from(qry),
                CQuery.with(qManager).annotate(qManager, new MatchAnnotation(qry)).build()));
    }


    @Test(dataProvider = "reasonerData")
    public void testMatchSubPropertyWithFilter(Supplier<TBoxReasoner> supplier) {
        reasoner = supplier.get();
        reasoner.load(tBoxSpec);
        SemanticSelectDescription description;
        description = new SemanticSelectDescription(ep, true, reasoner);

        SPARQLFilter ann = SPARQLFilter.build("regex(iri(?x), \"Bob$\")");
        CQuery query = createQuery(
                Alice, knows, TestContext.x, ann,
                TestContext.x, age, lit(23));
        SemanticCQueryMatch match = description.semanticMatch(query);

        assertEquals(match.getKnownExclusiveGroups(), emptyList());
        assertEquals(match.getNonExclusiveRelevant(), singletonList(query.get(0)));

        Set<CQuery> expected = new HashSet<>();
        Triple altAdvises = query.get(0).withPredicate(advises);
        Triple altMentors = query.get(0).withPredicate(mentors);
        Triple altManages = query.get(0).withPredicate(manages);
        MatchAnnotation matchAnnotation = new MatchAnnotation(query.get(0));
        expected.add(CQuery.with(altAdvises).annotate(altAdvises, matchAnnotation).build());
        expected.add(CQuery.with(altMentors).annotate(altMentors, matchAnnotation).build());
        expected.add(CQuery.with(altManages).annotate(altManages, matchAnnotation).build());

        assertEquals(match.getAlternatives(query.get(0)), expected);
    }


}