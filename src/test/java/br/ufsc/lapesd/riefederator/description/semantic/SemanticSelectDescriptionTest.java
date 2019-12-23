package br.ufsc.lapesd.riefederator.description.semantic;

import br.ufsc.lapesd.riefederator.jena.TBoxLoader;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxReasoner;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxReasonerTest;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxSpec;
import com.google.common.collect.Sets;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.vocabulary.RDF;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.testng.Assert.assertEquals;

public class SemanticSelectDescriptionTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    public static final @Nonnull StdURI CHARLIE = new StdURI("http://example.org/Charlie");
    public static final @Nonnull StdURI DAVE = new StdURI("http://example.org/Dave");

    public static final @Nonnull StdURI Person = new StdURI("http://example.org/source-onto-1.ttl#Person");
    public static final @Nonnull StdURI Employee = new StdURI("http://example.org/source-onto-1.ttl#Employee");
    public static final @Nonnull StdURI Manager = new StdURI("http://example.org/source-onto-1.ttl#Manager");

    public static final @Nonnull StdURI type = new StdURI(RDF.type.getURI());
    public static final @Nonnull StdURI knows = new StdURI("http://example.org/source-onto-1.ttl#knows");
    public static final @Nonnull StdURI advises = new StdURI("http://example.org/source-onto-1.ttl#advises");
    public static final @Nonnull StdURI mentors = new StdURI("http://example.org/source-onto-1.ttl#mentors");
    public static final @Nonnull StdURI manages = new StdURI("http://example.org/source-onto-1.ttl#manages");

    public static final @Nonnull StdVar S = new StdVar("S");
    public static final @Nonnull StdVar P = new StdVar("P");
    public static final @Nonnull StdVar O = new StdVar("O");

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
        Triple qry = new Triple(ALICE, manages, BOB);
        SemanticCQueryMatch m = d.semanticMatch(CQuery.from(qry));
        assertEquals(m.getIrrelevant(), emptyList());
        assertEquals(m.getAllRelevant(), singleton(qry));
        assertEquals(m.getKnownExclusiveGroups(), emptySet());
        assertEquals(m.getAlternatives(qry), singleton(CQuery.from(qry)));
        assertEquals(m.getAlternatives(new Triple(ALICE, manages, O)), emptySet());
    }

    @Test(dataProvider = "reasonerData")
    public void testVarWithExactPredicates(Supplier<TBoxReasoner> supplier) {
        reasoner = supplier.get();
        reasoner.load(tBoxSpec);
        Triple[] ts = {new Triple(ALICE, manages, O), new Triple(O, mentors, CHARLIE)};
        SemanticSelectDescription d = new SemanticSelectDescription(ep, reasoner);
        SemanticCQueryMatch m = d.semanticMatch(CQuery.from(asList(ts)));
        assertEquals(m.getIrrelevant(), emptyList());
        assertEquals(new HashSet<>(m.getAllRelevant()), Sets.newHashSet(ts));
        assertEquals(m.getKnownExclusiveGroups(), emptySet());

        assertEquals(m.getAlternatives(ts[0]), singleton(CQuery.from(ts[0])));
        assertEquals(m.getAlternatives(ts[1]), singleton(CQuery.from(ts[1])));
        assertEquals(m.getAlternatives(new Triple(ALICE, manages, S)), emptySet());
    }

    @Test(dataProvider = "reasonerData")
    public void testSuperPredicateNotUsed(Supplier<TBoxReasoner> supplier) {
        reasoner = supplier.get();
        reasoner.load(tBoxSpec);
        SemanticSelectDescription d = new SemanticSelectDescription(ep, reasoner);
        Triple qry = new Triple(ALICE, knows, O);
        SemanticCQueryMatch m = d.semanticMatch(CQuery.from(qry));
        assertEquals(m.getIrrelevant(), emptyList());
        assertEquals(m.getKnownExclusiveGroups(), emptyList());
        assertEquals(m.getAllRelevant(), singletonList(qry));

        assertEquals(m.getAlternatives(qry),
                Sets.newHashSet(CQuery.from(new Triple(ALICE, advises, O)),
                                CQuery.from(new Triple(ALICE, mentors, O)),
                                CQuery.from(new Triple(ALICE, manages, O))));
        assertEquals(m.getAlternatives(CQuery.from(qry)), emptySet());
    }

    @Test(dataProvider = "reasonerData")
    public void testSuperPredicateUsed(Supplier<TBoxReasoner> supplier) {
        reasoner = supplier.get();
        reasoner.load(tBoxSpec);
        SemanticSelectDescription d = new SemanticSelectDescription(ep, reasoner);
        Triple qry = new Triple(S, advises, CHARLIE);
        SemanticCQueryMatch m = d.semanticMatch(CQuery.from(qry));
        assertEquals(m.getIrrelevant(), emptyList());
        assertEquals(m.getKnownExclusiveGroups(), emptyList());
        assertEquals(m.getAllRelevant(), singletonList(qry));
        assertEquals(m.getNonExclusiveRelevant(), singletonList(qry));

        assertEquals(m.getAlternatives(qry),
                Sets.newHashSet(CQuery.from(qry),
                                CQuery.from(new Triple(S, advises, CHARLIE)),
                                CQuery.from(new Triple(S, mentors, CHARLIE))));
    }


    @Test(dataProvider = "reasonerData")
    public void testVarWithExactClass(Supplier<TBoxReasoner> supplier) {
        reasoner = supplier.get();
        reasoner.load(tBoxSpec);
        SemanticSelectDescription d = new SemanticSelectDescription(ep, true, reasoner);
        Triple qry = new Triple(S, type, Manager);
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
        Triple qry = new Triple(S, type, Person);
        SemanticCQueryMatch m = d.semanticMatch(CQuery.from(qry));
        assertEquals(m.getIrrelevant(), emptyList());
        assertEquals(m.getKnownExclusiveGroups(), emptyList());
        assertEquals(m.getAllRelevant(), singletonList(qry));
        assertEquals(m.getNonExclusiveRelevant(), singletonList(qry));

        assertEquals(m.getAlternatives(qry),
                     Sets.newHashSet(CQuery.from(new Triple(S, type, Employee)),
                                     CQuery.from(new Triple(S, type, Manager))));
    }


    @Test(dataProvider = "reasonerData")
    public void testVarWithSuperClassUsed(Supplier<TBoxReasoner> supplier) {
        reasoner = supplier.get();
        reasoner.load(tBoxSpec);
        SemanticSelectDescription d = new SemanticSelectDescription(ep, true, reasoner);
        Triple qry = new Triple(S, type, Employee);
        SemanticCQueryMatch m = d.semanticMatch(CQuery.from(qry));
        assertEquals(m.getIrrelevant(), emptyList());
        assertEquals(m.getKnownExclusiveGroups(), emptyList());
        assertEquals(m.getAllRelevant(), singletonList(qry));
        assertEquals(m.getNonExclusiveRelevant(), singletonList(qry));

        assertEquals(m.getAlternatives(qry),
                     Sets.newHashSet(CQuery.from(qry), CQuery.from(new Triple(S, type, Manager))));
    }

}