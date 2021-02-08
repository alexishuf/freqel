package br.ufsc.lapesd.freqel.description.semantic;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.description.CQueryMatch;
import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.description.EmptyDescription;
import br.ufsc.lapesd.freqel.description.MatchReasoning;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.reason.tbox.EmptyTBox;
import br.ufsc.lapesd.freqel.reason.tbox.TBox;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxSpec;
import br.ufsc.lapesd.freqel.reason.tbox.TransitiveClosureTBoxMaterializer;
import br.ufsc.lapesd.freqel.reason.tbox.vlog.SystemVLogMaterializer;
import com.github.lapesd.rdfit.components.jena.JenaHelpers;
import com.github.lapesd.rdfit.source.RDFResource;
import com.google.common.collect.Lists;
import org.apache.jena.rdf.model.Model;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

public abstract class SemanticDescriptionTestBase implements TestContext {
    protected CQEndpoint ep;
    protected List<TBox> tBoxes;

    protected static final StdURI D = new StdURI(EX+"D");
    protected static final StdURI D1 = new StdURI(EX+"D1");
    protected static final StdURI D11 = new StdURI(EX+"D11");
    protected static final StdURI D12 = new StdURI(EX+"D12");

    protected static final StdURI p = new StdURI(EX+"p");
    protected static final StdURI p1 = new StdURI(EX+"p1");
    protected static final StdURI p11 = new StdURI(EX+"p11");
    protected static final StdURI p12 = new StdURI(EX+"p12");

    protected static final StdURI U0 = new StdURI(EX+"U0");
    protected static final StdURI U11 = new StdURI(EX+"U11");
    protected static final StdURI U12 = new StdURI(EX+"U12");
    protected static final StdURI U2 = new StdURI(EX+"U2");

    @BeforeClass
    public void setUp() {
        Model model = JenaHelpers.toModel(new RDFResource(getClass(), "data.ttl"));
        ep = (CQEndpoint)ARQEndpoint.forModel(model).setDescription(new EmptyDescription());

        SystemVLogMaterializer vlog = new SystemVLogMaterializer();
        vlog.load(new TBoxSpec().addResource(getClass(), "onto.ttl"));
        TransitiveClosureTBoxMaterializer transitive = new TransitiveClosureTBoxMaterializer();
        transitive.load(new TBoxSpec().addResource(getClass(), "onto.ttl"));
        tBoxes = asList(
                new EmptyTBox(),
                transitive,
                vlog
        );
    }

    protected @Nonnull List<List<Object>> getBaseData() {
        return asList(
                asList(createQuery(x, type, D), singleton(new Triple(x, type, D))), //explicit
                asList(createQuery(x, knows, y), emptyList()), // bad match
                asList(createQuery(x, type, D11), singleton(new Triple(x, type, D11))), //explicit
                asList(createQuery(x, type, D1), singleton(new Triple(x, type, D1))), //implicit

                asList(createQuery(x, p, y), singleton(new Triple(x, p, y))),   //explicit
                asList(createQuery(x, p, U0), singleton(new Triple(x, p, U0))), //explicit
                asList(createQuery(x, p1, y), singleton(new Triple(x, p1, y))), //explicit
                asList(createQuery(x, p11, U11), singleton(new Triple(x, p11, U11))), //explicit
                asList(createQuery(x, p11, U11), singleton(new Triple(x, p11, U11))), //explicit
                asList(createQuery(x, p1, U11), singleton(new Triple(x, p1, U11))), //infer p1 from p11
                asList(createQuery(x, p, U2), singleton(new Triple(x, p, U2))), //infer p from p2
                asList(createQuery(x, p, U11), singleton(new Triple(x, p, U11))) //infer p from p11
        );
    }

    @DataProvider public @Nonnull Object[][] matchData() {
        List<List<Object>> base = getBaseData();
        List<Object[]> rows = new ArrayList<>();
        assertNotNull(tBoxes);
        assertTrue(tBoxes.stream().noneMatch(Objects::isNull));
        for (List<Object> objects : Lists.cartesianProduct(asList(false, true), tBoxes, base)) {
            ArrayList<Object> row = new ArrayList<>((List<?>)objects.get(2));
            row.add(0, objects.get(1));
            row.add(0, objects.get(0));
            rows.add(row.toArray());
        }
        return rows.toArray(new Object[0][]);
    }

    @Test(dataProvider = "matchData")
    public void testMatch(boolean fetchClasses, @Nonnull TBox tBox, @Nonnull CQuery query,
                          @Nonnull Collection<Triple> expected) {
        Description description = createSemantic(ep, fetchClasses, tBox);
        CQueryMatch match = description.match(query, MatchReasoning.TRANSPARENT);
        assertTrue(match.getKnownExclusiveGroups().isEmpty());
        if (tBox instanceof EmptyTBox) {
            Description rawDescription = createNonSemantic(ep, fetchClasses);
            CQueryMatch expectedMatch = rawDescription.match(query, MatchReasoning.TRANSPARENT);
            assertEquals(match.getNonExclusiveRelevant(), expectedMatch.getNonExclusiveRelevant());
        } else {
            if (expected instanceof Set && expected.size() > 1)
                assertEquals(new HashSet<>(match.getNonExclusiveRelevant()), expected);
            assertEquals(match.getNonExclusiveRelevant(), expected);
        }
    }

    protected abstract @Nonnull Description createNonSemantic(@Nonnull CQEndpoint ep,
                                                              boolean fetchClasses);
    protected abstract @Nonnull SemanticDescription
    createSemantic(@Nonnull CQEndpoint ep, boolean fetchClasses, @Nonnull TBox tBox);
}