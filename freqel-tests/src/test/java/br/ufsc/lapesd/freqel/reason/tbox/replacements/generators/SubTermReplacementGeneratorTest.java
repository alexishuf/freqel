package br.ufsc.lapesd.freqel.reason.tbox.replacements.generators;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.description.AskDescription;
import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.description.SelectDescription;
import br.ufsc.lapesd.freqel.description.semantic.SemanticAskDescription;
import br.ufsc.lapesd.freqel.description.semantic.SemanticSelectDescription;
import br.ufsc.lapesd.freqel.federation.inject.dagger.DaggerTestComponent;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.DQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.decorators.EndpointDecorators;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxMaterializer;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxSpec;
import br.ufsc.lapesd.freqel.reason.tbox.TransitiveClosureTBoxMaterializer;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.Replacement;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementContext;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementPruner;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.pruners.DescriptionReplacementPruner;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.pruners.MultiReplacementPruner;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.pruners.NoReplacementPruner;
import com.google.common.collect.Lists;
import org.apache.jena.rdf.model.Model;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static br.ufsc.lapesd.freqel.description.MatchReasoning.NONE;
import static br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterFactory.parseFilter;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class SubTermReplacementGeneratorTest implements TestContext {
    private static final @Nonnull StdURI D = new StdURI(EX+"D");
    private static final @Nonnull StdURI D1 = new StdURI(EX+"D1");
    private static final @Nonnull StdURI D11 = new StdURI(EX+"D11");
    private static final @Nonnull StdURI D111 = new StdURI(EX+"D111");
    private static final @Nonnull StdURI D1111 = new StdURI(EX+"D1111");
    private static final @Nonnull StdURI D12 = new StdURI(EX+"D12");
    private static final @Nonnull StdURI D2 = new StdURI(EX+"D2");

    private static final @Nonnull StdURI p = new StdURI(EX+"p");
    private static final @Nonnull StdURI p1 = new StdURI(EX+"p1");
    private static final @Nonnull StdURI p11 = new StdURI(EX+"p11");
    private static final @Nonnull StdURI p111 = new StdURI(EX+"p111");
    private static final @Nonnull StdURI p1111 = new StdURI(EX+"p1111");
    private static final @Nonnull StdURI p12 = new StdURI(EX+"p12");
    private static final @Nonnull StdURI p2 = new StdURI(EX+"p2");

    private final @Nonnull EmptyEndpoint emptyEp = new EmptyEndpoint();
    private final @Nonnull TBoxMaterializer onto = new TransitiveClosureTBoxMaterializer();
    private ARQEndpoint ep1;

    @BeforeClass
    public void beforeClass() {
        onto.load(new TBoxSpec().addResource(getClass(), "subterm-onto.ttl"));
        assertTrue(onto.withSubClasses(D).collect(toSet()).contains(D));
        assertTrue(onto.withSubClasses(D).collect(toSet()).contains(D11));

        Model model1 = new TBoxSpec().addResource(getClass(), "subterm-1.ttl").loadModel();
        ep1 = ARQEndpoint.forModel(model1);
        ep1.setDescription(new SelectDescription(ep1));
        ep1.getDescription().init();
        assertTrue(ep1.getDescription().waitForInit(5000));
    }

    private @Nonnull SubTermReplacementGenerator createGen(@Nonnull ReplacementPruner... pruners) {
        ReplacementPruner thePruner;
        if (pruners.length == 1)
            thePruner = pruners[0];
        else if (pruners.length > 1)
            thePruner = new MultiReplacementPruner(newHashSet(pruners));
        else
            thePruner = NoReplacementPruner.INSTANCE;
        SubTermReplacementGenerator gen = new SubTermReplacementGenerator(thePruner);
        gen.setTBox(onto);
        return gen;
    }

    @Test
    public void testDoNotReplaceWithItself() {
        SubTermReplacementGenerator gen = createGen();
        MutableCQuery q = createQuery(Alice, knows, Bob);
        assertFalse(gen.generate(q, ep1).iterator().hasNext());
    }

    @Test
    public void testReplaceSubProperty() {
        SubTermReplacementGenerator gen = createGen();
        MutableCQuery q = createQuery(Alice, p, o);

        ReplacementContext ctx = new ReplacementContext(q, singleton(q.get(0)), ep1);
        Set<Term> subProps = onto.withSubProperties(p).collect(toSet());
        Replacement replacement = new Replacement(p, subProps, ctx);
        assertEquals(gen.generateList(q, ep1), singletonList(replacement));
    }

    @Test
    public void testReplaceClass() {
        SubTermReplacementGenerator gen = createGen();
        MutableCQuery q = createQuery(Alice, type, D);

        ReplacementContext ctx = new ReplacementContext(q, singleton(q.get(0)), ep1);
        HashSet<Term> alternatives = newHashSet(D, D1, D11, D111, D1111, D12, D2);
        assertEquals(gen.generateList(q, ep1),
                     singletonList(new Replacement(D, alternatives, ctx)));
    }

    @Test
    public void testReplaceSubPropertyAllPruned() {
        SubTermReplacementGenerator gen = createGen(new DescriptionReplacementPruner());
        assertFalse(gen.generate(createQuery(Alice, p, o), emptyEp).iterator().hasNext());
        assertFalse(gen.generate(createQuery(x, p, o), emptyEp).iterator().hasNext());
    }

    @DataProvider public @Nonnull Object[][] baseTestData() {
        List<Function<TPEndpoint, ? extends Description>> factories = asList(
                SelectDescription.MEM_FACTORY,
                SelectDescription.MEM_FACTORY_WITH_CLASSES,
                AskDescription::new,
                ep -> new SemanticSelectDescription((CQEndpoint) ep, false, onto),
                ep -> new SemanticSelectDescription((CQEndpoint) ep, true, onto),
                ep -> new SemanticAskDescription(ep, onto));
        List<ReplacementPruner> pruners = Arrays.asList(
                new NoReplacementPruner(),
                new DescriptionReplacementPruner(),
                DaggerTestComponent.builder().build().replacementPruner());
        return Lists.cartesianProduct(pruners, factories).stream().map(List::toArray)
                .toArray(Object[][]::new);
    }

    @Test(dataProvider = "baseTestData")
    public void testReplaceSubPropertiesAndPrune(@Nonnull ReplacementPruner pruner,
                                 @Nonnull Function<TPEndpoint, Description> descriptionFactory) {
        SubTermReplacementGenerator gen = createGen(pruner);
        MutableCQuery q = createQuery(Alice, p, o);

        // DescriptionReplacementPruner will get null from localMatch and will not prune
        CQEndpoint ep = EndpointDecorators.withDescription(ep1, descriptionFactory.apply(ep1));
        Description description = ep.getDescription();
        onto.withSubProperties(p).forEach(subp ->
                description.match(createQuery(x, subp, y), NONE));

        ReplacementContext ctx = new ReplacementContext(q, singleton(q.get(0)), ep);
        HashSet<Term> alternatives = pruner instanceof NoReplacementPruner
                                   ? newHashSet(p, p1, p11, p111, p1111, p12, p2)
                                   : newHashSet(p, p1, p11);
        assertEquals(gen.generateList(q, ep),
                     singletonList(new Replacement(p, alternatives, ctx)));
    }

    @Test
    public void testReplaceSubPropertiesAndPruneWithColdAskDescription() {
        SubTermReplacementGenerator gen = createGen(new DescriptionReplacementPruner());
        MutableCQuery q = createQuery(Alice, p, o);

        // DescriptionReplacementPruner will get null from localMatch and will not prune
        CQEndpoint ep = EndpointDecorators.withDescription(ep1, new AskDescription(ep1));

        ReplacementContext ctx = new ReplacementContext(q, q.get(0), ep);
        HashSet<Term> alternatives = newHashSet(p, p1, p11, p111, p1111, p12, p2);
        assertEquals(gen.generateList(q, ep),
                     singletonList(new Replacement(p, alternatives, ctx)));
    }

    @Test(dataProvider = "baseTestData")
    public void testMultipleReplacementsWithHotDescription(@Nonnull ReplacementPruner pruner,
                                   @Nonnull Function<TPEndpoint, Description> descriptionFactory) {
        SubTermReplacementGenerator gen = createGen(pruner);
        MutableCQuery q = createQuery(Alice, knows, x,
                                      Alice, p, x,
                                      x, type, D1,
                                      x, age, u, parseFilter("?u < 27"));
        DQEndpoint ep = EndpointDecorators.withDescription(ep1, descriptionFactory.apply(ep1));
        Description description = ep.getDescription();
        onto.withSubProperties(p).forEach(sp -> description.match(createQuery(x, sp, y), NONE));
        onto.withSubClasses(D).forEach(sc -> description.match(createQuery(x, type, sc), NONE));

        Set<Term> pAlternatives = pruner instanceof NoReplacementPruner
                                ? onto.withSubProperties(p).collect(toSet())
                                : newHashSet(p, p1, p11);
        boolean looseClasses = pruner instanceof NoReplacementPruner
                            || !description.match(createQuery(x, type, D1111), NONE).isEmpty();
        Set<Term> d1Alternatives = looseClasses
                                 ? onto.withSubClasses(D1).collect(toSet()) : singleton(D11);

        assertEquals(gen.generateList(q, ep), asList(
                new Replacement(p,  pAlternatives,  new ReplacementContext(q, q.get(1), ep)),
                new Replacement(D1, d1Alternatives, new ReplacementContext(q, q.get(2), ep))
        ));
    }
}