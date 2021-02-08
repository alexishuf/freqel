package br.ufsc.lapesd.freqel.federation.planner.pre.steps;

import br.ufsc.lapesd.freqel.ResultsAssert;
import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.algebra.inner.PipeOp;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.description.SelectDescription;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.planner.post.steps.PushDistinctStep;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.modifiers.Distinct;
import br.ufsc.lapesd.freqel.query.parse.CQueryContext;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParser;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxSpec;
import br.ufsc.lapesd.freqel.util.ref.EmptyRefSet;
import br.ufsc.lapesd.freqel.util.ref.IdentityHashSet;
import br.ufsc.lapesd.freqel.util.ref.RefSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.XSD;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.PlanAssert.assertPlanAnswers;
import static br.ufsc.lapesd.freqel.PlanAssert.dqDeepStreamPreOrder;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

@Test(groups = {"fast"})
public class PushDistinctStepTest implements TestContext {
    private static final CQEndpoint ep1 = ARQEndpoint.forModel(ModelFactory.createDefaultModel());

    private static @Nonnull EndpointQueryOp q(Object... args) {
        return new EndpointQueryOp(ep1, CQueryContext.createQuery(args));
    }

    @DataProvider
    public static Object[][] testData() {
        EndpointQueryOp xKnowsAlice = q(x, knows, Alice);
        PipeOp xKnowsAlicePipe = new PipeOp(xKnowsAlice);
        xKnowsAlicePipe.modifiers().add(Distinct.INSTANCE);
        EndpointQueryOp xKnowsAliceDistinct = q(x, knows, Alice, Distinct.INSTANCE);
        return Stream.of(
                asList(q(x, knows, y), null, EmptyRefSet.emptySet()),
                asList(q(x, knows, y, Distinct.INSTANCE), null, EmptyRefSet.emptySet()),
                asList(xKnowsAlice, null, IdentityHashSet.of(xKnowsAlice)),
                asList(xKnowsAliceDistinct, null, IdentityHashSet.of(xKnowsAliceDistinct)),
                // no change with union
                asList(UnionOp.builder()
                                .add(q(x, knows, Alice))
                                .add(q(x, knows, Bob))
                                .build(),
                       null, EmptyRefSet.emptySet()),
                // push distinct down union
                asList(UnionOp.builder()
                                .add(q(x, knows, Alice))
                                .add(q(x, knows, Bob))
                                .add(Distinct.INSTANCE).build(),
                       UnionOp.builder()
                               .add(q(x, knows, Alice, Distinct.INSTANCE))
                               .add(q(x, knows, Bob, Distinct.INSTANCE))
                               .add(Distinct.INSTANCE).build(),
                        EmptyRefSet.emptySet()),
                // add pipe while pushing down Join
                asList(JoinOp.builder(xKnowsAlice, q(x, age, u))
                                .add(Distinct.INSTANCE).build(),
                       JoinOp.builder(xKnowsAlicePipe, q(x, age, u, Distinct.INSTANCE))
                               .add(Distinct.INSTANCE).build(),
                       IdentityHashSet.of(xKnowsAlice)),
                // do not add Distinct to intermediate nodes
                asList(JoinOp.builder(UnionOp.builder()
                                                .add(q(x, knows, Alice))
                                                .add(q(x, knows, Bob)).build(),
                                      q(x, age, u)).add(Distinct.INSTANCE).build(),
                       JoinOp.builder(UnionOp.builder()
                                               .add(q(x, knows, Alice, Distinct.INSTANCE))
                                               .add(q(x, knows, Bob, Distinct.INSTANCE)).build(),
                                       q(x, age, u, Distinct.INSTANCE)
                       ).add(Distinct.INSTANCE).build(),
                       EmptyRefSet.emptySet())
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testData")
    public void test(@Nonnull Op in, @Nullable Op expected,
                     @Nonnull RefSet<Op> shared) {
        if (expected == null)
            expected = in;
        expected = TreeUtils.deepCopy(expected);
        boolean expectSame = expected == in;
        Op actual = new PushDistinctStep().plan(in, shared);
        assertEquals(actual, expected);
        if (expectSame)
            assertSame(actual, in);
    }

    @DataProvider
    public static @Nonnull Object[][] testOnDefaultFederationData() {
        String prolog = "PREFIX foaf: <"+ FOAF.NS+">\n" +
                "PREFIX ex: <"+ EX+">\n" +
                "PREFIX xsd: <"+ XSD.NS+">";
        StdURI l1 = new StdURI(EX + "l1");
        StdURI r1 = new StdURI(EX + "r1");
        StdLit i23 = StdLit.fromUnescaped("23", xsdInteger);
        StdLit i27 = StdLit.fromUnescaped("27", xsdInteger);
        StdLit i31 = StdLit.fromUnescaped("31", xsdInteger);
        return Stream.of(
                // single source, candidate solutions already distinct
                asList(prolog+"SELECT DISTINCT ?x WHERE { ?x ex:p1 1 }",
                        singletonList("../../../rdf-optional-1.ttl"),
                       newHashSet(MapSolution.build(x, l1), MapSolution.build(x, r1))),
                // single source, non-distinct candidate solutions
                asList(prolog+"SELECT DISTINCT ?x WHERE {\n" +
                                "  {?x foaf:knows ?y .} UNION {?y foaf:knows ?x .}\n" +
                                "}",
                        singletonList("../../../rdf-optional-1.ttl"),
                        newHashSet(MapSolution.build(x, Alice),
                                   MapSolution.build(x, Bob),
                                   MapSolution.build(x, Charlie))),
                // each source contributes the same solution (deduplicate at the mediator)
                asList(prolog+"SELECT DISTINCT ?x WHERE {?x foaf:name \"alice\".}",
                       asList("source-1.ttl", "source-2.ttl"),
                       newHashSet(MapSolution.build(x, Alice))),
                // plan a cross-source join where one of the sources has duplicate solutions
                asList(prolog+"SELECT DISTINCT ?o WHERE {?x foaf:knows/ex:p1 ?o .}",
                        asList("source-1.ttl", "source-2.ttl"),
                        newHashSet(MapSolution.build(o, i23))),
                // same as above, but expose ?x (making ?o  non-distinct)
                asList(prolog+"SELECT DISTINCT ?x ?o WHERE {?x foaf:knows/ex:p1 ?o .}",
                        asList("source-1.ttl", "source-2.ttl"),
                        newHashSet(
                                MapSolution.builder().put(x, Bob).put(o, i23).build(),
                                MapSolution.builder().put(x, Charlie).put(o, i23).build()
                        )),
                // cartesian over two sources with  non-distinct candidates
                asList(prolog+"SELECT DISTINCT ?o WHERE { ex:Bob foaf:knows/ex:p2 ?o .}",
                       asList("source-1.ttl", "source-2.ttl"),
                       newHashSet(MapSolution.build(o, i27),
                                  MapSolution.build(o, i31)))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "testOnDefaultFederationData")
    public void testOnDefaultFederation(@Nonnull String sparql, @Nonnull List<String> sources,
                                        @Nonnull Set<Solution> expected) throws SPARQLParseException {
        try (Federation federation = Federation.createDefault()) {
            for (String file : sources) {
                Model model = new TBoxSpec().addResource(getClass(), file).loadModel();
                ARQEndpoint ep = ARQEndpoint.forModel(model);
                federation.addSource(ep.setDescription(new SelectDescription(ep)));
            }

            Op query = SPARQLParser.strict().parse(sparql);
            Op plan = federation.plan(query);
            assertPlanAnswers(plan, query);
            boolean isDistinct = query.modifiers().distinct() != null;
            List<Op> bad = dqDeepStreamPreOrder(plan)
                    .filter(n -> n.getChildren().isEmpty())
                    .filter(n -> (n.modifiers().distinct() == null) == isDistinct)
                    .collect(toList());
            assertEquals(bad, emptyList());

            ResultsAssert.assertExpectedResults(federation.execute(plan), expected);
        }
    }
}