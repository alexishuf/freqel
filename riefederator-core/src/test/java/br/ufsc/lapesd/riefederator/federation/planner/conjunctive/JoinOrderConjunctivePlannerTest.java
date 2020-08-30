package br.ufsc.lapesd.riefederator.federation.planner.conjunctive;

import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.JoinInfo;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.federation.SimpleFederationModule;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityEnsemble;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityHeuristic;
import br.ufsc.lapesd.riefederator.federation.cardinality.EstimatePolicy;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.GeneralSelectivityHeuristic;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.WorstCaseCardinalityEnsemble;
import br.ufsc.lapesd.riefederator.federation.planner.ConjunctivePlannerTest;
import br.ufsc.lapesd.riefederator.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.util.CollectionUtils;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import com.google.common.collect.Collections2;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.multibindings.Multibinder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.algebra.util.TreeUtils.*;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

public class JoinOrderConjunctivePlannerTest implements TestContext {
    private static final URI o1 = new StdURI("http://example.org/o1");
    private static final URI o2 = new StdURI("http://example.org/o2");
    private static final URI o3 = new StdURI("http://example.org/o3");
    private static final URI o4 = new StdURI("http://example.org/o4");

    private static final Var a = new StdVar("a");
    private static final Var b = new StdVar("b");
    private static final Var c = new StdVar("c");
    private static final Var d = new StdVar("d");
    private static final Var s = new StdVar("s");
    private static final Var t = new StdVar("t");

    private static final EmptyEndpoint e1 = new EmptyEndpoint(), e1a = new EmptyEndpoint(),
                                 e2 = new EmptyEndpoint();

    private static final Atom Person = new Atom("Person");

    static {
        e1.addAlternative(e1a);
    }

    public static final List<Supplier<JoinOrderPlanner>> suppliers =
            asList(
                    new NamedSupplier<>(ArbitraryJoinOrderPlanner.class),
                    new NamedSupplier<>("GreedyJoinOrderPlanner, without estimation",
                        () -> Guice.createInjector(new AbstractModule() {
                            @Override
                            protected void configure() {}
                        }).getInstance(GreedyJoinOrderPlanner.class)),
                    new NamedSupplier<>("GreedyJoinOrderPlanner+GeneralSelectivityHeuristic",
                            () -> Guice.createInjector(new AbstractModule() {
                                @Override
                                protected void configure() {
                                    bind(CardinalityEnsemble.class).to(WorstCaseCardinalityEnsemble.class);

                                    Multibinder<CardinalityHeuristic> mb
                                            = Multibinder.newSetBinder(binder(), CardinalityHeuristic.class);
                                    mb.addBinding().to(GeneralSelectivityHeuristic.class);
                                }
                            }).getInstance(GreedyJoinOrderPlanner.class)),
                    new NamedSupplier<>("GreedyJoinOrderPlanner+default estimation",
                        () -> Guice.createInjector(new AbstractModule() {
                            @Override
                            protected void configure() {
                                SimpleFederationModule.configureCardinalityEstimation(binder(),
                                        EstimatePolicy.local(50));
                            }
                        }).getInstance(GreedyJoinOrderPlanner.class))
            );

    private void checkPlan(Op root, Set<Op> expectedLeaves) {
        assertTrue(isTree(root)); //stricter than acyclic

        Set<Triple> allTriples = expectedLeaves.stream().map(Op::getMatchedTriples)
                .reduce(CollectionUtils::union).orElse(emptySet());

        // more general tests from PlannerTest
        CQuery query = CQuery.from(allTriples);
        ConjunctivePlannerTest.assertPlanAnswers(root, query);

        // no duplicate leaves (QueryNodes)
        List<Op> leaves = streamPreOrder(root)
                .filter(n -> n instanceof EndpointQueryOp).collect(toList());
        assertEquals(leaves.size(), new HashSet<>(leaves).size());

        // all leaf QueryNode was present in expectedLeaves
        Set<Op> expectedQNLeaves = expectedLeaves.stream()
                .flatMap(n -> childrenIfMulti(n).stream()).collect(toSet());
        assertTrue(expectedQNLeaves.containsAll(leaves)); //no made-up QueryNodes

        // every QueryNode from expectedQNLeaves missing from leaves must be equivalent to another
        List<Op> missingLeaves = expectedQNLeaves.stream().filter(qn -> !leaves.contains(qn))
                .filter(missing -> {
                    Set<Triple> triples = ((EndpointQueryOp) missing).getQuery().attr().getSet();
                    TPEndpoint ep = ((EndpointQueryOp) missing).getEndpoint();
                    for (Op leaf : leaves) {
                        EndpointQueryOp leafQN = (EndpointQueryOp) leaf;
                        TPEndpoint candidate = leafQN.getEndpoint();
                        if (leafQN.getQuery().attr().getSet().equals(triples) &&
                                (candidate.isAlternative(ep) || ep.isAlternative(candidate))) {
                            return false;
                        }
                    }
                    return true;
                }).collect(toList());
        assertEquals(missingLeaves, emptyList());

        // nothing but JoinNodes should be introduced
        Set<Op> nonJoinInner = streamPreOrder(root).filter(n -> !leaves.contains(n))
                .filter(n -> !(n instanceof JoinOp) && !expectedLeaves.contains(n))
                .collect(toSet());
        assertEquals(nonJoinInner, emptySet());
    }

    private @Nonnull Set<Op> getPlanNodes(List<JoinInfo> list) {
        Set<Op> expectedLeaves;
        expectedLeaves = list.stream().flatMap(i -> i.getNodes().stream()).collect(toSet());
        return expectedLeaves;
    }

    private boolean isBadPath(List<JoinInfo> list) {
        return list.stream().anyMatch(i -> !i.isValid());
    }


    @DataProvider
    public static Object[][] planData() {
        EndpointQueryOp n1 = new EndpointQueryOp(e1, createQuery(Alice, p1, x));
        EndpointQueryOp n2 = new EndpointQueryOp(e1, createQuery(x, p2, y));
        EndpointQueryOp n3 = new EndpointQueryOp(e1, createQuery(y, p3, z));
        EndpointQueryOp n4 = new EndpointQueryOp(e1, createQuery(z, p4, w));
        EndpointQueryOp n5 = new EndpointQueryOp(e1, createQuery(w, p5, Bob));
        EndpointQueryOp n6 = new EndpointQueryOp(e1, createQuery(w, p6, Bob));
        EndpointQueryOp n7 = new EndpointQueryOp(e1, createQuery(z, p7, x));

        EndpointQueryOp n2i = new EndpointQueryOp(e1, createQuery(x, AtomInputAnnotation.asRequired(Person, "Person").get(), p2, y));
        EndpointQueryOp n3a = new EndpointQueryOp(e1a, createQuery(y, p3, z));
        EndpointQueryOp n4b  = new EndpointQueryOp(e2, createQuery(z, p4, w));

        Op m2 = UnionOp.builder().add(n2).add(n2i).build();
        Op m3 = UnionOp.builder().add(n3).add(n3a).build();
        Op m4 = UnionOp.builder().add(n4).add(n4b).build();

        return suppliers.stream().flatMap(s -> Stream.of(
                asList(s, singletonList(JoinInfo.getJoinability(n1, n2))),
                asList(s, singletonList(JoinInfo.getJoinability(n2, n4))),
                asList(s, asList(JoinInfo.getJoinability(n1, n2), JoinInfo.getJoinability(n2, n3))),
                asList(s, asList(JoinInfo.getJoinability(n4, n2), JoinInfo.getJoinability(n2, n1))),
                asList(s, asList(JoinInfo.getJoinability(n2, n3), JoinInfo.getJoinability(n1, n2))),
                asList(s, asList(JoinInfo.getJoinability(n1, n2), JoinInfo.getJoinability(n2, n3), JoinInfo.getJoinability(n3, n4))),
                asList(s, asList(JoinInfo.getJoinability(n4, n3), JoinInfo.getJoinability(n3, n2), JoinInfo.getJoinability(n2, n1))),
                asList(s, asList(JoinInfo.getJoinability(n3, n4), JoinInfo.getJoinability(n2, n3), JoinInfo.getJoinability(n1, n2))),
                asList(s, asList(JoinInfo.getJoinability(n1, n2), JoinInfo.getJoinability(n2, n3), JoinInfo.getJoinability(n3, n4), JoinInfo.getJoinability(n4, n5))),
                asList(s, asList(JoinInfo.getJoinability(n5, n4), JoinInfo.getJoinability(n4, n3), JoinInfo.getJoinability(n3, n2), JoinInfo.getJoinability(n2, n1))),
                asList(s, asList(JoinInfo.getJoinability(n4, n5), JoinInfo.getJoinability(n3, n4), JoinInfo.getJoinability(n2, n3), JoinInfo.getJoinability(n1, n2))),
                asList(s, asList(JoinInfo.getJoinability(n6, n5), JoinInfo.getJoinability(n4, n5), JoinInfo.getJoinability(n4, n7), JoinInfo.getJoinability(n3, n4), JoinInfo.getJoinability(n2, n3), JoinInfo.getJoinability(n1, n2))),
                asList(s, asList(JoinInfo.getJoinability(n1, n2), JoinInfo.getJoinability(n2, n3), JoinInfo.getJoinability(n3, n4), JoinInfo.getJoinability(n4, n7), JoinInfo.getJoinability(n4, n5), JoinInfo.getJoinability(n5, n6))),
                /* test cases exploring removal of equivalent query nodes within MultiQueryNodes */
                asList(s, singletonList(JoinInfo.getJoinability(n1, m2))),
                asList(s, asList(JoinInfo.getJoinability(n1, m2), JoinInfo.getJoinability(m2, n3))),
                asList(s, asList(JoinInfo.getJoinability(n1, n2), JoinInfo.getJoinability(n2, m3))),
                asList(s, asList(JoinInfo.getJoinability(n1, n2), JoinInfo.getJoinability(n2, m3), JoinInfo.getJoinability(m3, n4))),
                asList(s, asList(JoinInfo.getJoinability(n1, n2), JoinInfo.getJoinability(n2, n3), JoinInfo.getJoinability(n3, m4))),
                asList(s, asList(JoinInfo.getJoinability(n1, n2), JoinInfo.getJoinability(n2, n3), JoinInfo.getJoinability(n3, m4), JoinInfo.getJoinability(m4, n5))),
                /* same as above, but in reversal join order (more tha Collections.reverse()) */
                asList(s, singletonList(JoinInfo.getJoinability(m2, n1))),
                asList(s, asList(JoinInfo.getJoinability(n3, m2), JoinInfo.getJoinability(m2, n1))),
                asList(s, asList(JoinInfo.getJoinability(n3, n2), JoinInfo.getJoinability(n2, n1))),
                asList(s, asList(JoinInfo.getJoinability(n4, m3), JoinInfo.getJoinability(m3, n2), JoinInfo.getJoinability(n2, n1))),
                asList(s, asList(JoinInfo.getJoinability(m4, n3), JoinInfo.getJoinability(n3, n2), JoinInfo.getJoinability(n2, n1))),
                asList(s, asList(JoinInfo.getJoinability(n5, m4), JoinInfo.getJoinability(m4, n3), JoinInfo.getJoinability(n3, n2), JoinInfo.getJoinability(n2, n1))),
                /* Same as the two previous blocks, now using getPlainJoinability */
                /*   - test cases exploring removal of equivalent query nodes within MultiQueryNodes */
                asList(s, singletonList(JoinInfo.getJoinability(n1, m2))),
                asList(s, asList(JoinInfo.getJoinability(n1, m2), JoinInfo.getJoinability(m2, n3))),
                asList(s, asList(JoinInfo.getJoinability(n1, n2), JoinInfo.getJoinability(n2, m3))),
                asList(s, asList(JoinInfo.getJoinability(n1, n2), JoinInfo.getJoinability(n2, m3), JoinInfo.getJoinability(m3, n4))),
                asList(s, asList(JoinInfo.getJoinability(n1, n2), JoinInfo.getJoinability(n2, n3), JoinInfo.getJoinability(n3, m4))),
                asList(s, asList(JoinInfo.getJoinability(n1, n2), JoinInfo.getJoinability(n2, n3), JoinInfo.getJoinability(n3, m4), JoinInfo.getJoinability(m4, n5))),
                /*   - same as above, but in reversal join order (more tha Collections.reverse()) */
                asList(s, singletonList(JoinInfo.getJoinability(m2, n1))),
                asList(s, asList(JoinInfo.getJoinability(n3, m2), JoinInfo.getJoinability(m2, n1))),
                asList(s, asList(JoinInfo.getJoinability(n3, n2), JoinInfo.getJoinability(n2, n1))),
                asList(s, asList(JoinInfo.getJoinability(n4, m3), JoinInfo.getJoinability(m3, n2), JoinInfo.getJoinability(n2, n1))),
                asList(s, asList(JoinInfo.getJoinability(m4, n3), JoinInfo.getJoinability(n3, n2), JoinInfo.getJoinability(n2, n1))),
                asList(s, asList(JoinInfo.getJoinability(n5, m4), JoinInfo.getJoinability(m4, n3), JoinInfo.getJoinability(n3, n2), JoinInfo.getJoinability(n2, n1)))
                )).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "planData")
    public void testPlanGivenNodes(Supplier<JoinOrderPlanner> supplier, List<JoinInfo> list) {
        JoinOrderPlanner planner = supplier.get();
        Set<Op> leavesSet = getPlanNodes(list);
        JoinGraph joinGraph = new JoinGraph(IndexedSet.from(leavesSet));
        int rounds = 0;
        //noinspection UnstableApiUsage
        for (List<Op> permutation : Collections2.permutations(new ArrayList<>(leavesSet))) {
            if (isBadPath(list)) {
                expectThrows(IllegalArgumentException.class,
                        () -> planner.plan(joinGraph, permutation));
            } else {
                checkPlan(planner.plan(joinGraph, permutation), leavesSet);
            }
            if (++rounds > 1024)
                break;
        }
    }

    @DataProvider
    public static Object[][] suppliersData() {
        return suppliers.stream().map(s -> new Object[]{s}).toArray(Object[][]::new);
    }


    @Test(dataProvider = "suppliersData", groups = {"fast"})
    public void testPlanGivenNodesNonLinearPath(Supplier<JoinOrderPlanner> supplier) {
        EndpointQueryOp orgByDesc = new EndpointQueryOp(e1, createQuery(o1, p1, t));
        EndpointQueryOp contract = new EndpointQueryOp(e1, createQuery(
                o2, p2, b,
                o2, p3, c,
                o2, p4, t, AtomInputAnnotation.asRequired(new Atom("A1"), "A1").get()));
        EndpointQueryOp contractById = new EndpointQueryOp(e1, createQuery(
                b, AtomInputAnnotation.asRequired(new Atom("A2"), "A2").get(), p5, o3));
        EndpointQueryOp contractorByName = new EndpointQueryOp(e1, createQuery(
                c, AtomInputAnnotation.asRequired(new Atom("A3"), "A3").get(), p6, s));
        EndpointQueryOp procurementsOfContractor = new EndpointQueryOp(e1, createQuery(
                s, AtomInputAnnotation.asRequired(new Atom("A4"), "A4").get(), p7, a));
        EndpointQueryOp procurementById = new EndpointQueryOp(e1, createQuery(
                a, AtomInputAnnotation.asRequired(new Atom("A5"), "A5").get(), p8, d));
        EndpointQueryOp modalities = new EndpointQueryOp(e1, createQuery(o4, p9, d));

        JoinOrderPlanner planner = supplier.get();
        IndexedSet<Op> leaves = IndexedSet.from(asList(
                contractorByName, procurementsOfContractor, contractById, modalities,
                procurementById, orgByDesc, contract));
        JoinGraph graph = new JoinGraph(leaves);
        assertEquals(graph.size(), 7);

        Op plan = planner.plan(graph, new ArrayList<>(leaves));
        checkPlan(plan, leaves);
    }

}