package br.ufsc.lapesd.freqel.federation.planner.conjunctive;

import br.ufsc.lapesd.freqel.PlanAssert;
import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.JoinInfo;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.cardinality.impl.GeneralSelectivityHeuristic;
import br.ufsc.lapesd.freqel.cardinality.impl.NoCardinalityEnsemble;
import br.ufsc.lapesd.freqel.cardinality.impl.QuickSelectivityHeuristic;
import br.ufsc.lapesd.freqel.cardinality.impl.WorstCaseCardinalityEnsemble;
import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.description.molecules.annotations.AtomInputAnnotation;
import br.ufsc.lapesd.freqel.federation.inject.dagger.DaggerTestComponent;
import br.ufsc.lapesd.freqel.federation.inject.dagger.TestComponent;
import br.ufsc.lapesd.freqel.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.freqel.federation.planner.equiv.DefaultEquivCleaner;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.URI;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.model.term.std.StdVar;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.freqel.query.modifiers.Optional;
import br.ufsc.lapesd.freqel.util.CollectionUtils;
import br.ufsc.lapesd.freqel.util.NamedSupplier;
import br.ufsc.lapesd.freqel.util.indexed.ref.RefIndexSet;
import com.google.common.collect.Collections2;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.algebra.JoinInfo.getJoinability;
import static br.ufsc.lapesd.freqel.algebra.util.TreeUtils.*;
import static br.ufsc.lapesd.freqel.federation.FreqelConfig.Key.*;
import static br.ufsc.lapesd.freqel.federation.FreqelConfig.createDefault;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

public class JoinOrderPlannerTest implements TestContext {
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
                    new NamedSupplier<>("Default JoinOrderPlanner",
                            () -> DaggerTestComponent.builder()
                                    .overrideFreqelConfig(createDefault()
                                            .set(EQUIV_CLEANER, DefaultEquivCleaner.class))
                                    .build().joinOrderPlanner()),
                    new NamedSupplier<>("Default GreedyJoinOrderPlanner",
                            () -> {
                                TestComponent.Builder b = DaggerTestComponent.builder();
                                b.overrideFreqelConfig(createDefault()
                                        .set(EQUIV_CLEANER, DefaultEquivCleaner.class)
                                        .set(JOIN_ORDER_PLANNER, GreedyJoinOrderPlanner.class));
                                return b.build().joinOrderPlanner();
                            }),
                    new NamedSupplier<>("GreedyJoinOrderPlanner, without estimation",
                            () -> {
                                TestComponent.Builder b = DaggerTestComponent.builder();
                                b.overrideEquivCleaner(DefaultEquivCleaner.INSTANCE);
                                b.overrideFreqelConfig(createDefault()
                                        .set(JOIN_ORDER_PLANNER, GreedyJoinOrderPlanner.class)
                                        .set(CARDINALITY_ENSEMBLE, NoCardinalityEnsemble.class));
                                return b.build().joinOrderPlanner();
                    }),
                    new NamedSupplier<>("GreedyJoinOrderPlanner+GeneralSelectivityHeuristic",
                            () -> {
                                TestComponent.Builder b = DaggerTestComponent.builder();
                                b.overrideFreqelConfig(createDefault()
                                        .set(CARDINALITY_ENSEMBLE, WorstCaseCardinalityEnsemble.class)
                                        .set(EQUIV_CLEANER, DefaultEquivCleaner.class)
                                        .set(CARDINALITY_HEURISTICS, singleton(GeneralSelectivityHeuristic.class))
                                        .set(JOIN_ORDER_PLANNER, GreedyJoinOrderPlanner.class)
                                );
                                return b.build().joinOrderPlanner();
                            }),
                    new NamedSupplier<>("GreedyJoinOrderPlanner+QuickSelectivityHeuristic",
                            () -> {
                                TestComponent.Builder b = DaggerTestComponent.builder();
                                b.overrideFreqelConfig(createDefault()
                                        .set(CARDINALITY_ENSEMBLE, WorstCaseCardinalityEnsemble.class)
                                        .set(EQUIV_CLEANER, DefaultEquivCleaner.class)
                                        .set(CARDINALITY_HEURISTICS, singleton(QuickSelectivityHeuristic.class))
                                        .set(JOIN_ORDER_PLANNER, GreedyJoinOrderPlanner.class)
                                );
                                return b.build().joinOrderPlanner();
                            })
            );

    private void checkPlan(Op root, Set<Op> expectedLeaves) {
        assertTrue(isTree(root)); //stricter than acyclic

        Set<Triple> allTriples = expectedLeaves.stream().map(Op::getMatchedTriples)
                .reduce(CollectionUtils::union).orElse(emptySet());

        // more general tests from PlannerTest
        MutableCQuery query = MutableCQuery.from(allTriples);
        if (expectedLeaves.stream().allMatch(o -> o.modifiers().optional() != null))
            query.mutateModifiers().add(Optional.IMPLICIT);
        PlanAssert.assertPlanAnswers(root, query);

        // no duplicate leaves (QueryNodes)
        List<Op> leaves = streamPreOrder(root)
                .filter(n -> n instanceof EndpointQueryOp).collect(toList());
        assertEquals(leaves.size(), new HashSet<>(leaves).size());

        // all leaf QueryNode was present in expectedLeaves
        Set<Op> expectedQNLeaves = expectedLeaves.stream()
                .flatMap(n -> childrenIfUnion(n).stream()).collect(toSet());
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

        // if both children of a JoinOp are optional, the join itself should be optional
        List<Op> missingOptional = streamPreOrder(root).filter(o -> o instanceof JoinOp
                && o.getChildren().stream().allMatch(c -> c.modifiers().optional() != null)
                && o.modifiers().optional() == null).collect(toList());
        assertEquals(missingOptional, emptyList());

        // joins should never be Optional, except if both children are
        List<Op> badOptional = streamPreOrder(root).filter(o -> o instanceof JoinOp
                && !o.getChildren().stream().allMatch(c -> c.modifiers().optional() != null)
                && o.modifiers().optional() != null).collect(toList());
        assertEquals(badOptional, emptyList());

    }

    private @Nonnull Set<Op> getPlanNodes(List<JoinInfo> list) {
        return list.stream().flatMap(i -> Stream.of(i.getLeft(), i.getRight()))
                            .collect(toSet());
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

        EndpointQueryOp n1o = new EndpointQueryOp(e1, createQuery(Alice, p1, x, Optional.EXPLICIT));
        EndpointQueryOp n2o = new EndpointQueryOp(e1, createQuery(x, p2, y, Optional.EXPLICIT));
        EndpointQueryOp n3o = new EndpointQueryOp(e1, createQuery(y, p3, z, Optional.EXPLICIT));
        EndpointQueryOp n4o = new EndpointQueryOp(e1, createQuery(z, p4, w, Optional.EXPLICIT));

        Op m2 = UnionOp.builder().add(n2).add(n2i).build();
        Op m3 = UnionOp.builder().add(n3).add(n3a).build();
        Op m4 = UnionOp.builder().add(n4).add(n4b).build();

        return suppliers.stream().flatMap(s -> Stream.of(
                asList(s, singletonList(getJoinability(n1, n2))),
                asList(s, singletonList(getJoinability(n2, n4))),
                asList(s, asList(getJoinability(n1, n2), getJoinability(n2, n3))),
                asList(s, asList(getJoinability(n4, n2), getJoinability(n2, n1))),
                asList(s, asList(getJoinability(n2, n3), getJoinability(n1, n2))),
                asList(s, asList(getJoinability(n1, n2), getJoinability(n2, n3), getJoinability(n3, n4))),
                asList(s, asList(getJoinability(n4, n3), getJoinability(n3, n2), getJoinability(n2, n1))),
                asList(s, asList(getJoinability(n3, n4), getJoinability(n2, n3), getJoinability(n1, n2))),
                asList(s, asList(getJoinability(n1, n2), getJoinability(n2, n3), getJoinability(n3, n4), getJoinability(n4, n5))),
                asList(s, asList(getJoinability(n5, n4), getJoinability(n4, n3), getJoinability(n3, n2), getJoinability(n2, n1))),
                asList(s, asList(getJoinability(n4, n5), getJoinability(n3, n4), getJoinability(n2, n3), getJoinability(n1, n2))),
                asList(s, asList(getJoinability(n6, n5), getJoinability(n4, n5), getJoinability(n4, n7), getJoinability(n3, n4), getJoinability(n2, n3), getJoinability(n1, n2))),
                asList(s, asList(getJoinability(n1, n2), getJoinability(n2, n3), getJoinability(n3, n4), getJoinability(n4, n7), getJoinability(n4, n5), getJoinability(n5, n6))),
                /* test cases exploring removal of equivalent query nodes within MultiQueryNodes */
                asList(s, singletonList(getJoinability(n1, m2))),
                asList(s, asList(getJoinability(n1, m2), getJoinability(m2, n3))),
                asList(s, asList(getJoinability(n1, n2), getJoinability(n2, m3))),
                asList(s, asList(getJoinability(n1, n2), getJoinability(n2, m3), getJoinability(m3, n4))),
                asList(s, asList(getJoinability(n1, n2), getJoinability(n2, n3), getJoinability(n3, m4))),
                asList(s, asList(getJoinability(n1, n2), getJoinability(n2, n3), getJoinability(n3, m4), getJoinability(m4, n5))),
                /* same as above, but in reversal join order (more tha Collections.reverse()) */
                asList(s, singletonList(getJoinability(m2, n1))),
                asList(s, asList(getJoinability(n3, m2), getJoinability(m2, n1))),
                asList(s, asList(getJoinability(n3, n2), getJoinability(n2, n1))),
                asList(s, asList(getJoinability(n4, m3), getJoinability(m3, n2), getJoinability(n2, n1))),
                asList(s, asList(getJoinability(m4, n3), getJoinability(n3, n2), getJoinability(n2, n1))),
                asList(s, asList(getJoinability(n5, m4), getJoinability(m4, n3), getJoinability(n3, n2), getJoinability(n2, n1))),
                /* Same as the two previous blocks, now using getPlainJoinability */
                /*   - test cases exploring removal of equivalent query nodes within MultiQueryNodes */
                asList(s, singletonList(getJoinability(n1, m2))),
                asList(s, asList(getJoinability(n1, m2), getJoinability(m2, n3))),
                asList(s, asList(getJoinability(n1, n2), getJoinability(n2, m3))),
                asList(s, asList(getJoinability(n1, n2), getJoinability(n2, m3), getJoinability(m3, n4))),
                asList(s, asList(getJoinability(n1, n2), getJoinability(n2, n3), getJoinability(n3, m4))),
                asList(s, asList(getJoinability(n1, n2), getJoinability(n2, n3), getJoinability(n3, m4), getJoinability(m4, n5))),
                /*   - same as above, but in reversal join order (more tha Collections.reverse()) */
                asList(s, singletonList(getJoinability(m2, n1))),
                asList(s, asList(getJoinability(n3, m2), getJoinability(m2, n1))),
                asList(s, asList(getJoinability(n3, n2), getJoinability(n2, n1))),
                asList(s, asList(getJoinability(n4, m3), getJoinability(m3, n2), getJoinability(n2, n1))),
                asList(s, asList(getJoinability(m4, n3), getJoinability(n3, n2), getJoinability(n2, n1))),
                asList(s, asList(getJoinability(n5, m4), getJoinability(m4, n3), getJoinability(n3, n2), getJoinability(n2, n1))),

                /* plan with optionals */
                /*  - single optional node */
                asList(s, singletonList(getJoinability(n1o, n2))),
                /*  - two optional nodes: join becomes optional */
                asList(s, singletonList(getJoinability(n1o, n2o))),
                /*  - three optional nodes */
                asList(s, asList(getJoinability(n1o, n2o), getJoinability(n2o, n3o))),
                /*  - two optionals and one non-optiona. n1 should be the left-most node */
                asList(s, asList(getJoinability(n1, n2o), getJoinability(n2o, n3o)))
                )).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "planData")
    public void testPlanGivenNodes(Supplier<JoinOrderPlanner> supplier, List<JoinInfo> list) {
        JoinOrderPlanner planner = supplier.get();
        Set<Op> leavesSet = getPlanNodes(list);
        JoinGraph joinGraph = new ArrayJoinGraph(RefIndexSet.fromRefDistinct(leavesSet));
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
        RefIndexSet<Op> leaves = RefIndexSet.fromRefDistinct(asList(
                contractorByName, procurementsOfContractor, contractById, modalities,
                procurementById, orgByDesc, contract));
        JoinGraph graph = new ArrayJoinGraph(leaves);
        assertEquals(graph.size(), 7);

        Op plan = planner.plan(graph, new ArrayList<>(leaves));
        checkPlan(plan, leaves);
    }

}