package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.federation.planner.PlannerTest;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.federation.tree.*;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
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

import static br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo.getMultiJoinability;
import static br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo.getPlainJoinability;
import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.*;
import static br.ufsc.lapesd.riefederator.query.CQueryContext.createQuery;
import static br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation.asRequired;
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

    private static EmptyEndpoint e1 = new EmptyEndpoint(), e1a = new EmptyEndpoint(),
                                 e2 = new EmptyEndpoint();

    private static Atom Person = new Atom("Person");

    static {
        e1.addAlternative(e1a);
    }

    public static final List<Supplier<JoinOrderPlanner>> suppliers =
            asList(
                    new NamedSupplier<>(ArbitraryJoinOrderPlanner.class),
                    new NamedSupplier<>(GreedyJoinOrderPlanner.class)
            );

    private void checkPlan(PlanNode root, Set<PlanNode> expectedLeaves) {
        assertTrue(isTree(root)); //stricter than acyclic

        Set<Triple> allTriples = expectedLeaves.stream().map(PlanNode::getMatchedTriples)
                .reduce(TreeUtils::union).orElse(emptySet());

        // more general tests from PlannerTest
        CQuery query = CQuery.builder().addAll(allTriples).build();
        PlannerTest.assertPlanAnswers(root, query);

        // no duplicate leaves (QueryNodes)
        List<PlanNode> leaves = streamPreOrder(root)
                .filter(n -> n instanceof QueryNode).collect(toList());
        assertEquals(leaves.size(), new HashSet<>(leaves).size());

        // all leaf QueryNode was present in expectedLeaves
        Set<PlanNode> expectedQNLeaves = expectedLeaves.stream()
                .flatMap(n -> childrenIfMulti(n).stream()).collect(toSet());
        assertTrue(expectedQNLeaves.containsAll(leaves)); //no made-up QueryNodes

        // every QueryNode from expectedQNLeaves missing from leaves must be equivalent to another
        List<PlanNode> missingLeaves = expectedQNLeaves.stream().filter(qn -> !leaves.contains(qn))
                .filter(missing -> {
                    Set<Triple> triples = ((QueryNode) missing).getQuery().getSet();
                    TPEndpoint ep = ((QueryNode) missing).getEndpoint();
                    for (PlanNode leaf : leaves) {
                        QueryNode leafQN = (QueryNode) leaf;
                        TPEndpoint candidate = leafQN.getEndpoint();
                        if (leafQN.getQuery().getSet().equals(triples) &&
                                (candidate.isAlternative(ep) || ep.isAlternative(candidate))) {
                            return false;
                        }
                    }
                    return true;
                }).collect(toList());
        assertEquals(missingLeaves, emptyList());

        // nothing but JoinNodes should be introduced
        Set<PlanNode> nonJoinInner = streamPreOrder(root).filter(n -> !leaves.contains(n))
                .filter(n -> !(n instanceof JoinNode) && !expectedLeaves.contains(n))
                .collect(toSet());
        assertEquals(nonJoinInner, emptySet());
    }

    private @Nonnull Set<PlanNode> getPlanNodes(List<JoinInfo> list) {
        Set<PlanNode> expectedLeaves;
        expectedLeaves = list.stream().flatMap(i -> i.getNodes().stream()).collect(toSet());
        return expectedLeaves;
    }

    private boolean isBadPath(List<JoinInfo> list) {
        return list.stream().anyMatch(i -> !i.isValid());
    }


    @DataProvider
    public static Object[][] planData() {
        QueryNode n1 = new QueryNode(e1, createQuery(Alice, p1, x));
        QueryNode n2 = new QueryNode(e1, createQuery(x, p2, y));
        QueryNode n3 = new QueryNode(e1, createQuery(y, p3, z));
        QueryNode n4 = new QueryNode(e1, createQuery(z, p4, w));
        QueryNode n5 = new QueryNode(e1, createQuery(w, p5, Bob));
        QueryNode n6 = new QueryNode(e1, createQuery(w, p6, Bob));
        QueryNode n7 = new QueryNode(e1, createQuery(z, p7, x));

        QueryNode n2i = new QueryNode(e1, CQuery.with(new Triple(x, p2, y))
                .annotate(x, asRequired(Person, "Person")).build());
        QueryNode n3a = new QueryNode(e1a, createQuery(y, p3, z));
        QueryNode n4b  = new QueryNode(e2, createQuery(z, p4, w));

        MultiQueryNode m2 = MultiQueryNode.builder().add(n2).add(n2i).build();
        MultiQueryNode m3 = MultiQueryNode.builder().add(n3).add(n3a).build();
        MultiQueryNode m4 = MultiQueryNode.builder().add(n4).add(n4b).build();

        return suppliers.stream().flatMap(s -> Stream.of(
                asList(s, singletonList(getPlainJoinability(n1, n2))),
                asList(s, singletonList(getPlainJoinability(n2, n4))),
                asList(s, asList(getPlainJoinability(n1, n2), getPlainJoinability(n2, n3))),
                asList(s, asList(getPlainJoinability(n4, n2), getPlainJoinability(n2, n1))),
                asList(s, asList(getPlainJoinability(n2, n3), getPlainJoinability(n1, n2))),
                asList(s, asList(getPlainJoinability(n1, n2), getPlainJoinability(n2, n3), getPlainJoinability(n3, n4))),
                asList(s, asList(getPlainJoinability(n4, n3), getPlainJoinability(n3, n2), getPlainJoinability(n2, n1))),
                asList(s, asList(getPlainJoinability(n3, n4), getPlainJoinability(n2, n3), getPlainJoinability(n1, n2))),
                asList(s, asList(getPlainJoinability(n1, n2), getPlainJoinability(n2, n3), getPlainJoinability(n3, n4), getPlainJoinability(n4, n5))),
                asList(s, asList(getPlainJoinability(n5, n4), getPlainJoinability(n4, n3), getPlainJoinability(n3, n2), getPlainJoinability(n2, n1))),
                asList(s, asList(getPlainJoinability(n4, n5), getPlainJoinability(n3, n4), getPlainJoinability(n2, n3), getPlainJoinability(n1, n2))),
                asList(s, asList(getPlainJoinability(n6, n5), getPlainJoinability(n4, n5), getPlainJoinability(n4, n7), getPlainJoinability(n3, n4), getPlainJoinability(n2, n3), getPlainJoinability(n1, n2))),
                asList(s, asList(getPlainJoinability(n1, n2), getPlainJoinability(n2, n3), getPlainJoinability(n3, n4), getPlainJoinability(n4, n7), getPlainJoinability(n4, n5), getPlainJoinability(n5, n6))),
                /* test cases exploring removal of equivalent query nodes within MultiQueryNodes */
                asList(s, singletonList(getMultiJoinability(n1, m2))),
                asList(s, asList(getMultiJoinability(n1, m2), getMultiJoinability(m2, n3))),
                asList(s, asList(getMultiJoinability(n1, n2), getMultiJoinability(n2, m3))),
                asList(s, asList(getMultiJoinability(n1, n2), getMultiJoinability(n2, m3), getMultiJoinability(m3, n4))),
                asList(s, asList(getMultiJoinability(n1, n2), getMultiJoinability(n2, n3), getMultiJoinability(n3, m4))),
                asList(s, asList(getMultiJoinability(n1, n2), getMultiJoinability(n2, n3), getMultiJoinability(n3, m4), getMultiJoinability(m4, n5))),
                /* same as above, but in reversal join order (more tha Collections.reverse()) */
                asList(s, singletonList(getMultiJoinability(m2, n1))),
                asList(s, asList(getMultiJoinability(n3, m2), getMultiJoinability(m2, n1))),
                asList(s, asList(getMultiJoinability(n3, n2), getMultiJoinability(n2, n1))),
                asList(s, asList(getMultiJoinability(n4, m3), getMultiJoinability(m3, n2), getMultiJoinability(n2, n1))),
                asList(s, asList(getMultiJoinability(m4, n3), getMultiJoinability(n3, n2), getMultiJoinability(n2, n1))),
                asList(s, asList(getMultiJoinability(n5, m4), getMultiJoinability(m4, n3), getMultiJoinability(n3, n2), getMultiJoinability(n2, n1))),
                /* Same as the two previous blocks, now using getPlainJoinability */
                /*   - test cases exploring removal of equivalent query nodes within MultiQueryNodes */
                asList(s, singletonList(getPlainJoinability(n1, m2))),
                asList(s, asList(getPlainJoinability(n1, m2), getPlainJoinability(m2, n3))),
                asList(s, asList(getPlainJoinability(n1, n2), getPlainJoinability(n2, m3))),
                asList(s, asList(getPlainJoinability(n1, n2), getPlainJoinability(n2, m3), getPlainJoinability(m3, n4))),
                asList(s, asList(getPlainJoinability(n1, n2), getPlainJoinability(n2, n3), getPlainJoinability(n3, m4))),
                asList(s, asList(getPlainJoinability(n1, n2), getPlainJoinability(n2, n3), getPlainJoinability(n3, m4), getPlainJoinability(m4, n5))),
                /*   - same as above, but in reversal join order (more tha Collections.reverse()) */
                asList(s, singletonList(getPlainJoinability(m2, n1))),
                asList(s, asList(getPlainJoinability(n3, m2), getPlainJoinability(m2, n1))),
                asList(s, asList(getPlainJoinability(n3, n2), getPlainJoinability(n2, n1))),
                asList(s, asList(getPlainJoinability(n4, m3), getPlainJoinability(m3, n2), getPlainJoinability(n2, n1))),
                asList(s, asList(getPlainJoinability(m4, n3), getPlainJoinability(n3, n2), getPlainJoinability(n2, n1))),
                asList(s, asList(getPlainJoinability(n5, m4), getPlainJoinability(m4, n3), getPlainJoinability(n3, n2), getPlainJoinability(n2, n1)))
                )).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "planData")
    public void testPlanGivenNodes(Supplier<JoinOrderPlanner> supplier, List<JoinInfo> list) {
        JoinOrderPlanner planner = supplier.get();
        Set<PlanNode> leavesSet = getPlanNodes(list);
        JoinGraph joinGraph = new JoinGraph(IndexedSet.from(leavesSet));
        int rounds = 0;
        //noinspection UnstableApiUsage
        for (List<PlanNode> permutation : Collections2.permutations(new ArrayList<>(leavesSet))) {
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


    @Test(dataProvider = "suppliersData")
    public void testPlanGivenNodesNonLinearPath(Supplier<JoinOrderPlanner> supplier) {
        QueryNode orgByDesc = new QueryNode(e1, createQuery(o1, p1, t));
        QueryNode contract = new QueryNode(e1, CQuery.with(
                new Triple(o2, p2, b),
                new Triple(o2, p3, c),
                new Triple(o2, p4, t)
        ).annotate(t, asRequired(new Atom("A1"), "A1")).build());
        QueryNode contractById = new QueryNode(e1, CQuery.with(
                new Triple(b, p5, o3)
        ).annotate(b, asRequired(new Atom("A2"), "A2")).build());
        QueryNode contractorByName = new QueryNode(e1, CQuery.with(
                new Triple(c, p6, s)
        ).annotate(c, asRequired(new Atom("A3"), "A3")).build());
        QueryNode procurementsOfContractor = new QueryNode(e1, CQuery.with(
                new Triple(s, p7, a)
        ).annotate(s, asRequired(new Atom("A4"), "A4")).build());
        QueryNode procurementById = new QueryNode(e1, CQuery.with(
                new Triple(a, p8, d)
        ).annotate(a, asRequired(new Atom("A5"), "A5")).build());
        QueryNode modalities = new QueryNode(e1, createQuery(o4, p9, d));

        JoinOrderPlanner planner = supplier.get();
        IndexedSet<PlanNode> leaves = IndexedSet.from(asList(
                contractorByName, procurementsOfContractor, contractById, modalities,
                procurementById, orgByDesc, contract));
        JoinGraph graph = new JoinGraph(leaves);
        assertEquals(graph.size(), 7);

        PlanNode plan = planner.plan(graph, new ArrayList<>(leaves));
        checkPlan(plan, leaves);
    }

}