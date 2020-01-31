package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.TreeUtils;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.impl.EmptyEndpoint;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.federation.planner.impl.JoinInfo.getPlainJoinability;
import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.isTree;
import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.streamPreOrder;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class JoinOrderPlannerTest {
    private static final URI Alice = new StdURI("http://example.org/Alice");
    private static final URI Bob = new StdURI("http://example.org/Bob");
    private static final URI p1 = new StdURI("http://example.org/p1");
    private static final URI p2 = new StdURI("http://example.org/p2");
    private static final URI p3 = new StdURI("http://example.org/p3");
    private static final URI p4 = new StdURI("http://example.org/p4");
    private static final URI p5 = new StdURI("http://example.org/p4");
    private static final Var x = new StdVar("x");
    private static final Var y = new StdVar("y");
    private static final Var z = new StdVar("z");
    private static final Var w = new StdVar("w");

    private static EmptyEndpoint ep = new EmptyEndpoint();

    public static final List<Supplier<JoinOrderPlanner>> suppliers =
            singletonList(new NamedSupplier<>(ArbitraryJoinOrderPlanner.class));

    @DataProvider
    public static Object[][] planData() {
        QueryNode n1 = new QueryNode(ep, CQuery.from(new Triple(Alice, p1, x)));
        QueryNode n2 = new QueryNode(ep, CQuery.from(new Triple(x, p2, y)));
        QueryNode n3 = new QueryNode(ep, CQuery.from(new Triple(y, p3, z)));
        QueryNode n4 = new QueryNode(ep, CQuery.from(new Triple(z, p4, w)));
        QueryNode n5 = new QueryNode(ep, CQuery.from(new Triple(w, p5, Bob)));

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
                asList(s, asList(getPlainJoinability(n4, n5), getPlainJoinability(n3, n4), getPlainJoinability(n2, n3), getPlainJoinability(n1, n2)))
        )).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "planData")
    public void testPlan(Supplier<JoinOrderPlanner> supplier, List<JoinInfo> list) {
        JoinOrderPlanner planner = supplier.get();
        PlanNode root = planner.plan(list);
        assertTrue(isTree(root));

        Set<PlanNode> leaves = streamPreOrder(root)
                .filter(n -> !(n instanceof JoinNode)).collect(toSet());
        Set<PlanNode> expectedLeaves = list.stream()
                .flatMap(i -> Stream.of(i.getLeft(), i.getRight())).collect(toSet());
        assertEquals(leaves, expectedLeaves);

        Set<PlanNode> nonJoinInner = streamPreOrder(root).filter(n -> !leaves.contains(n))
                .filter(n -> !(n instanceof JoinNode)).collect(toSet());
        assertEquals(nonJoinInner, emptySet());

        Set<Triple> allTriples = expectedLeaves.stream().map(PlanNode::getMatchedTriples)
                .reduce(TreeUtils::union).orElse(emptySet());
        assertEquals(root.getMatchedTriples(), allTriples);
    }

}