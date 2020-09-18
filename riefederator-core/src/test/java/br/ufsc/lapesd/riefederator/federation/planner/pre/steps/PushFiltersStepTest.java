package br.ufsc.lapesd.riefederator.federation.planner.pre.steps;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.EmptyRefSet;
import br.ufsc.lapesd.riefederator.util.IdentityHashSet;
import br.ufsc.lapesd.riefederator.util.RefSet;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static br.ufsc.lapesd.riefederator.util.CollectionUtils.intersect;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class PushFiltersStepTest implements TestContext {
    private @Nonnull CQEndpoint ep1 = new EmptyEndpoint(), ep2 = new EmptyEndpoint();

    private void checkAllFilters(@Nonnull Set<SPARQLFilter> filters,
                                 @Nonnull Op root) {
        Set<SPARQLFilter> pendingFilters = new HashSet<>(filters);

        ArrayDeque<List<Op>> stack = new ArrayDeque<>();
        stack.push(Collections.singletonList(root));
        while (!stack.isEmpty()) {
            List<Op> path = stack.pop();
            Op node = path.get(path.size() - 1);
            pendingFilters.removeAll(node.modifiers().filters());
            // no filter is present in any ancestor
            for (Op ancestor : path.subList(0, path.size() - 1)) {
                assertEquals(intersect(node.modifiers().filters(), ancestor.modifiers().filters()),
                        emptySet());
            }
            // queue one path for each child
            for (Op child : node.getChildren()) {
                ArrayList<Op> list = new ArrayList<>(path);
                list.add(child);
                stack.push(list);
            }
        }

        assertEquals(pendingFilters, emptySet(), "Some filters were not observed in the plan");
    }

    @Test
    public void testNoFilter() {
        EndpointQueryOp q1 = new EndpointQueryOp(ep1, createQuery(Alice, knows, x));
        EndpointQueryOp q2 = new EndpointQueryOp(ep1, createQuery(x, knows, y));
        EndpointQueryOp q3 = new EndpointQueryOp(ep1, createQuery(y, age, u));
        JoinOp j1 = JoinOp.create(q1, q2);
        JoinOp root = JoinOp.create(j1, q3);

        HashSet<SPARQLFilter> set = new HashSet<>(root.modifiers().filters());
        Op replacement = new PushFiltersStep().plan(root, EmptyRefSet.emptySet());
        checkAllFilters(set, replacement);
    }

    @Test
    public void testFilterAlreadyPresentInUnion() {
        CQuery query = createQuery(
                Alice, knows, x,
                x,     age,   u,
                SPARQLFilter.build("?u > 23"));
        Op node = UnionOp.builder()
                .add(new EndpointQueryOp(ep1, query))
                .add(new EndpointQueryOp(ep2, query))
                .build();
        PushFiltersStep step = new PushFiltersStep();
        Op replacement = step.plan(node, EmptyRefSet.emptySet());
        checkAllFilters(singleton(SPARQLFilter.build("?u > 23")), replacement);
    }

    @Test
    public void testDistributeFiltersInUnion() {
        CQuery query = createQuery(
                Alice, knows, x,
                x,     age,   u);
        Op node = UnionOp.builder()
                .add(new EndpointQueryOp(ep1, query))
                .add(new EndpointQueryOp(ep2, query))
                .add(SPARQLFilter.build("?u > 23"))
                .build();
        PushFiltersStep step = new PushFiltersStep();
        Op replacement = step.plan(node, EmptyRefSet.emptySet());
        checkAllFilters(singleton(SPARQLFilter.build("?u > 23")), replacement);
    }

    @Test
    public void testApplyOnJoin() {
        SPARQLFilter eq, greater;
        eq = SPARQLFilter.build("iri(?x) != iri(?y)");
        greater = SPARQLFilter.build("?u > ?v");

        List<EndpointQueryOp> leaves = asList(
                new EndpointQueryOp(ep1, createQuery(Alice, knows, x)),
                new EndpointQueryOp(ep1, createQuery(x, manages, y, y, age, u)),
                new EndpointQueryOp(ep1, createQuery(x, manages, y, y, age, v))
        );
        JoinOp ltr = JoinOp.create(leaves.get(0), JoinOp.create(leaves.get(1), leaves.get(2)));
        ltr.modifiers().add(eq);
        ltr.modifiers().add(greater);
        JoinOp rtl = JoinOp.create(leaves.get(2), JoinOp.create(leaves.get(1), leaves.get(0)));
        rtl.modifiers().add(eq);
        rtl.modifiers().add(greater);

        PushFiltersStep step = new PushFiltersStep();
        checkAllFilters(Sets.newHashSet(eq, greater), step.plan(ltr, EmptyRefSet.emptySet()));
        checkAllFilters(Sets.newHashSet(eq, greater), step.plan(rtl, EmptyRefSet.emptySet()));
    }

    @Test
    public void testApplyOnProduct() {
        Op op = CartesianOp.builder()
                .add(new EndpointQueryOp(ep1, createQuery(Alice, knows, x)))
                .add(new EndpointQueryOp(ep1, createQuery(Alice, age, u)))
                .add(SPARQLFilter.build("?u > 23"))
                .build();
        Op replacement = new PushFiltersStep().plan(op, EmptyRefSet.emptySet());
        checkAllFilters(singleton(SPARQLFilter.build("?u > 23")), replacement);
    }

    @Test
    public void testPushOnIncomplete() {
        JoinOp op = JoinOp.create(
                CartesianOp.builder()
                        .add(new EndpointQueryOp(ep1, createQuery(Alice, knows, x)))
                        .add(new EndpointQueryOp(ep1, createQuery(Alice, age, u)))
                        .build(),
                new EndpointQueryOp(ep1, createQuery(y, age, u, SPARQLFilter.build("?u > 23"))));
        op.modifiers().add(SPARQLFilter.build("?u > 23"));

        Op replacement = new PushFiltersStep().plan(op, EmptyRefSet.emptySet());
        assertSame(replacement, op);
        checkAllFilters(singleton(SPARQLFilter.build("?u > 23")), replacement);
        assertEquals(op.getChildren().get(0).getChildren().get(1).modifiers().filters(),
                     singleton(SPARQLFilter.build("?u > 23")));
        assertEquals(op.getChildren().get(1).modifiers().filters(),
                singleton(SPARQLFilter.build("?u > 23")));
    }

    @Test
    public void testAddPipeOp() {
        JoinOp op = JoinOp.create(new EndpointQueryOp(ep1, createQuery(x, knows, y)),
                                  new EndpointQueryOp(ep1, createQuery(y, age, u)));
        op.modifiers().add(SPARQLFilter.build("?u > 23"));
        Op lockedNode = op.getRight();
        RefSet<Op> lockedSet = IdentityHashSet.of(lockedNode);

        Op replacement = new PushFiltersStep().plan(op, lockedSet);
        assertSame(replacement, op);
        checkAllFilters(singleton(SPARQLFilter.build("?u > 23")), replacement);

        assertNotSame(((JoinOp)replacement).getRight(), lockedNode);
        assertEquals(((JoinOp) replacement).getRight().modifiers().filters(),
                     singleton(SPARQLFilter.build("?u > 23")));
    }
}