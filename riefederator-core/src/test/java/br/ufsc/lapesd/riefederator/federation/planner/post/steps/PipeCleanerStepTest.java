package br.ufsc.lapesd.riefederator.federation.planner.post.steps;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.PipeOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.EmptyRefSet;
import org.testng.annotations.Test;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

public class PipeCleanerStepTest implements TestContext {
    private CQEndpoint e1 = new EmptyEndpoint(), e2 = new EmptyEndpoint();

    @Test
    public void testNoPipes() {
        JoinOp op = JoinOp.create(new EndpointQueryOp(e1, createQuery(x, knows, y)),
                                  new EndpointQueryOp(e1, createQuery(y, age, u)));
        Op expected = TreeUtils.deepCopy(op);
        Op plan = new PipeCleanerStep().plan(op, EmptyRefSet.emptySet());
        assertSame(plan, op);
        assertEquals(plan, expected);
    }

    @Test
    public void testRemoveSinglePipe() {
        PipeOp pipe = new PipeOp(new EndpointQueryOp(e1, createQuery(y, age, v)));
        pipe.modifiers().add(SPARQLFilter.build("?v > 23"));
        Op root = CartesianOp.builder()
                .add(new EndpointQueryOp(e1, createQuery(x, age, u)))
                .add(pipe)
                .add(SPARQLFilter.build("?u > ?v"))
                .build();
        Op plan = new PipeCleanerStep().plan(root, EmptyRefSet.emptySet());
        assertSame(root, plan);
        assertEquals(plan, CartesianOp.builder()
                .add(new EndpointQueryOp(e1, createQuery(x, age, u)))
                .add(new EndpointQueryOp(e1, createQuery(y, age, v, SPARQLFilter.build("?v > 23"))))
                .add(SPARQLFilter.build("?u > ?v"))
                .build());
    }

    @Test
    public void testNoEffectTwoPipes() {
        EndpointQueryOp queryOp = new EndpointQueryOp(e1, createQuery(x, age, u));
        PipeOp p1 = new PipeOp(queryOp), p2 = new PipeOp(queryOp);
        p1.modifiers().add(SPARQLFilter.build("?u < 23"));
        p2.modifiers().add(SPARQLFilter.build("?u > 23"));
        Op root = UnionOp.builder().add(p1).add(p2).build();

        Op expected = TreeUtils.deepCopy(root);
        Op plan = new PipeCleanerStep().plan(root, EmptyRefSet.emptySet());
        assertSame(plan, root);
        assertSame(plan.getChildren().get(0), root.getChildren().get(0));
        assertSame(plan.getChildren().get(1), root.getChildren().get(1));
        assertEquals(plan, expected);
    }

    @Test
    public void testRemoveEqualPipes() {
        EndpointQueryOp queryOp = new EndpointQueryOp(e1, createQuery(x, age, u));
        PipeOp p1 = new PipeOp(queryOp), p2 = new PipeOp(queryOp);
        p1.modifiers().add(SPARQLFilter.build("?u > 23"));
        p2.modifiers().add(SPARQLFilter.build("?u > 23"));

        Op root = UnionOp.builder()
                .add(JoinOp.create(p1, new EndpointQueryOp(e1, createQuery(x, knows, y))))
                .add(JoinOp.create(p2, new EndpointQueryOp(e2, createQuery(x, knows, y))))
                .build();

        Op plan = new PipeCleanerStep().plan(root, EmptyRefSet.emptySet());
        assertSame(plan, root);
        assertSame(plan.getChildren().get(0), root.getChildren().get(0));
        assertSame(plan.getChildren().get(0).getChildren().get(0), queryOp);
        assertSame(plan.getChildren().get(1), root.getChildren().get(1));
        assertSame(plan.getChildren().get(1).getChildren().get(0), queryOp);

        assertEquals(plan, UnionOp.builder()
                .add(JoinOp.create(queryOp, new EndpointQueryOp(e1, createQuery(x, knows, y))))
                .add(JoinOp.create(queryOp, new EndpointQueryOp(e2, createQuery(x, knows, y))))
                .build());
    }
}