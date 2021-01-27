package br.ufsc.lapesd.freqel.federation.decomp;

import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.freqel.query.modifiers.SPARQLFilter;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;

import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static br.ufsc.lapesd.freqel.util.CollectionUtils.intersect;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class FilterAssignerTest implements TestContext {
    private final @Nonnull CQEndpoint ep1 = new EmptyEndpoint(), ep2 = new EmptyEndpoint();

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

        CQuery fullQuery = createQuery(Alice, knows, x, x, knows, y, y, age, u);

        // place on leaves
        List<EndpointQueryOp> queryOps = asList(q1, q2, q3);

        FilterAssigner.placeFiltersOnLeaves(queryOps, fullQuery.getModifiers().filters());
        assertEquals(queryOps.size(), 3);
        assertTrue(queryOps.stream().noneMatch(Objects::isNull));

        FilterAssigner.placeInnerBottommost(root, fullQuery.getModifiers().filters());
        checkAllFilters(fullQuery.getModifiers().filters(), root);
    }

    @Test
    public void testFilterOnMultiQuery() {
        CQuery query = createQuery(
                Alice, knows, x,
                x,     age,   u,
                SPARQLFilter.build("?u > 23"));
        Op node = UnionOp.builder()
                .add(new EndpointQueryOp(ep1, query))
                .add(new EndpointQueryOp(ep2, query))
                .build();
        FilterAssigner.placeInnerBottommost(node, query.getModifiers().filters());
        checkAllFilters(query.getModifiers().filters(), node);
    }

    @Test
    public void testFilterOnMultiQueryOfAnnotated() {
        CQuery query = createQuery(
                Alice, knows, x,
                x,     age,   u,
                SPARQLFilter.build("?u > 23"));
        List<Op> leaves = asList(
                new EndpointQueryOp(ep1, query),
                new EndpointQueryOp(ep2, query)
        );

        FilterAssigner.placeFiltersOnLeaves(leaves, query.getModifiers().filters());
        for (Op queryOp : leaves)
            checkAllFilters(query.getModifiers().filters(), queryOp);

        Op multi = UnionOp.builder().addAll(leaves).build();
        FilterAssigner.placeInnerBottommost(multi, query.getModifiers().filters());
        checkAllFilters(query.getModifiers().filters(), multi);
    }

    @Test
    public void testApplyOnJoin() {
        SPARQLFilter annEquals, annGreater;
        annEquals = SPARQLFilter.build("iri(?x) != iri(?y)");
        annGreater = SPARQLFilter.build("?u > ?v");
        CQuery fullQuery = createQuery(
                Alice, knows,   x,
                x,     manages, y, annEquals,
                x,     age,     u,
                y,     age,     v, annGreater);
        List<Op> nodes = asList(
                new EndpointQueryOp(ep1, createQuery(Alice, knows, x)),
                new EndpointQueryOp(ep1, createQuery(x, manages, y, x, age, u)),
                new EndpointQueryOp(ep2, createQuery(x, manages, y, y, age, v))
        );

        FilterAssigner.placeFiltersOnLeaves(nodes, fullQuery.getModifiers().filters());

        assertEquals(nodes.size(), nodes.size());
        assertTrue(nodes.stream().noneMatch(Objects::isNull));
        assertEquals(nodes.get(0).modifiers().filters(), emptySet());
        assertEquals(nodes.get(1).modifiers().filters(), singleton(annEquals));
        assertEquals(nodes.get(2).modifiers().filters(), singleton(annEquals));

        JoinOp joinLeft  = JoinOp.create(nodes.get(1), nodes.get(2));
        JoinOp joinRight = JoinOp.create(nodes.get(1), nodes.get(2));
        JoinOp leftDeep  = JoinOp.create(joinLeft,          nodes.get(0));
        JoinOp rightDeep = JoinOp.create(nodes.get(0), joinRight        );

        FilterAssigner.placeInnerBottommost(leftDeep, fullQuery.getModifiers().filters());
        FilterAssigner.placeInnerBottommost(rightDeep, fullQuery.getModifiers().filters());

        assertEquals(joinLeft.modifiers().filters(), singleton(annGreater));
        assertEquals(joinRight.modifiers().filters(), singleton(annGreater));
        checkAllFilters(fullQuery.getModifiers().filters(), rightDeep);
        checkAllFilters(fullQuery.getModifiers().filters(), leftDeep);
    }
}