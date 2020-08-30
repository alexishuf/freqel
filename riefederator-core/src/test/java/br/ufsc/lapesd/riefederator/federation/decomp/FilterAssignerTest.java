package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;

import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static br.ufsc.lapesd.riefederator.util.CollectionUtils.intersect;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class FilterAssignerTest implements TestContext {
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

        CQuery fullQuery = createQuery(Alice, knows, x, x, knows, y, y, age, u);
        FilterAssigner assigner = new FilterAssigner(fullQuery);

        // place on leaves
        List<EndpointQueryOp> queryOps = asList(q1, q2, q3);
        List<ProtoQueryOp> prototypes;
        prototypes = queryOps.stream().map(ProtoQueryOp::new).collect(toList());

        List<EndpointQueryOp> annotated = assigner.placeFiltersOnLeaves(prototypes).stream()
                .map(n -> (EndpointQueryOp)n).collect(toList());
        assertEquals(annotated.size(), 3);
        assertTrue(annotated.stream().noneMatch(Objects::isNull));
        for (int i = 0; i < annotated.size(); i++)
            assertEquals(annotated.get(i).getQuery(), queryOps.get(i).getQuery());

        assigner.placeBottommost(root);
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
        FilterAssigner assigner = new FilterAssigner(query);
        assigner.placeBottommost(node);
        checkAllFilters(query.getModifiers().filters(), node);
    }

    @Test
    public void testFilterOnMultiQueryOfAnnotated() {
        CQuery query = createQuery(
                Alice, knows, x,
                x,     age,   u,
                SPARQLFilter.build("?u > 23"));
        List<ProtoQueryOp> prototypes = asList(
                new ProtoQueryOp(ep1, query),
                new ProtoQueryOp(ep2, query)
        );

        FilterAssigner assigner = new FilterAssigner(query);
        List<EndpointQueryOp> queryOps = assigner.placeFiltersOnLeaves(prototypes).stream()
                .map(n -> (EndpointQueryOp)n).collect(toList());
        for (EndpointQueryOp queryOp : queryOps)
            checkAllFilters(query.getModifiers().filters(), queryOp);

        Op multi = UnionOp.builder().addAll(queryOps).build();
        assigner.placeBottommost(multi);
        checkAllFilters(query.getModifiers().filters(), multi);
    }

    @Test
    public void testApplyOnJoin() {
        SPARQLFilter annEquals, annGreater;
        annEquals = SPARQLFilter.builder("iri(?x) != iri(?y)").map(x).map(y).build();
        annGreater = SPARQLFilter.builder("?u > ?v").map(u).map(v).build();
        CQuery fullQuery = createQuery(
                Alice, knows,   x,
                x,     manages, y, annEquals,
                x,     age,     u,
                y,     age,     v, annGreater);
        List<ProtoQueryOp> prototypes = asList(
                new ProtoQueryOp(ep1, createQuery(Alice, knows, x)),
                new ProtoQueryOp(ep1, createQuery(x, manages, y, x, age, u)),
                new ProtoQueryOp(ep2, createQuery(x, manages, y, y, age, v))
        );

        FilterAssigner assigner = new FilterAssigner(fullQuery);
        List<EndpointQueryOp> queryOps = assigner.placeFiltersOnLeaves(prototypes).stream()
                .map(n -> (EndpointQueryOp)n).collect(toList());

        assertEquals(queryOps.size(), prototypes.size());
        assertTrue(queryOps.stream().noneMatch(Objects::isNull));
        for (int i = 0; i < queryOps.size(); i++)
            assertEquals(prototypes.get(i).getMatchedQuery(), queryOps.get(i).getQuery());
        assertEquals(queryOps.get(0).modifiers().filters(), emptySet());
        assertEquals(queryOps.get(1).modifiers().filters(), singleton(annEquals));
        assertEquals(queryOps.get(2).modifiers().filters(), singleton(annEquals));

        JoinOp joinLeft  = JoinOp.create(queryOps.get(1), queryOps.get(2));
        JoinOp joinRight = JoinOp.create(queryOps.get(1), queryOps.get(2));
        JoinOp leftDeep  = JoinOp.create(joinLeft,          queryOps.get(0));
        JoinOp rightDeep = JoinOp.create(queryOps.get(0), joinRight        );

        assigner.placeBottommost(leftDeep);
        assigner.placeBottommost(rightDeep);

        assertEquals(joinLeft.modifiers().filters(), singleton(annGreater));
        assertEquals(joinRight.modifiers().filters(), singleton(annGreater));
        checkAllFilters(fullQuery.getModifiers().filters(), rightDeep);
        checkAllFilters(fullQuery.getModifiers().filters(), leftDeep);
    }
}