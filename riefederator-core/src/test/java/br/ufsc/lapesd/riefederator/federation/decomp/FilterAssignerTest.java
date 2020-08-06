package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.proto.ProtoQueryNode;
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

    private void checkAllFilters(@Nonnull CQuery query, @Nonnull PlanNode root) {
        Set<SPARQLFilter> modifiers = new HashSet<>();
        query.getModifiers().stream().filter(SPARQLFilter.class::isInstance)
                .forEach(m -> modifiers.add((SPARQLFilter)m));

        ArrayDeque<List<PlanNode>> stack = new ArrayDeque<>();
        stack.push(Collections.singletonList(root));
        while (!stack.isEmpty()) {
            List<PlanNode> path = stack.pop();
            PlanNode node = path.get(path.size() - 1);
            modifiers.removeAll(node.getFilters());
            // no filter is present in any ancestor
            for (PlanNode ancestor : path.subList(0, path.size() - 1)) {
                assertEquals(intersect(node.getFilters(), ancestor.getFilters()), emptySet());
            }
            // queue one path for each child
            for (PlanNode child : node.getChildren()) {
                ArrayList<PlanNode> list = new ArrayList<>(path);
                list.add(child);
                stack.push(list);
            }
        }

        assertEquals(modifiers, emptySet(), "Some filters were not observed in the plan");
    }

    @Test
    public void testNoFilter() {
        QueryNode q1 = new QueryNode(ep1, createQuery(Alice, knows, x));
        QueryNode q2 = new QueryNode(ep1, createQuery(x, knows, y));
        QueryNode q3 = new QueryNode(ep1, createQuery(y, age, u));
        JoinNode j1 = JoinNode.builder(q1, q2).build();
        JoinNode root = JoinNode.builder(j1, q3).build();

        CQuery fullQuery = createQuery(Alice, knows, x, x, knows, y, y, age, u);
        FilterAssigner assigner = new FilterAssigner(fullQuery);

        // place on leaves
        List<QueryNode> queryNodes = asList(q1, q2, q3);
        List<ProtoQueryNode> prototypes;
        prototypes = queryNodes.stream().map(ProtoQueryNode::new).collect(toList());

        List<QueryNode> annotated = assigner.placeFiltersOnLeaves(prototypes).stream()
                .map(n -> (QueryNode)n).collect(toList());
        assertEquals(annotated.size(), 3);
        assertTrue(annotated.stream().noneMatch(Objects::isNull));
        for (int i = 0; i < annotated.size(); i++)
            assertEquals(annotated.get(i).getQuery(), queryNodes.get(i).getQuery());

        assigner.placeBottommost(root);
        checkAllFilters(fullQuery, root);
    }

    @Test
    public void testFilterOnMultiQuery() {
        CQuery query = createQuery(
                Alice, knows, x,
                x,     age,   u,
                SPARQLFilter.build("?u > 23"));
        MultiQueryNode node = MultiQueryNode.builder()
                .add(new QueryNode(ep1, query))
                .add(new QueryNode(ep2, query))
                .build();
        FilterAssigner assigner = new FilterAssigner(query);
        assigner.placeBottommost(node);
        checkAllFilters(query, node);
    }

    @Test
    public void testFilterOnMultiQueryOfAnnotated() {
        CQuery query = createQuery(
                Alice, knows, x,
                x,     age,   u,
                SPARQLFilter.build("?u > 23"));
        List<ProtoQueryNode> prototypes = asList(
                new ProtoQueryNode(ep1, query),
                new ProtoQueryNode(ep2, query)
        );

        FilterAssigner assigner = new FilterAssigner(query);
        List<QueryNode> queryNodes = assigner.placeFiltersOnLeaves(prototypes).stream()
                .map(n -> (QueryNode)n).collect(toList());
        for (QueryNode queryNode : queryNodes)
            checkAllFilters(query, queryNode);

        MultiQueryNode multi = MultiQueryNode.builder().addAll(queryNodes).build();
        assigner.placeBottommost(multi);
        checkAllFilters(query, multi);
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
        List<ProtoQueryNode> prototypes = asList(
                new ProtoQueryNode(ep1, createQuery(Alice, knows, x)),
                new ProtoQueryNode(ep1, createQuery(x, manages, y, x, age, u)),
                new ProtoQueryNode(ep2, createQuery(x, manages, y, y, age, v))
        );

        FilterAssigner assigner = new FilterAssigner(fullQuery);
        List<QueryNode> queryNodes = assigner.placeFiltersOnLeaves(prototypes).stream()
                .map(n -> (QueryNode)n).collect(toList());

        assertEquals(queryNodes.size(), prototypes.size());
        assertTrue(queryNodes.stream().noneMatch(Objects::isNull));
        for (int i = 0; i < queryNodes.size(); i++)
            assertEquals(prototypes.get(i).getMatchedQuery(), queryNodes.get(i).getQuery());
        assertEquals(queryNodes.get(0).getFilters(), emptySet());
        assertEquals(queryNodes.get(1).getFilters(), singleton(annEquals));
        assertEquals(queryNodes.get(2).getFilters(), singleton(annEquals));

        JoinNode joinLeft  = JoinNode.builder(queryNodes.get(1), queryNodes.get(2)).build();
        JoinNode joinRight = JoinNode.builder(queryNodes.get(1), queryNodes.get(2)).build();
        JoinNode leftDeep  = JoinNode.builder(joinLeft,          queryNodes.get(0)).build();
        JoinNode rightDeep = JoinNode.builder(queryNodes.get(0), joinRight        ).build();

        assigner.placeBottommost(leftDeep);
        assigner.placeBottommost(rightDeep);

        assertEquals(joinLeft.getFilters(), singleton(annGreater));
        assertEquals(joinRight.getFilters(), singleton(annGreater));
        checkAllFilters(fullQuery, rightDeep);
        checkAllFilters(fullQuery, leftDeep);
    }
}