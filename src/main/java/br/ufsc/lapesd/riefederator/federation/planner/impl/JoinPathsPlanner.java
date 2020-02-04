package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinPath;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.SubPathAggregation;
import br.ufsc.lapesd.riefederator.federation.tree.*;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public class JoinPathsPlanner implements Planner {
    private static final Logger logger = LoggerFactory.getLogger(JoinPathsPlanner.class);
    private static final int PATHS_PAR_THRESHOLD = 10;
    private @Nonnull JoinOrderPlanner joinOrderPlanner;

    @Inject
    public JoinPathsPlanner(@Nonnull JoinOrderPlanner joinOrderPlanner) {
        this.joinOrderPlanner = joinOrderPlanner;
    }

    @Override
    public @Nonnull PlanNode plan(@Nonnull CQuery query, @Nonnull Collection<QueryNode> qns){
        checkArgument(!qns.isEmpty(), "Cannot plan without QueryNodes!");
        if (query.isEmpty())
            return new EmptyNode(query.streamTerms(Var.class).map(Var::getName).collect(toList()));
        IndexedSet<Triple> full = IndexedSet.fromDistinctCopy(query.getMatchedTriples());
        if (JoinPathsPlanner.class.desiredAssertionStatus()) {
            checkArgument(qns.stream().allMatch(n -> full.containsAll(n.getMatchedTriples())),
                    "Some QueryNodes match triples not in query");
        }
        if (!satisfiesAll(full, qns)) {
            logger.info("QueryNodes miss some triples in query {}, returning EmptyNode", query);
            return EmptyNode.createFor(query);
        }

        List<IndexedSet<Triple>> cartesianComponents = getCartesianComponents(full);
        assert !cartesianComponents.isEmpty();
        if (cartesianComponents.size() > 1) {
            List<PlanNode> list = new ArrayList<>(cartesianComponents.size());
            for (IndexedSet<Triple> component : cartesianComponents) {
                List<QueryNode> selected = qns.stream()
                        .filter(qn -> component.containsAny(qn.getMatchedTriples()))
                        .collect(toList());
                PlanNode root = plan(selected, component);
                if (root == null) {
                    logger.debug("Query {} is unsatisfiable because one component {} of a " +
                                 "cartesian product is unsatisfiable", query, component);
                    return EmptyNode.createFor(query);
                }
                list.add(root);
            }
            return new CartesianNode(list);
        } else {
            PlanNode root = plan(qns, full);
            if (root == null) {
                logger.debug("No join path across endpoints for join-connected query {}. " +
                             "Returning EmptyNode", query);
                return EmptyNode.createFor(query);
            }
            return root;
        }
    }

    private boolean hasJoin(@Nonnull Triple a, @Nonnull Triple b) {
        return  (a.getSubject().isVar()   && b.contains(a.getSubject()  )) ||
                (a.getPredicate().isVar() && b.contains(a.getPredicate())) ||
                (a.getObject().isVar()    && b.contains(a.getObject()   ));
    }

    @VisibleForTesting
    @Nonnull List<IndexedSet<Triple>>
    getCartesianComponents(@Nonnull IndexedSet<Triple> triples) {
        List<IndexedSet<Triple>> components = new ArrayList<>();

        IndexedSubset<Triple> visited = triples.emptySubset(), component = triples.emptySubset();
        ArrayDeque<Triple> stack = new ArrayDeque<>(triples.size());
        for (Triple start : triples) {
            if (visited.contains(start))
                continue;
            component.clear();
            stack.push(start);
            while (!stack.isEmpty()) {
                Triple triple = stack.pop();
                if (visited.add(triple)) {
                    component.add(triple);
                    for (Triple next : triples) {
                        if (!visited.contains(next) && hasJoin(triple, next))
                            stack.push(next);
                    }
                }
            }
            components.add(IndexedSet.fromDistinct(component));
        }
        return components;
    }

    private @Nullable PlanNode plan(@Nonnull Collection<QueryNode> qns,
                                    @Nonnull IndexedSet<Triple> triples) {
        IndexedSet<PlanNode> leaves = groupNodes(qns);
        JoinGraph g = new JoinGraph(leaves);
        Set<JoinPath> paths = new HashSet<>(leaves.size());
        getPaths(triples, g, paths);
        if (paths.isEmpty())
            return null;

        SubPathAggregation aggregation = SubPathAggregation.aggregate(g, paths, joinOrderPlanner);
        JoinGraph g2 = aggregation.getGraph();
        List<JoinPath> aggregatedPaths = aggregation.getJoinPaths();
        boolean parallel = paths.size() > PATHS_PAR_THRESHOLD;
        MultiQueryNode.Builder builder = MultiQueryNode.builder();
        builder.addAll((parallel ? aggregatedPaths.parallelStream() : aggregatedPaths.stream())
                .map(p -> p.isWhole() ? p.getWhole() : joinOrderPlanner.plan(p.getJoinInfos(), g2))
                .collect(toList()));
        return builder.buildIfMulti();
    }

    private boolean satisfiesAll(@Nonnull IndexedSet<Triple> all,
                                 @Nonnull Collection<QueryNode> qns) {
        IndexedSubset<Triple> subset = all.emptySubset();
        for (QueryNode qn : qns)
            subset.union(qn.getMatchedTriples());
        return subset.size() == all.size();
    }

    @VisibleForTesting
    @Nonnull IndexedSet<PlanNode> groupNodes(@Nonnull Collection<QueryNode> queryNodes) {
        ListMultimap<JoinInterface, QueryNode> mm;
        mm = MultimapBuilder.hashKeys(queryNodes.size()).arrayListValues().build();

        for (QueryNode node : queryNodes)
            mm.put(new JoinInterface(node), node);

        List<PlanNode> list = new ArrayList<>();
        for (JoinInterface key : mm.keySet()) {
            Collection<QueryNode> nodes = mm.get(key);
            assert !nodes.isEmpty();
            if (nodes.size() > 1)
                list.add(MultiQueryNode.builder().addAll(nodes).build());
            else
                list.add(nodes.iterator().next());
        }
        return IndexedSet.fromDistinct(list);
    }

    @VisibleForTesting
    void getPaths(@Nonnull IndexedSet<Triple> full, @Nonnull JoinGraph g,
                  @Nonnull Set<JoinPath> paths) {
        int totalTriples = full.size();
        IndexedSet<PlanNode> nodes = g.getNodes();
        ArrayDeque<State> stack = new ArrayDeque<>(nodes.size()*2);
        nodes.forEach(n -> stack.push(State.start(full, n)));
        while (!stack.isEmpty()) {
            State state = stack.pop();
            if (state.matched.size() == totalTriples) {
                if (!state.hasInputs()) //ignore join paths that leave pending inputs
                    paths.add(state.toPath(nodes));
                else
                    logger.debug("Discarding path with pending inputs {}", state.toPath(nodes));
            } else {
                g.forEachNeighbor(state.node, (info, node) -> {
                    State next = state.advance(info, node);
                    if (next != null)
                        stack.push(next);
                });
            }
        }
    }

    @VisibleForTesting
    static class State {
        final @Nonnull PlanNode node;
        final @Nullable JoinInfo joinInfo;
        final @Nullable State ancestor;
        final int depth;
        final IndexedSubset<Triple> matched;

        private State(@Nonnull PlanNode node, @Nullable JoinInfo joinInfo,
                      @Nullable State ancestor,
                      @Nonnull IndexedSubset<Triple> matched) {
            this.node = node;
            this.joinInfo = joinInfo;
            this.ancestor = ancestor;
            this.depth = ancestor == null ? 0 : ancestor.depth+1;
            this.matched = matched;
        }
        static @Nonnull State start(@Nonnull IndexedSet<Triple> all, @Nonnull PlanNode start) {
            IndexedSubset<Triple> matchedTriples = all.subset(start.getMatchedTriples());
            return new State(start, null, null, matchedTriples);
        }
        @Nullable State advance(@Nonnull JoinInfo info, PlanNode nextNode) {
            assert nextNode != node;
            assert info.getLeft() == nextNode || info.getRight() == nextNode;
            assert info.getLeft() == node     || info.getRight() == node    ;

            IndexedSubset<Triple> novelMatched = matched.createUnion(nextNode.getMatchedTriples());
            assert novelMatched.size() >= matched.size();
            if (novelMatched.size() == matched.size())
                return null; // no new triples satisfied
            return new State(nextNode, info, this, novelMatched);
        }
        boolean hasInputs() {
            return joinInfo == null ? node.hasInputs() : !joinInfo.getPendingInputs().isEmpty();
        }

        @Nonnull JoinPath toPath(IndexedSet<PlanNode> allNodes) {
            if (depth == 0)
                return new JoinPath(allNodes, node);
            //noinspection UnstableApiUsage
            ImmutableList.Builder<JoinInfo> builder = ImmutableList.builderWithExpectedSize(depth);
            for (State s = this; s != null; s = s.ancestor) {
                if (s.joinInfo != null) builder.add(s.joinInfo);
                else                    assert s.ancestor == null;
            }
            return new JoinPath(allNodes, builder.build());
        }
    }

}
