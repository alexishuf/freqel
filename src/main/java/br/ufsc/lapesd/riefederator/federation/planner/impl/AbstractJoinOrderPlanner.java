package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;

import javax.annotation.Nonnull;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public abstract class AbstractJoinOrderPlanner implements JoinOrderPlanner {
    @Override
    public @Nonnull PlanNode plan(@Nonnull JoinGraph joinGraph,
                                  @Nonnull Collection<PlanNode> nodesCollection) {
        checkNodesPreconditions(joinGraph, nodesCollection);

        FindPathState state = new FindPathState(joinGraph, nodesCollection);
        if (!state.findPath())
            throw new IllegalArgumentException("There is no join-path covering all nodes");
        return plan(state.path, joinGraph);
    }

    private static class FindPathState {
        private final JoinGraph joinGraph;
        private final IndexedSubset<PlanNode> nodesSet, open;
        private final List<JoinInfo> path;
        private final int targetDepth;

        public FindPathState(@Nonnull JoinGraph joinGraph,
                             @Nonnull Collection<PlanNode> collection) {
            this.joinGraph = joinGraph;
            this.nodesSet = joinGraph.getNodes().subset(collection);
            this.open = joinGraph.getNodes().emptySubset();
            this.targetDepth = collection.size()-1;
            this.path = new ArrayList<>(targetDepth);
            for (int i = 0; i < targetDepth; i++)
                this.path.add(null);
        }

        public boolean findPath() {
            for (PlanNode node : nodesSet) {
                if (findPath(node, 0)) return true;
            }
            return false;
        }

        public boolean findPath(@Nonnull PlanNode node, int depth) {
            if (!open.add(node))
                return false;
            else if (depth == targetDepth)
                return true;

            boolean[] got = {false};
            joinGraph.forEachNeighbor(node, (i, n) -> {
                if (!got[0] && nodesSet.contains(n)) {
                    if ((got[0] = findPath(n, depth+1)))
                        path.set(depth, i);
                }
            });
            if (!got[0])
                open.remove(node);
            return got[0];
        }
    }

    protected void checkNodesPreconditions(@Nonnull JoinGraph joinGraph,
                                           @Nonnull Collection<PlanNode> nodes) {
        checkArgument(!nodes.isEmpty(), "Cannot build a plan without any nodes");
        checkArgument(nodes.size() > 1, "Needs at least two nodes to have a join");
        checkArgument(joinGraph.getNodes().containsAll(nodes), "JoinGraph misses some nodes");
        checkArgument(nodes.stream().noneMatch(Objects::isNull), "Null nodes not allowed");

        if (!getClass().desiredAssertionStatus()) //skip more expensive checks
            return;
        checkArgument(new HashSet<>(nodes).size() == nodes.size(), "Non-unique nodes");

        IndexedSet<Triple> allTriples = IndexedSet.from(nodes.stream()
                .flatMap(n -> n.getMatchedTriples().stream()).collect(toList()));
        List<IndexedSubset<Triple>> matched = nodes.stream()
                .map(n -> allTriples.subset(n.getMatchedTriples())).collect(toList());
        for (int i = 0; i < matched.size(); i++) {
            Set<Triple> inner = matched.get(i);
            for (int j = i+1; j < matched.size(); j++) {
                checkArgument(!matched.get(j).containsAll(inner),
                        "There are nodes whose getMatchedTriples() subsume another");
            }
        }
    }
}
