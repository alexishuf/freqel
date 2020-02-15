package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinPath;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.util.IndexedSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public abstract class AbstractJoinOrderPlanner implements JoinOrderPlanner {
    @Override
    public @Nonnull PlanNode plan(@Nonnull JoinGraph joinGraph,
                                  @Nonnull Collection<PlanNode> nodesCollection) {
        JoinPath path = JoinPath.findPath(joinGraph, nodesCollection);
        if (path == null)
            throw new IllegalArgumentException("There is no join-path covering all nodes");
        return plan(path.getJoinInfos(), joinGraph);
    }

    @Override
    public @Nonnull PlanNode plan(@Nonnull List<JoinInfo> joins, @Nullable JoinGraph graph) {
        Set<PlanNode> nodeSet = joins.stream()
                .flatMap(i -> Stream.of(i.getLeft(), i.getRight())).collect(toSet());
        if (graph == null) {
            graph = new JoinGraph(IndexedSet.fromDistinct(nodeSet));
            return plan(graph, graph.getNodes());
        }
        return plan(graph, nodeSet);
    }
}
