package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinPath;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;

import javax.annotation.Nonnull;
import java.util.Collection;

public abstract class AbstractJoinOrderPlanner implements JoinOrderPlanner {
    @Override
    public @Nonnull PlanNode plan(@Nonnull JoinGraph joinGraph,
                                  @Nonnull Collection<PlanNode> nodesCollection) {
        JoinPath path = JoinPath.findPath(joinGraph, nodesCollection);
        if (path == null)
            throw new IllegalArgumentException("There is no join-path covering all nodes");
        return plan(path.getJoinInfos(), joinGraph);
    }

}
