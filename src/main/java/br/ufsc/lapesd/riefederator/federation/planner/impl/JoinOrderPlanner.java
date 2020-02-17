package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface JoinOrderPlanner {
    /**
     * Plans the order of executing joins among all given nodes.
     *
     * @param joinGraph A {@link JoinGraph} with {@link JoinInfo}s between all nodes
     * @param nodes The set of {@link PlanNode}s to be arranged in a plan
     * @throws IllegalArgumentException if one of the following occurs:
     * <ul>
     *     <li>The {@link PlanNode}s in nodes are not a single join-connected component</li>
     *     <li>There are repeated nodes om nodes</li>
     *     <li>There are nodes whose {@link PlanNode#getMatchedTriples()} is submsumed by others</li>
     * </ul>
     * @return A tree containing all nodes aggregated by {@link JoinNode}s
     */
    @Nonnull PlanNode plan(@Nonnull JoinGraph joinGraph, @Nonnull Collection<PlanNode> nodes);
}
