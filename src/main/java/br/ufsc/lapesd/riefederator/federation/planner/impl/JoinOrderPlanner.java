package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

public interface JoinOrderPlanner {
    /**
     * Plans the order of execution of the given joins.
     *
     * @param joins List of joins that need to be executed.
     * @param graph (optional) {@link JoinGraph} with JoinInfos between all nodes
     * @return A tree whose root corresponds to execution of all joins
     */
    @Nonnull PlanNode plan(@Nonnull List<JoinInfo> joins, @Nullable JoinGraph graph);

    @Nonnull default PlanNode plan(@Nonnull List<JoinInfo> joins) {
        return plan(joins, null);
    }

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
