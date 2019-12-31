package br.ufsc.lapesd.riefederator.federation.execution;

import br.ufsc.lapesd.riefederator.federation.execution.tree.CartesianNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.JoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.MultiQueryNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.QueryNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.tree.*;
import br.ufsc.lapesd.riefederator.query.Results;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.inject.Inject;

public class InjectedExecutor implements PlanExecutor {
    private @Nonnull QueryNodeExecutor queryNodeExecutor;
    private @Nonnull MultiQueryNodeExecutor multiQueryNodeExecutor;
    private @Nonnull JoinNodeExecutor joinNodeExecutor;
    private @Nonnull CartesianNodeExecutor cartesianNodeExecutor;

    @Inject
    public InjectedExecutor(@Nonnull QueryNodeExecutor queryNodeExecutor,
                            @Nonnull MultiQueryNodeExecutor multiQueryNodeExecutor,
                            @Nonnull JoinNodeExecutor joinNodeExecutor,
                            @Nonnull CartesianNodeExecutor cartesianNodeExecutor) {
        this.queryNodeExecutor = queryNodeExecutor;
        this.multiQueryNodeExecutor = multiQueryNodeExecutor;
        this.joinNodeExecutor = joinNodeExecutor;
        this.cartesianNodeExecutor = cartesianNodeExecutor;
    }

    @Override
    public @Nonnull  Results executePlan(@Nonnull PlanNode plan) {
        return executeNode(plan);
    }

    @Override
    public @Nonnull Results executeNode(@Nonnull PlanNode node) {
        Preconditions.checkArgument(TreeUtils.isTree(node));
        Class<? extends PlanNode> cls = node.getClass();
        if (QueryNode.class.isAssignableFrom(cls))
            return queryNodeExecutor.execute(node);
        else if (MultiQueryNode.class.isAssignableFrom(cls))
            return multiQueryNodeExecutor.execute(node);
        else if (JoinNode.class.isAssignableFrom(cls))
            return joinNodeExecutor.execute(node);
        else if (CartesianNode.class.isAssignableFrom(cls))
            return cartesianNodeExecutor.execute(node);
        throw new UnsupportedOperationException("No executor for "+cls);
    }
}
