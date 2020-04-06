package br.ufsc.lapesd.riefederator.federation.execution;

import br.ufsc.lapesd.riefederator.federation.execution.tree.*;
import br.ufsc.lapesd.riefederator.federation.tree.*;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;
import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;

public class InjectedExecutor implements PlanExecutor {
    private @Nonnull QueryNodeExecutor queryNodeExecutor;
    private @Nonnull MultiQueryNodeExecutor multiQueryNodeExecutor;
    private @Nonnull JoinNodeExecutor joinNodeExecutor;
    private @Nonnull CartesianNodeExecutor cartesianNodeExecutor;
    private @Nonnull EmptyNodeExecutor emptyNodeExecutor;

    @Inject
    public InjectedExecutor(@Nonnull QueryNodeExecutor queryNodeExecutor,
                            @Nonnull MultiQueryNodeExecutor multiQueryNodeExecutor,
                            @Nonnull JoinNodeExecutor joinNodeExecutor,
                            @Nonnull CartesianNodeExecutor cartesianNodeExecutor,
                            @Nonnull EmptyNodeExecutor emptyNodeExecutor) {
        this.queryNodeExecutor = queryNodeExecutor;
        this.multiQueryNodeExecutor = multiQueryNodeExecutor;
        this.joinNodeExecutor = joinNodeExecutor;
        this.cartesianNodeExecutor = cartesianNodeExecutor;
        this.emptyNodeExecutor = emptyNodeExecutor;
    }

    @Override
    public @Nonnull  Results executePlan(@Nonnull PlanNode plan) {
        return executeNode(plan);
    }

    @Override
    public @Nonnull Results executeNode(@Nonnull PlanNode node) {
        checkArgument(TreeUtils.isAcyclic(node), "Node "+node+"is not a tree");
        checkArgument(node.getRequiredInputVars().isEmpty(), "Node "+node+" needs inputs");
        Class<? extends PlanNode> cls = node.getClass();
        if (QueryNode.class.isAssignableFrom(cls))
            return queryNodeExecutor.execute(node);
        else if (MultiQueryNode.class.isAssignableFrom(cls))
            return multiQueryNodeExecutor.execute(node);
        else if (JoinNode.class.isAssignableFrom(cls))
            return joinNodeExecutor.execute(node);
        else if (CartesianNode.class.isAssignableFrom(cls))
            return cartesianNodeExecutor.execute(node);
        else if (EmptyNode.class.isAssignableFrom(cls))
            return emptyNodeExecutor.execute(node);
        throw new UnsupportedOperationException("No executor for "+cls);
    }
}
