package br.ufsc.lapesd.riefederator.federation.execution;

import br.ufsc.lapesd.riefederator.federation.execution.tree.*;
import br.ufsc.lapesd.riefederator.federation.tree.*;
import br.ufsc.lapesd.riefederator.query.results.Results;

import javax.annotation.Nonnull;
import javax.inject.Inject;

public class InjectedExecutor implements PlanExecutor {
    private final @Nonnull QueryNodeExecutor queryNodeExecutor;
    private final @Nonnull MultiQueryNodeExecutor multiQueryNodeExecutor;
    private final @Nonnull JoinNodeExecutor joinNodeExecutor;
    private final @Nonnull CartesianNodeExecutor cartesianNodeExecutor;
    private final @Nonnull EmptyNodeExecutor emptyNodeExecutor;

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
        assert TreeUtils.isAcyclic(node) : "Node is not a tree";
        assert node.getRequiredInputVars().isEmpty() : "Node needs inputs";
        Class<? extends PlanNode> cls = node.getClass();
        Results results;
        if (QueryNode.class.isAssignableFrom(cls))
            results = queryNodeExecutor.execute(node);
        else if (MultiQueryNode.class.isAssignableFrom(cls))
            results = multiQueryNodeExecutor.execute(node);
        else if (JoinNode.class.isAssignableFrom(cls))
            results = joinNodeExecutor.execute(node);
        else if (CartesianNode.class.isAssignableFrom(cls))
            results = cartesianNodeExecutor.execute(node);
        else if (EmptyNode.class.isAssignableFrom(cls))
            results = emptyNodeExecutor.execute(node);
        else
            throw new UnsupportedOperationException("No executor for "+cls);
        results.setNodeName(node.getName());
        return results;
    }
}
