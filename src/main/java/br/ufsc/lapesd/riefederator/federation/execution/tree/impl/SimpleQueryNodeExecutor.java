package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.CartesianNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.MultiQueryNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.QueryNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.tree.CartesianNode;
import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.query.Results;
import br.ufsc.lapesd.riefederator.query.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.impl.ParallelResults;
import br.ufsc.lapesd.riefederator.query.impl.ProjectingResults;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Collections;

public class SimpleQueryNodeExecutor extends SimpleNodeExecutor
        implements QueryNodeExecutor, MultiQueryNodeExecutor, CartesianNodeExecutor {

    @Inject
    public SimpleQueryNodeExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider) {
        super(planExecutorProvider);
    }

    public SimpleQueryNodeExecutor(@Nonnull PlanExecutor planExecutor) {
        super(planExecutor);
    }

    @Override
    public boolean canExecute(@Nonnull Class<? extends PlanNode> nodeClass) {
        return QueryNode.class.isAssignableFrom(nodeClass)
                || MultiQueryNode.class.isAssignableFrom(nodeClass)
                || CartesianNode.class.isAssignableFrom(nodeClass);
    }

    @Override
    public @Nonnull Results execute(@Nonnull PlanNode node) {
        if (node instanceof MultiQueryNode)
            return execute((MultiQueryNode)node);
        else if (node instanceof QueryNode)
            return execute((QueryNode)node);
        throw new IllegalArgumentException("");
    }

    @Override
    public @Nonnull Results execute(@Nonnull QueryNode node) {
        return node.getEndpoint().query(node.getQuery());
    }

    @CheckReturnValue
    public @Nonnull Results executeAsMultiQuery(@Nonnull PlanNode node) {
        if (node.getChildren().isEmpty())
            return new CollectionResults(Collections.emptyList(), node.getResultVars());
        ArrayList<Results> resultList = new ArrayList<>(node.getChildren().size());
        PlanExecutor executor = getPlanExecutor();
        for (PlanNode child : node.getChildren())
            resultList.add(executor.executeNode(child));
        ParallelResults r = new ParallelResults(resultList);
        return node.isProjecting() ? new ProjectingResults(r, node.getResultVars()) : r;
    }

    @Override
    public @Nonnull Results execute(@Nonnull MultiQueryNode node) {
        return executeAsMultiQuery(node);
    }

    /**
     * Execute a {@link CartesianNode} without doing a cartesian product. This is
     * nonconconformant and highly confusing, but may be useful if the user wants it
     */
    @Override
    public @Nonnull Results execute(@Nonnull CartesianNode node) {
        return executeAsMultiQuery(node);
    }
}
