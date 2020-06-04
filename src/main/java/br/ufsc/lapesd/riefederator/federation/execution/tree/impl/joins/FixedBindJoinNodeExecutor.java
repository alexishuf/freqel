package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.impl.SPARQLFilterResults;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;

import static com.google.common.base.Preconditions.checkArgument;

public class FixedBindJoinNodeExecutor extends AbstractSimpleJoinNodeExecutor {
    private final @Nonnull BindJoinResultsFactory factory;

    @Inject
    public FixedBindJoinNodeExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                     @Nonnull BindJoinResultsFactory factory) {
        super(planExecutorProvider);
        this.factory = factory;
    }

    public FixedBindJoinNodeExecutor(@Nonnull PlanExecutor planExecutor,
                                     @Nonnull BindJoinResultsFactory factory) {
        super(planExecutor);
        this.factory = factory;
    }

    @Override
    public @Nonnull Results execute(@Nonnull JoinNode node) {
        PlanExecutor planExecutor = getPlanExecutor();
        Results leftResults = null;
        PlanNode left = node.getLeft();
        PlanNode right = node.getRight();
        int leftWeight  = ( left.hasInputs() ? 1 : 0) + ( left.hasRequiredInputs() ? 1 : 0);
        int rightWeight = (right.hasInputs() ? 1 : 0) + (right.hasRequiredInputs() ? 1 : 0);
        if (leftWeight > rightWeight) {
            PlanNode tmp = left;
            left = right;
            right = tmp;
        }
        checkArgument(!left.hasInputs(), "Both left and right children have required inputs. " +
                                         "Cannot bind join "+node);
        try {
            leftResults = planExecutor.executeNode(left);
            Results results = factory.createResults(leftResults, right,
                                                    node.getJoinVars(), node.getResultVars());
            leftResults = null;
            return SPARQLFilterResults.applyIf(results, node);
        } finally {
            if (leftResults != null)
                leftResults.close();
        }
    }
}
