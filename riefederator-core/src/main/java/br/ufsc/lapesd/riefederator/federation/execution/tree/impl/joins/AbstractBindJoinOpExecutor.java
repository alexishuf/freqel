package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.impl.SPARQLFilterResults;

import javax.annotation.Nonnull;
import javax.inject.Provider;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class AbstractBindJoinOpExecutor extends AbstractSimpleJoinOpExecutor {
    public AbstractBindJoinOpExecutor(Provider<PlanExecutor> planExecutorProvider) {
        super(planExecutorProvider);
    }

    public AbstractBindJoinOpExecutor(PlanExecutor planExecutor) {
        super(planExecutor);
    }

    @Override
    public @Nonnull Results execute(@Nonnull JoinOp node) {
        PlanExecutor planExecutor = getPlanExecutor();
        Results leftResults = null;
        Op[] nodes = orderForBind(node);
        try {
            leftResults = planExecutor.executeNode(nodes[0]);
            Results results = createResults(leftResults, nodes[1], node);
            leftResults = null; // ownership transferred
            return SPARQLFilterResults.applyIf(results, node);
        } finally {
            if (leftResults != null)
                leftResults.close();
        }
    }

    protected abstract @Nonnull  Results createResults(@Nonnull Results left,
                                                       @Nonnull Op right,
                                                       @Nonnull JoinOp node);

    protected @Nonnull  Op[] orderForBind(@Nonnull JoinOp node) {
        Op[] nodes = new Op[] {node.getLeft(), node.getRight()};

        int leftWeight  = (nodes[0].hasInputs() ? 1 : 0) + (nodes[0].hasRequiredInputs() ? 1 : 0);
        int rightWeight = (nodes[1].hasInputs() ? 1 : 0) + (nodes[1].hasRequiredInputs() ? 1 : 0);
        if (leftWeight > rightWeight) { //send node with inputs to the right
            Op tmp = nodes[0];
            nodes[0] = nodes[1];
            nodes[1] = tmp;
        }
        checkArgument(!nodes[0].hasInputs(), "Both left and right children have required inputs. " +
                                             "Cannot bind join "+node);
        return nodes;
    }
}
