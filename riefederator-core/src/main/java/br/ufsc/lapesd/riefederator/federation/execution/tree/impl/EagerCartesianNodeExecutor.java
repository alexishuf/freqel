package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.CartesianNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.tree.CartesianNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsList;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.EagerCartesianResults;
import br.ufsc.lapesd.riefederator.query.results.impl.SPARQLFilterResults;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Comparator;

import static java.util.Collections.emptyList;

public class EagerCartesianNodeExecutor extends SimpleNodeExecutor implements CartesianNodeExecutor {
    private final @Nonnull CardinalityComparator comparator;

    @Inject
    public EagerCartesianNodeExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                      @Nonnull CardinalityComparator comparator) {
        super(planExecutorProvider);
        this.comparator = comparator;
    }

    @Override
    public boolean canExecute(@Nonnull Class<? extends PlanNode> nodeClass) {
        return CartesianNode.class.isAssignableFrom(nodeClass);
    }

    @Override
    public @Nonnull Results execute(@Nonnull CartesianNode node) throws IllegalArgumentException {
        if (node.getChildren().isEmpty())
            return new CollectionResults(emptyList(), node.getResultVars());
        PlanExecutor executor = getPlanExecutor();

        PlanNode max = node.getChildren().stream()
                .max(Comparator.comparing(PlanNode::getCardinality, comparator)).orElse(null);
        assert max != null;
        ResultsList<Results> toFetch = new ResultsList<>();
        for (PlanNode child : node.getChildren()) {
            if (child != max)
                toFetch.add(executor.executeNode(child));
        }
        Results results = new EagerCartesianResults(executor.executeNode(max), toFetch,
                                               node.getResultVars());
        return SPARQLFilterResults.applyIf(results, node);
    }

    @Override
    public @Nonnull Results execute(@Nonnull PlanNode node) throws IllegalArgumentException {
        Preconditions.checkArgument(node instanceof CartesianNode);
        return execute((CartesianNode) node);
    }
}
