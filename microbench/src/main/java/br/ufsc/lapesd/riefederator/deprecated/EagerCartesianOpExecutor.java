package br.ufsc.lapesd.riefederator.deprecated;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.CartesianOpExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleOpExecutor;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsList;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.SPARQLFilterResults;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Comparator;

import static java.util.Collections.emptyList;

public class EagerCartesianOpExecutor extends SimpleOpExecutor implements CartesianOpExecutor {

    private final @Nonnull CardinalityComparator comparator;

    @Inject
    public EagerCartesianOpExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                    @Nonnull CardinalityComparator comparator) {
        super(planExecutorProvider);
        this.comparator = comparator;
    }

    @Override
    public boolean canExecute(@Nonnull Class<? extends Op> nodeClass) {
        return CartesianOp.class.isAssignableFrom(nodeClass);
    }

    @Override
    public @Nonnull Results execute(@Nonnull CartesianOp node) throws IllegalArgumentException {
        if (node.getChildren().isEmpty())
            return new CollectionResults(emptyList(), node.getResultVars());
        PlanExecutor executor = getPlanExecutor();

        Op max = node.getChildren().stream()
                .max(Comparator.comparing(Op::getCardinality, comparator)).orElse(null);
        assert max != null;
        ResultsList<Results> toFetch = new ResultsList<>();
        for (Op child : node.getChildren()) {
            if (child != max)
                toFetch.add(executor.executeNode(child));
        }
        Results results = new EagerCartesianResults(executor.executeNode(max), toFetch,
                node.getResultVars());
        return SPARQLFilterResults.applyIf(results, node);
    }

    @Override
    public @Nonnull Results execute(@Nonnull Op node) throws IllegalArgumentException {
        Preconditions.checkArgument(node instanceof CartesianOp);
        return execute((CartesianOp) node);
    }
}
