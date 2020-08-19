package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.InMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.ParallelInMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsList;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Set;

public class DefaultHashJoinOpExecutor extends AbstractSimpleJoinOpExecutor {
    private final @Nonnull CardinalityComparator comparator;

    @Inject
    public DefaultHashJoinOpExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                     @Nonnull CardinalityComparator comparator) {
        super(planExecutorProvider);
        this.comparator = comparator;
    }

    public DefaultHashJoinOpExecutor(@Nonnull PlanExecutor planExecutor,
                                     @Nonnull CardinalityComparator comparator) {
        super(planExecutor);
        this.comparator = comparator;
    }

    @Override
    protected @Nonnull Results innerExecute(@Nonnull JoinOp node) {
        PlanExecutor exec = getPlanExecutor();
        assert node.getChildren().size() == 2;
        try (ResultsList<Results> list = new ResultsList<>()) {
            for (Op child : node.getChildren()) list.add(exec.executeNode(child));

            Results results;
            Set<String> joinVars = node.getJoinVars(), resultVars = node.getResultVars();
            Cardinality lc = node.getLeft().getCardinality(), rc = node.getLeft().getCardinality();
            int diff = comparator.compare(lc, rc);
            if (diff == 0) {
                results = new ParallelInMemoryHashJoinResults(list.get(0), list.get(1),
                                                              joinVars, resultVars);
            } else {
                int i = diff <= 0 ? 0 : 1;
                results = new InMemoryHashJoinResults(list.get(i), list.get((i+1) % 2),
                                                      joinVars, resultVars);
            }
            list.clear();
            return results;
        }
    }
}
