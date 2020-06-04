package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.InMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.ParallelInMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsList;
import br.ufsc.lapesd.riefederator.query.results.impl.SPARQLFilterResults;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Set;

public class HashJoinNodeExecutor extends AbstractSimpleJoinNodeExecutor {
    private final @Nonnull CardinalityComparator comparator;

    @Inject
    public HashJoinNodeExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                @Nonnull CardinalityComparator comparator) {
        super(planExecutorProvider);
        this.comparator = comparator;
    }

    public HashJoinNodeExecutor(@Nonnull PlanExecutor planExecutor,
                                @Nonnull CardinalityComparator comparator) {
        super(planExecutor);
        this.comparator = comparator;
    }

    @Override
    public @Nonnull Results execute(@Nonnull JoinNode node) {
        PlanExecutor exec = getPlanExecutor();
        assert node.getChildren().size() == 2;
        try (ResultsList list = new ResultsList()) {
            for (PlanNode child : node.getChildren()) list.add(exec.executeNode(child));

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
            return SPARQLFilterResults.applyIf(results, node);
        }
    }
}
