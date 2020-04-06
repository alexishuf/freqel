package br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.InMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.ParallelInMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsList;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.query.Cardinality.Reliability.UPPER_BOUND;

public class HashJoinNodeExecutor extends AbstractSimpleJoinNodeExecutor {
    @Inject
    public HashJoinNodeExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider) {
        super(planExecutorProvider);
    }

    public HashJoinNodeExecutor(@Nonnull PlanExecutor planExecutor) {
        super(planExecutor);
    }

    @Override
    public @Nonnull Results execute(@Nonnull JoinNode node) {
        PlanExecutor exec = getPlanExecutor();
        try (ResultsList list = new ResultsList()) {
            for (PlanNode child : node.getChildren()) list.add(exec.executeNode(child));

            int idx = -1, min = Integer.MAX_VALUE;
            for (int i = 0; i < list.size(); i++) {
                Results r = list.get(i);
                Cardinality card = r.getCardinality();
                Cardinality.Reliability reliability = card.getReliability();
                if (reliability.ordinal() >= UPPER_BOUND.ordinal() && card.getValue(min) < min) {
                    min = card.getValue(min);
                    idx = i;
                }
            }

            Results results;
            Set<String> joinVars = node.getJoinVars(), resultVars = node.getResultVars();
            if (idx >= 0 && min < 1024) {
                results = new InMemoryHashJoinResults(list.get(idx), list.get((idx + 1) % 2),
                                                      joinVars, resultVars);
            } else {
                results = new ParallelInMemoryHashJoinResults(list.get(0), list.get(1),
                                                              joinVars, resultVars);
            }
            list.clear();
            return results;
        }
    }
}
