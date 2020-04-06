package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.CartesianNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.tree.CartesianNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.CardinalityComparator;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsList;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.CartesianResults;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.ArrayList;

import static java.util.Collections.emptyList;

public class SimpleCartesianNodeExecutor extends SimpleNodeExecutor implements CartesianNodeExecutor {
    @Inject
    public SimpleCartesianNodeExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider) {
        super(planExecutorProvider);
    }

    public SimpleCartesianNodeExecutor(@Nonnull PlanExecutor planExecutor) {
        super(planExecutor);
    }

    @Override
    public boolean canExecute(@Nonnull Class<? extends PlanNode> nodeClass) {
        return CartesianNode.class.isAssignableFrom(nodeClass);
    }

    @Override
    public @Nonnull
    Results execute(@Nonnull CartesianNode node) throws IllegalArgumentException {
        if (node.getChildren().isEmpty())
            return new CollectionResults(emptyList(), node.getResultVars());
        PlanExecutor executor = getPlanExecutor();
        try (ResultsList list = new ResultsList()) {
            for (PlanNode child : node.getChildren())
                list.add(executor.executeNode(child));

            ArrayList<ArrayList<Solution>> fetched = new ArrayList<>();
            CardinalityComparator comp = new CardinalityComparator();
            int maxIdx = 0;
            Cardinality max = list.get(0).getCardinality();
            for (int i = 1; i < list.size(); i++) {
                Results results = list.get(i);
                if (comp.compare(results.getCardinality(), max) > 0) {
                    max = results.getCardinality();
                    maxIdx = i;
                }
            }
            for (int i = 0; i < list.size(); i++) {
                if (i != maxIdx) {
                    ArrayList<Solution> solutions = new ArrayList<>();
                    list.get(i).forEachRemainingThenClose(solutions::add);
                    fetched.add(solutions);
                }
            }
            CartesianResults results = new CartesianResults(list.get(maxIdx), fetched,
                                                            node.getResultVars());
            list.remove(maxIdx); // Now owned by results. Do not close on exit of try-with
            return results;
        }
    }

    @Override
    public @Nonnull Results execute(@Nonnull PlanNode node) throws IllegalArgumentException {
        Preconditions.checkArgument(node instanceof CartesianNode);
        return execute((CartesianNode) node);
    }
}
