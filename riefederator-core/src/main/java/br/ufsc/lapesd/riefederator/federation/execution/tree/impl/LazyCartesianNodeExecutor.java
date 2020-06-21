package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.CartesianNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.tree.CartesianNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsList;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.LazyCartesianResults;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class LazyCartesianNodeExecutor extends SimpleNodeExecutor implements CartesianNodeExecutor {
    private final  @Nonnull CardinalityComparator comp;

    @Inject
    public LazyCartesianNodeExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                     @Nonnull CardinalityComparator comparator) {
        super(planExecutorProvider);
        this.comp = comparator;
    }

    public LazyCartesianNodeExecutor(@Nonnull PlanExecutor planExecutor,
                                     @Nonnull CardinalityComparator comparator) {
        super(planExecutor);
        this.comp = comparator;
    }

    @Override
    public @Nonnull Results execute(@Nonnull CartesianNode node) {
        PlanExecutor planExecutor = getPlanExecutor();
        try (ResultsList<Results> list = new ResultsList<>()) {
            List<PlanNode> cs = new ArrayList<>(node.getChildren());
            if (cs.isEmpty())
                return new CollectionResults(Collections.emptyList(), node.getResultVars());

            int size = cs.size(), maxIdx = 0;
            Cardinality max = cs.get(0).getCardinality();
            for (int i = 1; i < size; i++) {
                Cardinality cardinality = cs.get(i).getCardinality();
                if (comp.compare(cardinality, max) > 0) {
                    maxIdx = i;
                    max = cardinality;
                }
            }
            if (maxIdx != 0)
                cs.add(0, cs.remove(maxIdx));
            for (PlanNode child : cs)
                list.add(planExecutor.executeNode(child));
            Set<String> varNames = node.getResultVars();
            // parallelizing the inputs provides no significant improvement
            // the parallelization provided by lazyness is enough and is significant
            return new LazyCartesianResults(list.steal(), varNames);
        }
    }

    @Override
    public boolean canExecute(@Nonnull Class<? extends PlanNode> nodeClass) {
        return CartesianNode.class.isAssignableFrom(nodeClass);
    }

    @Override
    public @Nonnull Results execute(@Nonnull PlanNode node) throws IllegalArgumentException {
        Preconditions.checkArgument(node instanceof CartesianNode);
        return execute((CartesianNode)node);
    }
}
