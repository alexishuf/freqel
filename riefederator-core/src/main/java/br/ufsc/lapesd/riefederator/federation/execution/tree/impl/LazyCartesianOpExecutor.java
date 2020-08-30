package br.ufsc.lapesd.riefederator.federation.execution.tree.impl;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.execution.PlanExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.CartesianOpExecutor;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsList;
import br.ufsc.lapesd.riefederator.query.results.impl.CollectionResults;
import br.ufsc.lapesd.riefederator.query.results.impl.LazyCartesianResults;
import br.ufsc.lapesd.riefederator.query.results.impl.SPARQLFilterResults;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class LazyCartesianOpExecutor extends SimpleOpExecutor implements CartesianOpExecutor {
    private final  @Nonnull CardinalityComparator comp;

    @Inject
    public LazyCartesianOpExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                   @Nonnull CardinalityComparator comparator) {
        super(planExecutorProvider);
        this.comp = comparator;
    }

    @Override
    public @Nonnull Results execute(@Nonnull CartesianOp node) {
        PlanExecutor planExecutor = getPlanExecutor();
        try (ResultsList<Results> list = new ResultsList<>()) {
            List<Op> cs = new ArrayList<>(node.getChildren());
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
            for (Op child : cs)
                list.add(planExecutor.executeNode(child));
            Set<String> varNames = node.getResultVars();
            // parallelizing the inputs provides no significant improvement
            // the parallelization provided by lazyness is enough and is significant
            Results r = new LazyCartesianResults(list.steal(), varNames);
            r.setOptional(node.modifiers().optional() != null);
            r = SPARQLFilterResults.applyIf(r, node);
            assert r.isOptional() == (node.modifiers().optional() != null);
            return r;

        }
    }

    @Override
    public boolean canExecute(@Nonnull Class<? extends Op> nodeClass) {
        return CartesianOp.class.isAssignableFrom(nodeClass);
    }

    @Override
    public @Nonnull Results execute(@Nonnull Op node) throws IllegalArgumentException {
        Preconditions.checkArgument(node instanceof CartesianOp);
        return execute((CartesianOp)node);
    }
}
