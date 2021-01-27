package br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.federation.cardinality.CardinalityComparator;
import br.ufsc.lapesd.freqel.federation.cardinality.impl.ThresholdCardinalityComparator;
import br.ufsc.lapesd.freqel.federation.execution.PlanExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.freqel.query.results.Results;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;

import static br.ufsc.lapesd.freqel.algebra.Cardinality.Reliability.UPPER_BOUND;
import static br.ufsc.lapesd.freqel.federation.cardinality.CardinalityUtils.multiply;

public class DefaultJoinOpExecutor extends AbstractSimpleJoinOpExecutor {
    private static final Logger logger = LoggerFactory.getLogger(DefaultJoinOpExecutor.class);

    private @Nonnull final DefaultHashJoinOpExecutor hashExecutor;
    private @Nonnull final FixedBindJoinOpExecutor bindExecutor;
    private @Nonnull final CardinalityComparator comparator;

    @Inject
    public DefaultJoinOpExecutor(@Nonnull Provider<PlanExecutor> planExecutorProvider,
                                 @Nonnull BindJoinResultsFactory bindJoinResultsFactory,
                                 @Nonnull CardinalityComparator cardinalityComparator) {
        super(planExecutorProvider);
        this.comparator = cardinalityComparator;
        this.hashExecutor = new DefaultHashJoinOpExecutor(planExecutorProvider, comparator);
        this.bindExecutor = new FixedBindJoinOpExecutor(planExecutorProvider,
                                                          bindJoinResultsFactory);
    }

    public DefaultJoinOpExecutor(@Nonnull PlanExecutor planExecutor,
                                 @Nonnull BindJoinResultsFactory bindJoinResultsFactory) {
        super(planExecutor);
        this.comparator = ThresholdCardinalityComparator.DEFAULT;
        this.hashExecutor = new DefaultHashJoinOpExecutor(planExecutor, this.comparator);
        this.bindExecutor = new FixedBindJoinOpExecutor(planExecutor, bindJoinResultsFactory);
    }

    @Override
    protected @Nonnull Results innerExecute(@Nonnull JoinOp node) {
        boolean leftOptional = node.getLeft().modifiers().optional() != null;
        boolean rightOptional = node.getLeft().modifiers().optional() != null;
        boolean leftInputs = node.getLeft().hasInputs();
        boolean rightInputs = node.getRight().hasInputs();
        boolean rightReqInputs = node.getRight().hasRequiredInputs();
        boolean leftReqInputs = node.getLeft().hasRequiredInputs();

        if (leftOptional && rightOptional && !leftReqInputs && !rightReqInputs) {
            return hashExecutor.innerExecute(node);
        } else if (leftInputs || rightInputs) {
            return bindExecutor.innerExecute(node);
        } else {
            Cardinality lc = node.getLeft().getCardinality(), rc = node.getRight().getCardinality();
            int diff = comparator.compare(lc, rc);
            Cardinality minC = diff <= 0 ? lc : rc;
            Cardinality.Reliability lr = lc.getReliability(), rr = rc.getReliability();
            if (lr.isAtLeast(UPPER_BOUND) && rr.isAtLeast(UPPER_BOUND)) {
                if (minC.getValue(Integer.MAX_VALUE) < 1024)
                    return hashExecutor.innerExecute(node);
            }

            Op m = comparator.compare(rc, lc) >= 0 ? node.getRight() : node.getLeft();
            boolean askDegenerate = node.getJoinVars().containsAll(m.getPublicVars());
            Cardinality askDegenCeil = comparator.min(Cardinality.guess(2000),
                                                      multiply(minC, 2));
            if (askDegenerate && comparator.compare(m.getCardinality(), askDegenCeil) <= 0)
                return hashExecutor.innerExecute(node);

            return bindExecutor.innerExecute(node);
        }
    }
}
