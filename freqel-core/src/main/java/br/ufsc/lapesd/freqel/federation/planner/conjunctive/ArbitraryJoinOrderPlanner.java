package br.ufsc.lapesd.freqel.federation.planner.conjunctive;

import br.ufsc.lapesd.freqel.algebra.JoinInfo;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.federation.PerformanceListener;
import br.ufsc.lapesd.freqel.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.freqel.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.freqel.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.freqel.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.freqel.query.modifiers.Optional;
import br.ufsc.lapesd.freqel.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Collection;

import static br.ufsc.lapesd.freqel.algebra.util.TreeUtils.cleanEquivalents;
import static com.google.common.base.Preconditions.checkArgument;

public class ArbitraryJoinOrderPlanner implements JoinOrderPlanner {
    private final @Nonnull PerformanceListener performance;

    @Inject
    public ArbitraryJoinOrderPlanner(@Nonnull PerformanceListener performance) {
        this.performance = performance;
    }

    public ArbitraryJoinOrderPlanner() {
        this(NoOpPerformanceListener.INSTANCE);
    }

    @Override
    public @Nonnull Op plan(@Nonnull JoinGraph joinGraph,
                            @Nonnull Collection<Op> nodesCollection) {
        try (TimeSampler ignored = Metrics.OPT_MS.createThreadSampler(performance)) {
            checkArgument(!nodesCollection.isEmpty(), "Cannot plan joins without any nodes!");
            RefIndexSet<Op> nodes = RefIndexSet.fromRefDistinct(nodesCollection);
            IndexSubset<Op> pending = nodes.fullSubset();
            Op root = pending.iterator().next();
            boolean optional = root.modifiers().optional() != null;
            pending.remove(root);
            root = cleanEquivalents(root);
            while (!pending.isEmpty()) {
                Op selected = null;
                for (Op candidate : pending) {
                    if (JoinInfo.getJoinability(root, candidate).isValid()) {
                        selected = candidate;
                        break;
                    }
                }
                if (selected == null)
                    throw new IllegalArgumentException("nodesCollection is not join-connected");
                pending.remove(selected);
                root = JoinOp.create(root, cleanEquivalents(selected));
                if ((optional &= selected.modifiers().optional() != null))
                    root.modifiers().add(Optional.IMPLICIT);
            }
            return root;
        }
    }

    @Override public @Nonnull String toString() {
        return getClass().getSimpleName();
    }
}
