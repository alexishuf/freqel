package br.ufsc.lapesd.riefederator.federation.planner.conjunctive;

import br.ufsc.lapesd.riefederator.algebra.JoinInfo;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.query.modifiers.Optional;
import br.ufsc.lapesd.riefederator.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Collection;

import static br.ufsc.lapesd.riefederator.algebra.util.TreeUtils.cleanEquivalents;
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

    //    @Override
//    public @Nonnull PlanNode plan(@Nonnull List<JoinInfo> joins, @Nullable JoinGraph ignored) {
//        checkArgument(!joins.isEmpty());
//        JoinNode root = null;
//        JoinInfo last = null;
//        for (JoinInfo info : joins) {
//            if (root == null) {
//                PlanNode l = cleanEquivalents(info.getLeft()), r = cleanEquivalents(info.getRight());
//                root = JoinNode.builder(l, r).build();
//            } else {
//                PlanNode clean = cleanEquivalents(info.getOppositeToLinked(last));
//                root = JoinNode.builder(root, clean).build();
//            }
//            last = info;
//        }
//        return root;
//    }

}
