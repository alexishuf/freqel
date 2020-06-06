package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.PerformanceListener;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityEnsemble;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityUtils;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.TimeSampler;
import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.TreeUtils;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.CardinalityAdder;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import br.ufsc.lapesd.riefederator.webapis.WebApiEndpoint;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.*;

import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.cleanEquivalents;
import static br.ufsc.lapesd.riefederator.query.Cardinality.Reliability.*;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Integer.MAX_VALUE;

public class GreedyJoinOrderPlanner implements JoinOrderPlanner {
    private final @Nonnull PerformanceListener performance;
    private final @Nonnull CardinalityEnsemble cardEnsemble;
    private final @Nonnull CardinalityAdder cardAdder;

    @Inject
    public GreedyJoinOrderPlanner(@Nonnull PerformanceListener performance,
                                  @Nonnull CardinalityEnsemble cardEnsemble,
                                  @Nonnull CardinalityAdder cardinalityAdder) {
        this.performance = performance;
        this.cardEnsemble = cardEnsemble;
        this.cardAdder = cardinalityAdder;
    }

    @VisibleForTesting
    class Data {
        private @Nonnull final JoinGraph graph;
        public @Nonnull final IndexedSet<PlanNode> clean;
        public @Nonnull final IndexedSubset<PlanNode> pending;
        public @Nonnull final IndexedSubset<PlanNode> webApi;

        public Data(@Nonnull JoinGraph graph, @Nonnull Collection<PlanNode> nodesCollection) {
            this.graph = graph;
            List<PlanNode> cleanList = new ArrayList<>();
            for (PlanNode node : nodesCollection) {
                PlanNode c = cleanEquivalents(node, OrderTuple.NODE_COMPARATOR);
                c.setCardinality(TreeUtils.estimate(c, cardEnsemble, cardAdder));
                cleanList.add(c);
            }
            this.clean = IndexedSet.fromDistinct(cleanList);
            this.webApi = clean.subset(OrderTuple::hasWebApi);
            this.pending = clean.fullSubset();

        }

        @Nullable JoinInfo weight(@Nonnull PlanNode a, @Nonnull PlanNode b) {
            IndexedSet<PlanNode> nodes = graph.getNodes();
            if (nodes.contains(a) && nodes.contains(b))
                return graph.getWeight(a, b);
            return JoinInfo.getPlainJoinability(a, b);
        }

        @Nonnull PlanNode take(@Nonnull PlanNode node) {
            assert clean.contains(node);
            assert pending.contains(node);
            pending.remove(node);
            return node;
        }
    }

    @VisibleForTesting @Nonnull Data createData(JoinGraph graph) {
        return new Data(graph, graph.getNodes());
    }

    @Override
    public @Nonnull PlanNode plan(@Nonnull JoinGraph joinGraph,
                                  @Nonnull Collection<PlanNode> nodesCollection) {
        try (TimeSampler ignored = Metrics.OPT_MS.createThreadSampler(performance)) {
            checkArgument(!nodesCollection.isEmpty(),
                          "Cannot optimize joins without nodes to join!");
            Data d = new Data(joinGraph, nodesCollection);
            Weigher weigher = new Weigher(takeInitialJoin(d));
            while (!d.pending.isEmpty()) {
                PlanNode best = d.pending.stream().min(weigher.comparator).orElse(null);
                weigher.root = JoinNode.builder(weigher.root, d.take(best)).build();
            }
            return weigher.root;
        }
    }

    static @Nonnull PlanNode takeInitialJoin(@Nonnull Data d) {
        assert d.pending.containsAll(d.clean) : "There are non-pending nodes!";
        int size = d.pending.size();
        checkArgument(size > 0, "No pending nodes, no join!");
        if (size == 1)
            return d.take(d.pending.iterator().next());

        PlanNode bestOuter = null, bestInner = null;
        OrderTuple best = OrderTuple.MAX;

        for (int i = 0; i < size; i++) {
            PlanNode outer = d.clean.get(i);
            for (int j = i+1; j < size; j++) {
                PlanNode inner = d.clean.get(j);
                JoinInfo info = d.weight(outer, inner);
                if (info == null || !info.isValid())
                    continue;

                // Estimate join cardinality using the worst reliability and average value
                // Rationale: not all relationships underlying a join are 1:1 (where min
                // would be the correct estimation).
                Cardinality lc = outer.getCardinality(), rc = inner.getCardinality();
                Cardinality avgCard;
                if (lc.equals(Cardinality.EMPTY) || rc.equals(Cardinality.EMPTY))
                    avgCard = Cardinality.EMPTY;
                else
                    avgCard = CardinalityUtils.worstAvg(OrderTuple::compareCardinality, lc, rc);

                // Build OrderTuple for this hypothetical join and compare to best
                boolean isWebApi = d.webApi.contains(outer) || d.webApi.contains(inner);
                int pendingInputs = info.getPendingRequiredInputs().size();
                OrderTuple tuple = new OrderTuple(avgCard, pendingInputs, isWebApi);
                if (tuple.compareTo(best) < 0) {
                    bestOuter = outer;
                    bestInner = inner;
                    best = tuple;
                }
            }
        }

        checkArgument(bestOuter != null,
                      "Found no joins in JoinGraph (with "+size+" nodes)!");
        int diff = OrderTuple.NODE_COMPARATOR.compare(bestOuter, bestInner);
        if (diff > 0) { // swap so that the best node is the left node
            PlanNode tmp = bestOuter;
            bestOuter = bestInner;
            bestInner = tmp;
        }
        return JoinNode.builder(d.take(bestOuter), d.take(bestInner)).build();
    }

    @VisibleForTesting
    private static class Weigher implements Function<PlanNode, OrderTuple> {
        public @Nonnull PlanNode root;
        public Comparator<PlanNode> comparator;

        public Weigher(@Nonnull PlanNode root) {
            this.root = root;
            comparator = Comparator.comparing(this);
        }

        @Override
        public @Nonnull OrderTuple apply(@Nullable PlanNode node) {
            checkNotNull(node);
            boolean hasWebApi = OrderTuple.hasWebApi(node);
            JoinInfo i = JoinInfo.getPlainJoinability(root, node);
            if (!i.isValid()) return OrderTuple.MAX;

            int pendingInputs = i.getPendingRequiredInputs().size();
            Cardinality cardinality = node.getCardinality();

            // if the join leaves pending inputs, node (and possibly other nodes under root)
            // will be re-instantiated to bind the variable. This will very likely occur more
            // than once. If cardinality of node is reliable, degrade it to lower bound to
            // represent that effect
            if (pendingInputs > 0 && cardinality.getReliability().ordinal() > LOWER_BOUND.ordinal())
                cardinality = Cardinality.lowerBound(cardinality.getValue(MAX_VALUE)*2);

            return new OrderTuple(cardinality, pendingInputs, hasWebApi);
        }
    }

    static class OrderTuple implements Comparable<OrderTuple> {
        public static final @Nonnull OrderTuple MAX =
                new OrderTuple(Cardinality.lowerBound(MAX_VALUE), MAX_VALUE, false);
        public static final @Nonnull Comparator<PlanNode> NODE_COMPARATOR
                = Comparator.comparing(OrderTuple::new);

        private static final int SMALL = 8;
        private static final int BIG   = 64;
        private static final int HUGE = 10000;
        public @Nonnull Cardinality cardinality;
        public int pendingInputs;
        public boolean isWebApi;

        public OrderTuple(@Nonnull Cardinality cardinality, int pendingInputs, boolean isWebApi) {
            checkArgument(pendingInputs >= 0, "pendingInputs "+pendingInputs+" must be >= 0");
            this.cardinality = cardinality;
            this.pendingInputs = pendingInputs;
            this.isWebApi = isWebApi;
        }

        public OrderTuple(@Nonnull PlanNode node) {
            this(node.getCardinality(), 0, hasWebApi(node));
        }

        public static boolean hasWebApi(@Nonnull PlanNode node) {
            return TreeUtils.childrenIfMulti(node).stream()
                    .anyMatch(n -> n instanceof QueryNode &&
                            ((QueryNode)n).getEndpoint() instanceof WebApiEndpoint);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OrderTuple)) return false;
            OrderTuple that = (OrderTuple) o;
            return pendingInputs == that.pendingInputs &&
                    isWebApi == that.isWebApi &&
                    cardinality.equals(that.cardinality);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cardinality, pendingInputs, isWebApi);
        }

        @Override
        public @Nonnull String toString() {
            return String.format("(%s, %s, %s)", cardinality, pendingInputs,
                                                 (isWebApi ? "" : '¬')+"WebAPI");
        }

        @Override
        public int compareTo(@NotNull GreedyJoinOrderPlanner.OrderTuple o) {
            int diff = compareCardinality(cardinality, o.cardinality);
            if (diff != 0) return diff;

            diff = Integer.compare(pendingInputs, o.pendingInputs);
            if (diff != 0) return diff;

            // web apis take priority (ordered first)
            return isWebApi == o.isWebApi ? 0 : (isWebApi ? -1 : 1);
        }

        public static int compareCardinality(@Nonnull Cardinality l, @Nonnull Cardinality r) {
            Cardinality.Reliability lr = l.getReliability(), rr = r.getReliability();
            int lv = l.getValue(MAX_VALUE), rv = r.getValue(MAX_VALUE);

            if (lr == UNSUPPORTED && rr == UNSUPPORTED) return 0;
            // If we guess or know a side has a HUGE value, prefer the side with UNSUPPORTED card
            if (lr == UNSUPPORTED) return rv > HUGE ? -1 :   1;
            if (rr == UNSUPPORTED) return lv > HUGE ?  1 :  -1;

            if (isGuess(lr) && isGuess(rr)) {
                // if values are close, but reliabilities differ, the most reliable is the smaller
                if (lr != rr && isClose(lv, rv, 0.05, BIG))
                    return -1 * Integer.compare(lr.ordinal(), rr.ordinal());
                if (lr == rr && isClose(lv, rv, 0.01, SMALL))
                    return 0; //if same reliability with small difference, consider equal
                // if values are not close or lr==rr, smaller value is smaller cardinality
                return Integer.compare(lv, rv);
            } else if (isGuess(lr)) {
                assert rr.ordinal() > LOWER_BOUND.ordinal();
                return 1;
            } else if (isGuess(rr)) {
                assert lr.ordinal() > LOWER_BOUND.ordinal();
                return -1;
            }

            // at this point we have reliable values on both sides. Compare by value
            assert rr.ordinal() > LOWER_BOUND.ordinal();
            assert lr.ordinal() > LOWER_BOUND.ordinal();
            return Integer.compare(lv, rv);
        }

        private static boolean isClose(int l, int r, double proportion, int threshold) {
            assert proportion >= 0;
            assert proportion < 1;

            int max = Math.max(l, r), min = Math.min(l, r);
            return max-min <= threshold || min >= max*(1-proportion);
        }

        private static boolean isGuess(Cardinality.Reliability lr) {
            return lr == LOWER_BOUND || lr == GUESS || lr == NON_EMPTY;
        }
    }
}
