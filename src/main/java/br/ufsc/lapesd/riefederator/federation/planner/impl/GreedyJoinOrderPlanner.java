package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.planner.impl.paths.JoinGraph;
import br.ufsc.lapesd.riefederator.federation.tree.JoinNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.TreeUtils;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import br.ufsc.lapesd.riefederator.webapis.WebApiEndpoint;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Comparator;
import java.util.Objects;

import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.cleanEquivalents;
import static br.ufsc.lapesd.riefederator.query.Cardinality.Reliability.*;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Integer.MAX_VALUE;

public class GreedyJoinOrderPlanner implements JoinOrderPlanner {

    @Override
    public @Nonnull PlanNode plan(@Nonnull JoinGraph joinGraph,
                                  @Nonnull Collection<PlanNode> nodesCollection) {
        checkArgument(!nodesCollection.isEmpty(), "Cannot optimize joins without nodes to join!");
        IndexedSet<PlanNode> nodes = IndexedSet.from(nodesCollection);
        IndexedSubset<PlanNode> pending = nodes.fullSubset();
        Weigher weigher = new Weigher(takeInitialJoin(joinGraph, pending));
        while (!pending.isEmpty()) {
            PlanNode best = pending.stream().min(weigher.comparator).orElse(null);
            pending.remove(best);
            PlanNode clean = cleanEquivalents(best, OrderTuple.NODE_COMPARATOR);
            weigher.root = JoinNode.builder(weigher.root, clean).build();
        }
        return weigher.root;
    }

    static @Nonnull PlanNode takeInitialJoin(@Nonnull JoinGraph graph,
                                             @Nonnull IndexedSubset<PlanNode> pending) {
        int size = graph.size();
        if (GreedyJoinOrderPlanner.class.desiredAssertionStatus()) {
            checkArgument(graph.getNodes().containsAll(pending), "The set of pending (" +
                    pending.size() + ") nodes should be a subset of the JoinGraph");
        }
        checkArgument(size > 0, "Cannot find best join in empty JoinGraph!");
        if (size == 1) {
            PlanNode node = pending.iterator().next();
            pending.remove(node);
            return cleanEquivalents(node, OrderTuple.NODE_COMPARATOR); // no join to do
        }

        boolean[] hasWebApi = new boolean[size];
        PlanNode[] clean = new PlanNode[size];
        for (int i = 0; i < size; i++) {
            clean[i] = TreeUtils.cleanEquivalents(graph.get(i), OrderTuple.NODE_COMPARATOR);
            hasWebApi[i] = OrderTuple.hasWebApi(clean[i]);
        }

        int bestI = -1, bestJ = -1;
        OrderTuple best = OrderTuple.MAX;
        for (int i = 0; i < size; i++) {
            if (!pending.contains(graph.get(i))) continue;
            for (int j = i+1; j < size; j++) {
                if (!pending.contains(graph.get(j))) continue;
                JoinInfo info = graph.getWeight(i, j);
                if (info == null || !info.isValid())
                    continue;

                Cardinality lCard = clean[i].getCardinality(),
                            rCard = clean[j].getCardinality();
                int cardDiff = OrderTuple.compareCardinality(lCard, rCard);
                Cardinality bestCard = cardDiff <= 0 ? lCard : rCard;
                boolean isWebApi = hasWebApi[i] || hasWebApi[j];
                int pendingInputs = info.getPendingInputs().size();
                OrderTuple tuple = new OrderTuple(bestCard, pendingInputs, isWebApi);
                if (tuple.compareTo(best) < 0) {
                    bestI = i;
                    bestJ = j;
                    best = tuple;
                }
            }
        }

        checkArgument(bestI >= 0,
                     "Found no joins in JoinGraph (with "+size+" nodes)!");
        boolean removedBestI = pending.remove(graph.get(bestI));
        assert removedBestI;
        boolean removedBestJ = pending.remove(graph.get(bestJ));
        assert removedBestJ;
        return JoinNode.builder(clean[bestI], clean[bestJ]).build();
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

            int pendingInputs = i.getPendingInputs().size();
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
        private static final int HUGE = 512;
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
                if (lr != rr && Math.abs(lv - rv) <= BIG)
                    return -1 * Integer.compare(lr.ordinal(), rr.ordinal());
                if (lr == rr && Math.abs(lv - rv) <= SMALL)
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

        private static boolean isGuess(Cardinality.Reliability lr) {
            return lr == LOWER_BOUND || lr == GUESS || lr == NON_EMPTY;
        }
    }
}
