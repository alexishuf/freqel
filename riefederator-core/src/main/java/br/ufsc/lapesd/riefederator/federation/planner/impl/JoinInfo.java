package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.util.CollectionUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static com.google.common.collect.Lists.cartesianProduct;
import static java.util.Collections.singletonList;
import static java.util.stream.Stream.concat;

@Immutable
public class JoinInfo {
    private static final Logger logger = LoggerFactory.getLogger(JoinInfo.class);

    private final @Nonnull ImmutableSet<String> joinVars, pendingReqInputs, pendingOptInputs;
    @SuppressWarnings("Immutable")
    private final @Nonnull ImmutableMap<ImmutablePair<PlanNode, PlanNode>, JoinInfo> childJoins;
    @SuppressWarnings("Immutable")
    private final @Nonnull ImmutableList<PlanNode> leftDead, rightDead;
    private final boolean subsumed, expandMultiNodes;
    @SuppressWarnings("Immutable")
    private final @Nonnull PlanNode left, right;

    protected JoinInfo(@Nonnull PlanNode left, @Nonnull PlanNode right,
                       boolean expandMultiNodes, @Nullable Set<String> allowedJoinVars) {
        this.left = left;
        this.right = right;
        this.expandMultiNodes = expandMultiNodes;
        Set<Triple> lm = left.getMatchedTriples(), rm = right.getMatchedTriples();
        if (lm.containsAll(rm) || rm.containsAll(lm)) {
            joinVars = pendingReqInputs = pendingOptInputs = ImmutableSet.of();
            childJoins = ImmutableMap.of();
            leftDead = rightDead = ImmutableList.of();
            subsumed = true;
        } else if (!expandMultiNodes) {
            ImmutableSet.Builder<String> pendingReqB = ImmutableSet.builder();
            ImmutableSet.Builder<String> pendingOptB = ImmutableSet.builder();
            joinVars = joinVars(left, right, pendingReqB, pendingOptB, allowedJoinVars);
            pendingReqInputs = pendingReqB.build();
            pendingOptInputs = pendingOptB.build();
            childJoins = ImmutableMap.of();
            leftDead = rightDead = ImmutableList.of();
            subsumed = false;
        } else {
            List<PlanNode> lns, rns;
            lns = (left instanceof MultiQueryNode) ? left.getChildren() : singletonList(left);
            rns = (right instanceof MultiQueryNode) ? right.getChildren() : singletonList(right);
            childJoins = fillChildJoins(lns, rns, allowedJoinVars);

            Set<PlanNode> ld = new HashSet<>(lns), rd = new HashSet<>(rns);
            for (ImmutablePair<PlanNode, PlanNode> pair : childJoins.keySet()) {
                ld.remove(pair.left);
                rd.remove(pair.right);
            }
            leftDead = ImmutableList.copyOf(ld);
            rightDead = ImmutableList.copyOf(rd);
            subsumed = false;
            if (!childJoins.isEmpty()) {
                ImmutableSet.Builder<String> pendingReqB = ImmutableSet.builder();
                ImmutableSet.Builder<String> pendingOptB = ImmutableSet.builder();
                joinVars = joinVars(left, right, pendingReqB, pendingOptB, allowedJoinVars);
                pendingReqInputs = pendingReqB.build();
                pendingOptInputs = pendingOptB.build();
            } else {
                joinVars = pendingReqInputs = pendingOptInputs = ImmutableSet.of();
            }
        }
    }

    private static @Nonnull ImmutableSet<String>
    joinVars(@Nonnull PlanNode l, @Nonnull PlanNode r,
             @Nonnull ImmutableSet.Builder<String> pendingReqIns,
             @Nonnull ImmutableSet.Builder<String> pendingOptIns,
             @Nullable Set<String> allowed) {
        Set<String> set = CollectionUtils.intersect(l.getPublicVars(), r.getPublicVars());

        if (allowed != null) {
            if (!set.containsAll(allowed)) {
                logger.warn("Join will fail because allowed={} has variables not contained " +
                            "in {}, shared by l and r nodes.", allowed, set);
                return ImmutableSet.of(); //deliberate fail
            }
            set.retainAll(allowed);
        }
        final Set<String> s = set; //final for lambda usage

        Set<String> lIn = l.getRequiredInputVars();
        Set<String> rIn = r.getRequiredInputVars();
        if (!lIn.isEmpty() && !rIn.isEmpty())
            s.removeIf(n -> lIn.contains(n) && rIn.contains(n));
        concat(lIn.stream(), rIn.stream()).filter(n -> !s.contains(n)).forEach(pendingReqIns::add);
        concat(l.getOptionalInputVars().stream(),
               r.getOptionalInputVars().stream()).filter(n -> !s.contains(n))
                                                 .forEach(pendingOptIns::add);
        return ImmutableSet.copyOf(s);
    }

    private ImmutableMap<ImmutablePair<PlanNode, PlanNode>, JoinInfo>
    fillChildJoins(List<PlanNode> lns, List<PlanNode> rns,
                   @Nullable Set<String> allowedJoinVars) {
        ImmutableMap.Builder<ImmutablePair<PlanNode, PlanNode>, JoinInfo> builder;
        //noinspection UnstableApiUsage
        builder = ImmutableMap.builderWithExpectedSize(lns.size() * rns.size());

        for (List<PlanNode> pair : cartesianProduct(lns, rns)) {
            JoinInfo joinInfo = getPlainJoinability(pair.get(0), pair.get(1), allowedJoinVars);
            if (joinInfo.isValid())
                builder.put(ImmutablePair.of(pair.get(0), pair.get(1)), joinInfo);
        }
        return builder.build();
    }

    enum Position {
        LEFT, RIGHT
    }

    public static @Nonnull
    JoinInfo getPlainJoinability(@Nonnull PlanNode left, @Nonnull PlanNode right) {
        return getPlainJoinability(left, right, null);
    }

    public static @Nonnull
    JoinInfo getPlainJoinability(@Nonnull PlanNode left, @Nonnull PlanNode right,
                                 @Nullable Set<String> allowedJoinVars) {
        return new JoinInfo(left, right, false, allowedJoinVars);
    }

    public static @Nonnull
    JoinInfo getMultiJoinability(@Nonnull PlanNode left, @Nonnull PlanNode right) {
        return getMultiJoinability(left, right, null);
    }

    public static @Nonnull
    JoinInfo getMultiJoinability(@Nonnull PlanNode left, @Nonnull PlanNode right, 
                                 @Nullable Set<String> allowedJoinVars) {
        return new JoinInfo(left, right, true, allowedJoinVars);
    }

    public boolean isValid() {
        return !joinVars.isEmpty();
    }

    public boolean isSubsumed() {
        return subsumed;
    }

    public boolean isLinkedTo(JoinInfo other) {
        return left == other.getLeft() || left == other.getRight()
                || right == other.getLeft() || right == other.getRight();
    }

    public @Nonnull PlanNode getOppositeToLinked(@Nonnull JoinInfo linked) {
        if (left == linked.getLeft() || left == linked.getRight())
            return right;
        else if (right == linked.getLeft() || right == linked.getRight())
            return left;
        throw new IllegalArgumentException("JoinInfo " + linked + " is not linked");
    }

    public @Nonnull Set<String> getJoinVars() {
        return joinVars;
    }

    public @Nonnull Set<String> getPendingRequiredInputs() {
        return pendingReqInputs;
    }

    public @Nonnull ImmutableSet<String> getPendingOptionalInputs() {
        return pendingOptInputs;
    }

    public @Nonnull PlanNode getLeft() {
        return left;
    }

    public @Nonnull PlanNode getRight() {
        return right;
    }

    public @Nonnull PlanNode get(@Nonnull Position position) {
        switch (position) {
            case LEFT: return getLeft();
            case RIGHT: return getRight();
            default: break;
        }
        throw new IllegalArgumentException("Bad Position: "+position);
    }

    public @Nonnull ImmutableList<PlanNode> getLeftDead() {
        return leftDead;
    }

    public @Nonnull ImmutableList<PlanNode> getRightDead() {
        return rightDead;
    }

    public @Nonnull List<PlanNode> getLeftNodes() {
        if (!expandMultiNodes) return ImmutableList.of(left);
        return left instanceof MultiQueryNode ? left.getChildren() : ImmutableList.of(left);
    }

    public @Nonnull List<PlanNode> getRightNodes() {
        if (!expandMultiNodes) return ImmutableList.of(right);
        return right instanceof MultiQueryNode ? right.getChildren() : ImmutableList.of(right);
    }

    public @Nonnull List<PlanNode> getNodes() {
        return Arrays.asList(left, right);
    }

    public @Nonnull List<PlanNode> getNodes(@Nonnull Position position) {
        switch (position) {
            case LEFT: return getLeftNodes();
            case RIGHT: return getRightNodes();
            default: break;
        }
        throw new IllegalArgumentException("Bad Position: "+position);
    }

    public boolean hasChildJoins() {
        return !childJoins.isEmpty();
    }

    public @Nonnull ImmutableMap<ImmutablePair<PlanNode, PlanNode>, JoinInfo> getChildJoins() {
        return childJoins;
    }

    public @Nonnull StringBuilder toString(StringBuilder builder) {
        if (!isValid()) return  builder.append("∅");
        String comma = ", ";
        if (!expandMultiNodes) {
            builder.append(String.join(comma, joinVars));
        } else {
            assert !getChildJoins().isEmpty();
            builder.append('{');
            for (JoinInfo joinInfo : getChildJoins().values())
                joinInfo.toString(builder).append(", ");
            builder.setLength(builder.length()-2);
            builder.append('}');
        }
        if (!pendingReqInputs.isEmpty())
            builder.append("+ReqInputs(").append(String.join(comma, pendingReqInputs)).append(")");
        if (!pendingOptInputs.isEmpty())
            builder.append("+OptInputs(").append(String.join(comma, pendingOptInputs)).append(")");
        return builder;
    }

    @Override
    public @Nonnull String toString() {
        if (!isValid()) return "∅";
        return toString(new StringBuilder()).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JoinInfo)) return false;
        JoinInfo joinInfo = (JoinInfo) o;
        return isSubsumed() == joinInfo.isSubsumed() &&
                getJoinVars().equals(joinInfo.getJoinVars()) &&
                getPendingRequiredInputs().equals(joinInfo.getPendingRequiredInputs()) &&
                getPendingOptionalInputs().equals(joinInfo.getPendingOptionalInputs()) &&
                getChildJoins().equals(joinInfo.getChildJoins());
    }

    @Override
    public int hashCode() {
        return Objects.hash(isSubsumed(), getJoinVars(), getPendingRequiredInputs(),
                            getPendingOptionalInputs(), getChildJoins());
    }
}
