package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.TreeUtils;
import br.ufsc.lapesd.riefederator.model.Triple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.annotation.Nonnull;
import java.util.*;

import static com.google.common.collect.Lists.cartesianProduct;
import static java.util.Collections.singletonList;
import static java.util.stream.Stream.concat;

@Immutable
public class JoinInfo {
    private final @Nonnull ImmutableSet<String> joinVars, pendingInputs;
    @SuppressWarnings("Immutable")
    private final @Nonnull ImmutableMap<ImmutablePair<PlanNode, PlanNode>, JoinInfo> childJoins;
    @SuppressWarnings("Immutable")
    private final @Nonnull ImmutableList<PlanNode> leftDead, rightDead;
    private final boolean subsumed, expandMultiNodes;
    @SuppressWarnings("Immutable")
    private final @Nonnull PlanNode left, right;

    protected JoinInfo(@Nonnull PlanNode left, @Nonnull PlanNode right,
                       boolean expandMultiNodes) {
        this.left = left;
        this.right = right;
        this.expandMultiNodes = expandMultiNodes;
        Set<Triple> lm = left.getMatchedTriples(), rm = right.getMatchedTriples();
        if (lm.containsAll(rm) || rm.containsAll(lm)) {
            joinVars = pendingInputs = ImmutableSet.of();
            childJoins = ImmutableMap.of();
            leftDead = rightDead = ImmutableList.of();
            subsumed = true;
        } else if (!expandMultiNodes) {
            ImmutableSet.Builder<String> pendingInputsBuilder = ImmutableSet.builder();
            joinVars = joinVars(left, right, pendingInputsBuilder);
            pendingInputs = pendingInputsBuilder.build();
            childJoins = ImmutableMap.of();
            leftDead = rightDead = ImmutableList.of();
            subsumed = false;
        } else {
            List<PlanNode> lns, rns;
            lns = (left instanceof MultiQueryNode) ? left.getChildren() : singletonList(left);
            rns = (right instanceof MultiQueryNode) ? right.getChildren() : singletonList(right);
            childJoins = fillChildJoins(lns, rns);

            Set<PlanNode> ld = new HashSet<>(lns), rd = new HashSet<>(rns);
            for (ImmutablePair<PlanNode, PlanNode> pair : childJoins.keySet()) {
                ld.remove(pair.left);
                rd.remove(pair.right);
            }
            leftDead = ImmutableList.copyOf(ld);
            rightDead = ImmutableList.copyOf(rd);
            subsumed = false;
            if (!childJoins.isEmpty()) {
                ImmutableSet.Builder<String> pendingInputsBuilder = ImmutableSet.builder();
                joinVars = joinVars(left, right, pendingInputsBuilder);
                pendingInputs = pendingInputsBuilder.build();
            } else {
                joinVars = pendingInputs = ImmutableSet.of();
            }
        }
    }

    private static @Nonnull ImmutableSet<String>
    joinVars(@Nonnull PlanNode l, @Nonnull PlanNode r,
             @Nonnull ImmutableSet.Builder<String> pendingIns){
        Set<String> s = TreeUtils.intersect(l.getResultVars(), r.getResultVars());
        Set<String> lIn = l.getInputVars();
        Set<String> rIn = r.getInputVars();
        if (l.hasInputs() && r.hasInputs())
            s.removeIf(n -> lIn.contains(n) && rIn.contains(n));
        concat(lIn.stream(), rIn.stream()).filter(n -> !s.contains(n)).forEach(pendingIns::add);
        return ImmutableSet.copyOf(s);
    }

    private ImmutableMap<ImmutablePair<PlanNode, PlanNode>, JoinInfo>
    fillChildJoins(List<PlanNode> lns, List<PlanNode> rns) {
        ImmutableMap.Builder<ImmutablePair<PlanNode, PlanNode>, JoinInfo> builder;
        //noinspection UnstableApiUsage
        builder = ImmutableMap.builderWithExpectedSize(lns.size() * rns.size());

        for (List<PlanNode> pair : cartesianProduct(lns, rns)) {
            JoinInfo joinInfo = getPlainJoinability(pair.get(0), pair.get(1));
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
        return new JoinInfo(left, right, false);
    }

    public static @Nonnull
    JoinInfo getMultiJoinability(@Nonnull PlanNode left, @Nonnull PlanNode right) {
        return new JoinInfo(left, right, true);
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

    public @Nonnull Set<String> getPendingInputs() {
        return pendingInputs;
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
        if (!pendingInputs.isEmpty())
            builder.append("+Inputs(").append(String.join(comma, pendingInputs)).append(")");
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
        JoinInfo that = (JoinInfo) o;
        return  isSubsumed() == that.isSubsumed() &&
                getJoinVars().equals(that.getJoinVars()) &&
                getPendingInputs().equals(that.getPendingInputs()) &&
                getChildJoins().equals(that.getChildJoins());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getJoinVars(), getPendingInputs(), getChildJoins(), isSubsumed());
    }
}
