package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.util.CollectionUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Stream.concat;

@Immutable
public class JoinInfo {
    private static final Logger logger = LoggerFactory.getLogger(JoinInfo.class);

    private final @Nonnull ImmutableSet<String> joinVars, pendingReqInputs, pendingOptInputs;
    @SuppressWarnings("Immutable")
    private final @Nonnull ImmutableList<Op> leftDead, rightDead;
    private final boolean subsumed;
    @SuppressWarnings("Immutable")
    private final @Nonnull Op left, right;

    protected JoinInfo(@Nonnull Op left, @Nonnull Op right, @Nullable Set<String> allowedJoinVars) {
        this.left = left;
        this.right = right;
        Set<Triple> lm = left.getMatchedTriples(), rm = right.getMatchedTriples();
        if (lm.containsAll(rm) || rm.containsAll(lm)) {
            joinVars = pendingReqInputs = pendingOptInputs = ImmutableSet.of();
            leftDead = rightDead = ImmutableList.of();
            subsumed = true;
        } else {
            ImmutableSet.Builder<String> pendingReqB = ImmutableSet.builder();
            ImmutableSet.Builder<String> pendingOptB = ImmutableSet.builder();
            joinVars = joinVars(left, right, pendingReqB, pendingOptB, allowedJoinVars);
            pendingReqInputs = pendingReqB.build();
            pendingOptInputs = pendingOptB.build();
            leftDead = rightDead = ImmutableList.of();
            subsumed = false;
        }
    }

    private static @Nonnull ImmutableSet<String>
    joinVars(@Nonnull Op l, @Nonnull Op r, @Nonnull ImmutableSet.Builder<String> pendingReqIns,
             @Nonnull ImmutableSet.Builder<String> pendingOptIns, @Nullable Set<String> allowed) {
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

    enum Position {
        LEFT, RIGHT
    }

    public static @Nonnull JoinInfo getJoinability(@Nonnull Op left, @Nonnull Op right) {
        return getJoinability(left, right, null);
    }

    public static @Nonnull JoinInfo getJoinability(@Nonnull Op left, @Nonnull Op right,
                                                   @Nullable Set<String> allowedJoinVars) {
        return new JoinInfo(left, right, allowedJoinVars);
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

    public @Nonnull Op getOppositeToLinked(@Nonnull JoinInfo linked) {
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

    public @Nonnull Op getLeft() {
        return left;
    }

    public @Nonnull Op getRight() {
        return right;
    }

    public @Nonnull Op get(@Nonnull Position position) {
        switch (position) {
            case LEFT: return getLeft();
            case RIGHT: return getRight();
            default: break;
        }
        throw new IllegalArgumentException("Bad Position: "+position);
    }

    public @Nonnull ImmutableList<Op> getLeftDead() {
        return leftDead;
    }

    public @Nonnull ImmutableList<Op> getRightDead() {
        return rightDead;
    }

    public @Nonnull List<Op> getLeftNodes() {
        return ImmutableList.of(left);
    }

    public @Nonnull List<Op> getRightNodes() {
        return ImmutableList.of(right);
    }

    public @Nonnull List<Op> getNodes() {
        return Arrays.asList(left, right);
    }

    public @Nonnull List<Op> getNodes(@Nonnull Position position) {
        switch (position) {
            case LEFT: return getLeftNodes();
            case RIGHT: return getRightNodes();
            default: break;
        }
        throw new IllegalArgumentException("Bad Position: "+position);
    }

    public @Nonnull StringBuilder toString(StringBuilder builder) {
        if (!isValid()) return  builder.append("∅");
        String comma = ", ";
        builder.append(String.join(comma, joinVars));
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
                getPendingOptionalInputs().equals(joinInfo.getPendingOptionalInputs());
    }

    @Override
    public int hashCode() {
        return Objects.hash(isSubsumed(), getJoinVars(), getPendingRequiredInputs(),
                            getPendingOptionalInputs());
    }
}
