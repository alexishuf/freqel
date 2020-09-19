package br.ufsc.lapesd.riefederator.algebra;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.util.CollectionUtils;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static java.util.Collections.emptySet;

@Immutable
public class JoinInfo {
    private static final Logger logger = LoggerFactory.getLogger(JoinInfo.class);

    @SuppressWarnings("Immutable")
    private Set<String> joinVars, pendingReqInputs, pendingOptInputs;
    private final boolean subsumed;
    @SuppressWarnings("Immutable")
    private final @Nonnull Op left, right;

    protected JoinInfo(@Nonnull Op left, @Nonnull Op right) {
        this.left = left;
        this.right = right;
        boolean subsumed = false;

        if (computeJoinVars()) {
            Set<Triple> lm = left.getMatchedTriples(), rm = right.getMatchedTriples();
            if ((subsumed = lm.containsAll(rm) || rm.containsAll(lm)))
                joinVars = emptySet();
        }
        this.subsumed = subsumed;
        assert joinVars != null;
    }


    private boolean computeJoinVars() {
        joinVars = CollectionUtils.intersect(left.getPublicVars(), right.getPublicVars());
        if (joinVars.isEmpty() || !removeSharedRequiredInputs()) {
            joinVars = emptySet(); // allow emptied-out HashSet to be collected
            return false;
        }
        return true;
    }

    private boolean removeSharedRequiredInputs() {
        Set<String> lv = left.getRequiredInputVars(), rv = right.getRequiredInputVars();
        int lvs = lv.size(), rvs = rv.size();
        if (rvs < lvs) {
            Set<String> tmp = lv; lv = rv; rv = tmp;
        }
        for (String v : lv) {
            if (rv.contains(v))
                joinVars.remove(v);
        }
        return !joinVars.isEmpty();
    }

    private @Nonnull Set<String> pending(@Nonnull Set<String> set1,
                                         @Nonnull Set<String> set2) {
        int size1 = set1.size();
        int capacity = size1 + set2.size();
        if (capacity == 0)
            return emptySet();

        Set<String> result = addPending(set1, capacity);
        if (result == null) result = addPending(set2, capacity-size1);
        else                addPending(result, set2);
        return result == null ? emptySet() : result;
    }
    private @Nullable Set<String> addPending(@Nonnull Set<String> input, int capacity) {
        Set<String> result = null;
        for (String v : input) {
            if (!joinVars.contains(v)) {
                (result == null ? result = new HashSet<>(capacity) : result).add(v);
            } else
                --capacity;
        }
        return result;
    }
    private void addPending(@Nonnull Set<String> result, @Nonnull Set<String> input) {
        for (String v : input) {
            if (!joinVars.contains(v))
                result.add(v);
        }
    }

//    private static @Nonnull ImmutableSet<String>
//    joinVars(@Nonnull Op l, @Nonnull Op r, @Nonnull ImmutableSet.Builder<String> pendingReqIns,
//             @Nonnull ImmutableSet.Builder<String> pendingOptIns, @Nullable Set<String> allowed) {
//        Set<String> set = CollectionUtils.intersect(l.getPublicVars(), r.getPublicVars());
//
//        if (allowed != null) {
//            if (!set.containsAll(allowed)) {
//                logger.warn("Join will fail because allowed={} has variables not contained " +
//                            "in {}, shared by l and r nodes.", allowed, set);
//                return ImmutableSet.of(); //deliberate fail
//            }
//            set.retainAll(allowed);
//        }
//        final Set<String> s = set; //final for lambda usage
//
//        Set<String> lIn = l.getRequiredInputVars();
//        Set<String> rIn = r.getRequiredInputVars();
//        if (!lIn.isEmpty() && !rIn.isEmpty())
//            s.removeIf(n -> lIn.contains(n) && rIn.contains(n));
//        concat(lIn.stream(), rIn.stream()).filter(n -> !s.contains(n)).forEach(pendingReqIns::add);
//        concat(l.getOptionalInputVars().stream(),
//               r.getOptionalInputVars().stream()).filter(n -> !s.contains(n))
//                                                 .forEach(pendingOptIns::add);
//        return ImmutableSet.copyOf(s);
//    }

    enum Position {
        LEFT, RIGHT
    }

    public static @Nonnull JoinInfo getJoinability(@Nonnull Op left, @Nonnull Op right) {
        return new JoinInfo(left, right);
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
        if (pendingReqInputs == null)
            pendingReqInputs = pending(left.getRequiredInputVars(), right.getRequiredInputVars());
        return pendingReqInputs;
    }

    public @Nonnull Set<String> getPendingOptionalInputs() {
        if (pendingOptInputs == null)
            pendingOptInputs = pending(left.getOptionalInputVars(), right.getOptionalInputVars());
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
        Set<String> pReq = getPendingRequiredInputs();
        if (!pReq.isEmpty())
            builder.append("+ReqInputs(").append(String.join(comma, pReq)).append(")");
        Set<String> pOpt = getPendingOptionalInputs();
        if (!pOpt.isEmpty())
            builder.append("+OptInputs(").append(String.join(comma, pOpt)).append(")");
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
