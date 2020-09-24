package br.ufsc.lapesd.riefederator.algebra;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.util.CollectionUtils;
import br.ufsc.lapesd.riefederator.util.indexed.NotInParentException;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
import com.google.errorprone.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

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

    public JoinInfo(@Nonnull Op left, @Nonnull Op right, @Nonnull Set<String> joinVars) {
        this.left = left;
        this.right = right;
        this.joinVars = joinVars;
        this.subsumed = false;
        assert validJoinVars(left, right, joinVars);
    }

    private boolean validJoinVars(@Nonnull Op left, @Nonnull Op right,
                                  @Nonnull Set<String> joinVars) {
        if (joinVars.isEmpty())
            return false; // no join variables
        Set<String> shared = CollectionUtils.intersect(left.getPublicVars(), right.getPublicVars());
        if (!shared.containsAll(joinVars))
            return false; // joinVars has non-shared variable
        Set<Triple> lm = left.getMatchedTriples(), rm = right.getMatchedTriples();
        if (lm.containsAll(rm) || rm.containsAll(lm))
            return false; // one node subsumes the other
        for (String lIn : left.getRequiredInputVars()) {
            if (right.getRequiredInputVars().contains(lIn))
                shared.remove(lIn);
        }
        return shared.containsAll(joinVars); // fails if a join var is required on both sides
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
        if (lv instanceof IndexSubset) {
            IndexSubset<String> shared = ((IndexSubset<String>) lv).createIntersection(rv);
            joinVars.removeAll(shared);
            return !joinVars.isEmpty();
        }

        // general case
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
        if (set1 instanceof IndexSubset) {
            try {
                IndexSubset<String> ss = ((IndexSubset<String>) set1).createUnion(set2);
                ss.minus(joinVars);
                return ss;
            } catch (NotInParentException ignored) { /* fallback */ }
        }
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
