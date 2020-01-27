package br.ufsc.lapesd.riefederator.federation.planner.impl;

import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.TreeUtils;
import br.ufsc.lapesd.riefederator.model.Triple;
import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static com.google.common.collect.Lists.cartesianProduct;
import static java.util.Collections.singletonList;
import static java.util.stream.Stream.concat;

public class Joinability {
    private final @Nonnull Set<String> joinVars, pendingInputs;
    private final @Nonnull Map<ImmutablePair<PlanNode, PlanNode>, Joinability> childJoins;
    private final @Nonnull List<PlanNode> leftDead, rightDead;
    private final boolean subsumed, expandMultiNodes;
    private final @Nonnull PlanNode left, right;

    protected Joinability(@Nonnull PlanNode left, @Nonnull PlanNode right,
                          boolean expandMultiNodes) {
        this.left = left;
        this.right = right;
        this.expandMultiNodes = expandMultiNodes;
        Set<Triple> lm = left.getMatchedTriples(), rm = right.getMatchedTriples();
        if (lm.containsAll(rm) || rm.containsAll(lm)) {
            joinVars = pendingInputs = Collections.emptySet();
            childJoins = Collections.emptyMap();
            leftDead = rightDead = Collections.emptyList();
            subsumed = true;
        } else if (!expandMultiNodes) {
            pendingInputs = new HashSet<>();
            joinVars = joinVars(left, right, pendingInputs);
            childJoins = Collections.emptyMap();
            leftDead = rightDead = Collections.emptyList();
            subsumed = false;
        } else {
            List<PlanNode> lns, rns;
            lns = (left instanceof MultiQueryNode) ? left.getChildren() : singletonList(left);
            rns = (right instanceof MultiQueryNode) ? right.getChildren() : singletonList(right);
            childJoins = new HashMap<>(lns.size()*rns.size());
            fillChildJoins(lns, rns);

            Set<PlanNode> ld = new HashSet<>(lns), rd = new HashSet<>(rns);
            for (ImmutablePair<PlanNode, PlanNode> pair : childJoins.keySet()) {
                ld.remove(pair.left);
                rd.remove(pair.right);
            }
            leftDead = new ArrayList<>(ld);
            rightDead = new ArrayList<>(rd);
            subsumed = false;
            if (!childJoins.isEmpty()) {
                pendingInputs = new HashSet<>();
                joinVars = joinVars(left, right, pendingInputs);
            } else {
                joinVars = pendingInputs = Collections.emptySet();
            }
        }
    }

    private static @Nonnull Set<String> joinVars(@Nonnull PlanNode l, @Nonnull PlanNode r,
                                                @Nullable Set<String> pendingIns) {
        Set<String> s = TreeUtils.intersect(l.getResultVars(), r.getResultVars());
        Set<String> lIn = l.getInputVars();
        Set<String> rIn = r.getInputVars();
        if (pendingIns != null)
            pendingIns.clear();
        if (l.hasInputs() && r.hasInputs())
            s.removeIf(n -> lIn.contains(n) && rIn.contains(n));
        if (pendingIns != null)
            concat(lIn.stream(), rIn.stream()).filter(n -> !s.contains(n)).forEach(pendingIns::add);
        return s;
    }

    private void fillChildJoins(List<PlanNode> lns, List<PlanNode> rns) {
        for (List<PlanNode> pair : cartesianProduct(lns, rns)) {
            Joinability joinability = getPlainJoinability(pair.get(0), pair.get(1));
            if (joinability.isValid())
                childJoins.put(ImmutablePair.of(pair.get(0), pair.get(1)), joinability);
        }
    }

    public static @Nonnull Joinability
    getPlainJoinability(@Nonnull PlanNode left, @Nonnull PlanNode right) {
        return new Joinability(left, right, false);
    }

    public static @Nonnull Joinability
    getMultiJoinability(@Nonnull PlanNode left, @Nonnull PlanNode right) {
        return new Joinability(left, right, true);
    }

    public boolean isValid() {
        return !joinVars.isEmpty();
    }

    public boolean isSubsumed() {
        return subsumed;
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

    public @Nonnull List<PlanNode> getLeftDead() {
        return leftDead;
    }

    public @Nonnull List<PlanNode> getRightDead() {
        return rightDead;
    }

    public @Nonnull List<PlanNode> getLeftNodes() {
        if (!expandMultiNodes) return singletonList(left);
        return left instanceof MultiQueryNode ? left.getChildren() : singletonList(left);
    }

    public @Nonnull List<PlanNode> getRightNodes() {
        if (!expandMultiNodes) return singletonList(right);
        return right instanceof MultiQueryNode ? right.getChildren() : singletonList(right);
    }

    public boolean hasChildJoins() {
        return !childJoins.isEmpty();
    }

    public @Nonnull Map<ImmutablePair<PlanNode, PlanNode>, Joinability> getChildJoins() {
        return childJoins;
    }

    public @Nonnull StringBuilder toString(StringBuilder builder) {
        if (!isValid()) return  builder.append("∅");
        String comma = ", ";
        if (expandMultiNodes) {
            builder.append(String.join(comma, joinVars));
        } else {
            assert !getChildJoins().isEmpty();
            builder.append('{');
            for (Joinability joinability : getChildJoins().values())
                joinability.toString(builder).append(", ");
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
        if (!(o instanceof Joinability)) return false;
        Joinability that = (Joinability) o;
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
