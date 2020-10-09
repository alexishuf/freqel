package br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset.priv;

import br.ufsc.lapesd.riefederator.algebra.JoinInfo;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.util.Bitset;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.ref.RefIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;

import javax.annotation.Nonnull;
import java.util.Collection;

public class InputsBitJoinGraph extends BitJoinGraph {
    public InputsBitJoinGraph(@Nonnull RefIndexSet<Op> nodes) {
        super(nodes);
        assert sameUniverse(nodes);
    }

    protected static class InputsAdjacencyChecker extends AdjacencyChecker {
        IndexSubset<String> li, ri, jv;

        @Override public void setLeft(@Nonnull Op left) {
            super.setLeft(left);
            li = (IndexSubset<String>)left.getRequiredInputVars();
        }

        @Override public void setRight(@Nonnull Op right) {
            super.setRight(right);
            ri = (IndexSubset<String>)right.getRequiredInputVars();
        }

        @Override public boolean checkAdjacent() {
            Bitset joinVars = lv.getBitset().copy();
            joinVars.and(rv.getBitset());
            if (!joinVars.isEmpty()) {
                if (lt.containsAll(rt) || rt.containsAll(lt)) { // abort
                    this.jv = null;
                    return false;
                } else { //remove vars that are required inputs on both sides
                    Bitset sharedInputs = li.getBitset().copy();
                    sharedInputs.and(ri.getBitset());
                    joinVars.andNot(sharedInputs);
                }
            }
            this.jv = joinVars.isEmpty() ? null : lv.getParent().subset(joinVars);
            return this.jv != null;
        }

        @Override public @Nonnull JoinInfo getJoinInfo() {
            assert jv != null : "checkAdjacent not called!";
            JoinInfo info = new JoinInfo(left, right, jv);
            assert info.equals(JoinInfo.getJoinability(left, right)) : "Computed JoinInfo is wrong";
            return info;
        }
    }

    @Override protected @Nonnull AdjacencyChecker createAdjacencyChecker() {
        return new InputsAdjacencyChecker();
    }

    @Override public void notifyAddedNodes() {
        assert sameUniverse(nodes);
        super.notifyAddedNodes();
    }

    private boolean sameUniverse(Collection<Op> nodes) {
        IndexSet<String> parent = null, cand;
        for (Op node : nodes) {
            cand = ((IndexSubset<String>) node.getPublicVars()).getParent();
            if      (parent == null) parent = cand;
            else if (parent != cand) return false;
            cand = ((IndexSubset<String>) node.getRequiredInputVars()).getParent();
            if (parent != cand) return false;
        }
        assert parent != null || nodes.isEmpty();
        return true; // no inconsistencies
    }
}
