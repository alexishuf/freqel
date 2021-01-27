package br.ufsc.lapesd.freqel.federation.planner.pre.steps;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.modifiers.Distinct;
import br.ufsc.lapesd.freqel.util.ref.IdentityHashSet;
import br.ufsc.lapesd.freqel.util.ref.RefSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

import static br.ufsc.lapesd.freqel.util.CollectionUtils.hasIntersect;

public class UnionDistributionStep extends AbstractDistributionStep {

    @Override
    public @Nonnull Op visit(@Nonnull Op parent, @Nonnull RefSet<Op> shared) {
        if (!(parent instanceof ConjunctionOp))
            return parent;
        // C(U(a{x}, b{x,y}, c{y}), P(d, e), f) --> C(U(af, bf, c), P(d, e))
        QueryOp outerQueryOp = getQuery(parent, shared);
        if (outerQueryOp == null) return parent; // no op to push into union
        MutableCQuery outerQuery = outerQueryOp.getQuery();
        Set<String> outerVars = outerQueryOp.getResultVars();

        boolean hasExtra = false;
        List<Op> children = parent.getChildren();
        RefSet<Op> matches = new IdentityHashSet<>(children.size()*children.size());
        for (Op child : children) {
            if (child instanceof UnionOp) {
                for (Op grandChild : child.getChildren()) {
                    if (hasIntersect(grandChild.getResultVars(), outerVars)) {
                        if (!(grandChild instanceof QueryOp)
                                || !((QueryOp) grandChild).getQuery().canMergeWith(outerQuery)
                                || shared.contains(grandChild)) {
                            return parent; //there is a inner node to which we cannot merge, abort
                        }
                        matches.add(grandChild);
                    }
                }
            } else if (child != outerQueryOp) {
                hasExtra = true;
            }
        }
        if (matches.isEmpty())
            return parent;

        return raiseUnion((ConjunctionOp) parent, outerQueryOp, hasExtra, matches);
    }

    private @Nonnull Op raiseUnion(@Nonnull ConjunctionOp parent, @Nonnull QueryOp outerQueryOp,
                                   boolean hasExtra, @Nonnull RefSet<Op> matches) {
        MutableCQuery outerQuery = outerQueryOp.getQuery();
        ConjunctionOp.Builder conjBuilder = hasExtra ? ConjunctionOp.builder() : null;
        UnionOp.Builder unionBuilder = UnionOp.builder();
        boolean makeDistinct = true;
        for (Op child : parent.getChildren()) {
            if (child instanceof UnionOp) {
                Distinct distinct = child.modifiers().distinct();
                makeDistinct &= distinct != null;
                for (Op grandchild : child.getChildren()) {
                    if (matches.contains(grandchild)) {
                        MutableCQuery query = ((QueryOp) grandchild).getQuery();
                        query.mergeWith(outerQuery); //die if unsafe
                        grandchild.purgeCaches();
                    }
                    unionBuilder.add(grandchild);
                }
            } else if (child != outerQueryOp) {
                assert  conjBuilder != null;
                conjBuilder.add(child);
            }
        }
        parent.detachChildren();
        if (makeDistinct)
            unionBuilder.add(Distinct.INSTANCE);
        Op union = unionBuilder.build();
        return (conjBuilder != null ? conjBuilder.add(union).build() : union);
    }
}
