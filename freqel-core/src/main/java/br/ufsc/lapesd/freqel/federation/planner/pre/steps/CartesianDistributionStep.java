package br.ufsc.lapesd.freqel.federation.planner.pre.steps;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.CartesianOp;
import br.ufsc.lapesd.freqel.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.modifiers.UnsafeMergeException;
import br.ufsc.lapesd.freqel.util.ref.IdentityHashSet;
import br.ufsc.lapesd.freqel.util.ref.RefSet;

import javax.annotation.Nonnull;
import java.util.Set;

import static br.ufsc.lapesd.freqel.util.CollectionUtils.hasIntersect;

public class CartesianDistributionStep extends AbstractDistributionStep {
    @Override
    public @Nonnull Op visit(@Nonnull Op parent, @Nonnull RefSet<Op> shared) {
        if (!(parent instanceof ConjunctionOp))
            return parent;
        // C(P(a{x}, b{y}), P(c{x}, d{z}), U(e, f), g{x}) --> C(P(acg, b, d), U(e, f))
        QueryOp queryOp = getQuery(parent, shared);
        if (queryOp == null) return parent;
        Set<String> queryVars = queryOp.getResultVars();
        MutableCQuery query = queryOp.getQuery();

        boolean hasExtra = false;
        RefSet<Op> merged = null;
        for (Op child : parent.getChildren()) {
            if (child instanceof CartesianOp && child.modifiers().isEmpty()) {
                for (Op grandchild : child.getChildren()) {
                    if (grandchild instanceof QueryOp
                            && hasIntersect(grandchild.getResultVars(), queryVars)) {
                        try {
                            query.mergeWith(((QueryOp) grandchild).getQuery());
                            if (merged == null) merged = new IdentityHashSet<>();
                            merged.add(grandchild);
                        } catch (UnsafeMergeException ignored) { }
                    }
                }
            } else if (child != queryOp) {
                hasExtra = true;
            }
        }
        if (merged == null)
            return parent; // no change
        queryOp.setQuery(query); //notify query changes
        queryOp.purgeCaches();
        return raiseProduct((ConjunctionOp) parent, queryOp, hasExtra, merged);
    }

    private @Nonnull Op raiseProduct(@Nonnull ConjunctionOp parent, QueryOp queryOp,
                                     boolean hasExtra, @Nonnull RefSet<Op> merged) {
        ConjunctionOp.Builder conjBuilder = hasExtra ? ConjunctionOp.builder() : null;
        CartesianOp.Builder prodBuilder = CartesianOp.builder().add(queryOp);
        for (Op child : parent.getChildren()) {
            if (child instanceof CartesianOp && child.modifiers().isEmpty()) {
                for (Op grandchild : child.getChildren()) {
                    if (!merged.contains(grandchild))
                        prodBuilder.add(grandchild);
                }
            } else if (child != queryOp) {
                assert conjBuilder != null;
                conjBuilder.add(child);
            }
        }
        parent.detachChildren();
        Op prod = prodBuilder.build();
        return conjBuilder != null ? conjBuilder.add(prod).build() : prod;
    }

}
