package br.ufsc.lapesd.riefederator.federation.planner.pre.steps;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.UnsafeMergeException;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import static java.util.Collections.emptySet;

public class FlattenStep implements PlannerStep {
    private static Logger logger = LoggerFactory.getLogger(FlattenStep.class);

    @Override
    public @Nonnull Op plan(@Nonnull Op root, @Nonnull Set<RefEquals<Op>> locked) {
        if (!(root instanceof InnerOp)) return root;
        InnerOp io = (InnerOp) root;
        boolean ioShared = locked.contains(RefEquals.of(io));
        Set<String> resultVars = io.getResultVars();
        try (TakenChildren children = io.takeChildren().setNoContentChange()) {
            for (ListIterator<Op> it = children.listIterator(); it.hasNext(); )
                it.set(plan(it.next(), locked));
            if (io instanceof ConjunctionOp || io instanceof CartesianOp || io instanceof UnionOp)
                flattenSameClass(locked, io, children, resultVars);
            if (io instanceof ConjunctionOp)
                mergeQueryOps(locked, children);
            if (children.size() == 1 && !ioShared)
                return children.get(0); // replace conj(a) with a
        } finally {
            assert io.assertTreeInvariants();
        }
        return io; //changed in-place
    }

    public boolean isFlat(@Nonnull Op op, @Nonnull Set<RefEquals<Op>> locked) {
        List<Op> list = op.getChildren();
        if (op instanceof ConjunctionOp
                && list.stream().anyMatch(c -> c instanceof ConjunctionOp
                && !locked.contains(RefEquals.of(c)))) {
            return false;
        }
        if (list.size() == 1 && !locked.contains(RefEquals.of(op)))
            return false; // tree should've been replaced with the single child
        if ((op instanceof CartesianOp || op instanceof ConjunctionOp)
                && list.stream().filter(c -> c instanceof QueryOp
                && c.modifiers().optional() == null
                && !locked.contains(RefEquals.of(c))).count() > 1) {
            return false; //all non-locked query nodes under ×, or . should've been merged
        }
        // check same-class nesting:
        if (op instanceof CartesianOp && list.stream().anyMatch(CartesianOp.class::isInstance))
            return false;
        if (op instanceof ConjunctionOp && list.stream().anyMatch(ConjunctionOp.class::isInstance))
            return false;
        if (op instanceof UnionOp && list.stream().anyMatch(UnionOp.class::isInstance))
            return false;
        return list.stream().allMatch(c -> isFlat(c, locked)); //recurse
    }

    @Override public String toString() {
        return getClass().getSimpleName();
    }

    /* --- --- --- Internals --- --- ---   */

    private void flattenSameClass(@Nonnull Set<RefEquals<Op>> shared, InnerOp parent,
                                  @Nonnull TakenChildren children,
                                  @Nonnull Set<String> parentVars) {
        boolean parentProjected = parent.modifiers().projection() != null;
        Class<? extends InnerOp> parentClass = parent.getClass();
        for (int i = 0, size = children.size(); i < size; i++) {
            Op c = children.get(i);
            if (c.getClass().equals(parentClass) && !shared.contains(RefEquals.of(c))) {
                Iterator<Op> it = c.getChildren().iterator();
                if (it.hasNext()) {
                    try {
                        Set<String> fallback = parentProjected ? emptySet() : c.getResultVars();
                        parent.modifiers().safeMergeWith(c.modifiers(), parentVars, fallback);
                        children.set(i, it.next());
                        while (it.hasNext())
                            children.add(++i, it.next());
                        size += c.getChildren().size() - 1;
                    } catch (UnsafeMergeException e) { }
                } else {
                    children.remove(i--);
                    --size;
                }
            }
        }
    }

    private void mergeQueryOps(@Nonnull Set<RefEquals<Op>> sharedNodes, TakenChildren children) {
        QueryOp queryOp = null;
        for (Iterator<Op> it = children.iterator(); it.hasNext(); ) {
            Op child = it.next();
            if (child instanceof QueryOp && !sharedNodes.contains(RefEquals.of(child))) {
                if (queryOp == null) {
                    if (child.modifiers().optional() == null)
                        queryOp = (QueryOp) child;
                } else {
                    MutableCQuery q = queryOp.getQuery();
                    try {
                        if (q.mergeWith(((QueryOp) child).getQuery()))
                            queryOp.setQuery(q);
                        else
                            logger.warn("Query appears twice in a conjunction: {}", q);
                        it.remove();
                    } catch (UnsafeMergeException ignored) { }
                }
            }
        }
    }

}
