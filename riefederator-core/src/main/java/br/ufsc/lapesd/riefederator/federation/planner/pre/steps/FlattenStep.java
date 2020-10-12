package br.ufsc.lapesd.riefederator.federation.planner.pre.steps;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.riefederator.algebra.inner.PipeOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerShallowStep;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.UnsafeMergeException;
import br.ufsc.lapesd.riefederator.util.ref.RefSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import static java.util.Collections.emptySet;

public class FlattenStep implements PlannerStep, PlannerShallowStep {
    private static Logger logger = LoggerFactory.getLogger(FlattenStep.class);

    @Override
    public @Nonnull Op plan(@Nonnull Op root, @Nonnull RefSet<Op> shared) {
        if (!(root instanceof InnerOp)) return root;
        InnerOp io = (InnerOp) root;
        boolean ioShared = shared.contains(io);
        Set<String> resultVars = io.getResultVars();
        try (TakenChildren children = io.takeChildren().setNoContentChange()) {
            for (ListIterator<Op> it = children.listIterator(); it.hasNext(); )
                it.set(plan(it.next(), shared));
            if (io instanceof ConjunctionOp || io instanceof CartesianOp || io instanceof UnionOp)
                flattenSameClass(shared, io, children, resultVars);
            if (io instanceof ConjunctionOp)
                mergeQueryOps(shared, children);
            if (!(io instanceof PipeOp) && children.size() == 1 && !ioShared)
                return children.get(0); // replace conj(a) with a
        } finally {
            assert io.assertTreeInvariants();
        }
        return io; //changed in-place
    }

    @Override public @Nonnull Op visit(@Nonnull Op op, @Nonnull RefSet<Op> shared) {
        if (!(op instanceof InnerOp)) return op;
        InnerOp io = (InnerOp) op;
        boolean ioShared = shared.contains(io);
        Set<String> resultVars = io.getResultVars();
        try (TakenChildren children = io.takeChildren().setNoContentChange()) {
            if (io instanceof ConjunctionOp || io instanceof CartesianOp || io instanceof UnionOp)
                flattenSameClass(shared, io, children, resultVars);
            if (io instanceof ConjunctionOp)
                mergeQueryOps(shared, children);
            if (!(io instanceof PipeOp) && children.size() == 1 && !ioShared)
                return children.get(0); // replace conj(a) with a
        } finally {
            assert io.assertTreeInvariants();
        }
        return io; //changed in-place
    }

    public boolean isFlat(@Nonnull Op op, @Nonnull RefSet<Op> shared) {
        List<Op> list = op.getChildren();
        if (op instanceof ConjunctionOp
                && list.stream().anyMatch(c -> c instanceof ConjunctionOp
                && !shared.contains(c))) {
            return false;
        }
        if (list.size() == 1 && !shared.contains(op))
            return false; // tree should've been replaced with the single child
        if ((op instanceof CartesianOp || op instanceof ConjunctionOp)
                && list.stream().filter(c -> c instanceof QueryOp
                && c.modifiers().optional() == null
                && !shared.contains(c)).count() > 1) {
            return false; //all non-shared query nodes under ×, or . should've been merged
        }
        // check same-class nesting:
        if (op instanceof CartesianOp && list.stream().anyMatch(CartesianOp.class::isInstance))
            return false;
        if (op instanceof ConjunctionOp && list.stream().anyMatch(ConjunctionOp.class::isInstance))
            return false;
        if (op instanceof UnionOp && list.stream().anyMatch(UnionOp.class::isInstance))
            return false;
        return list.stream().allMatch(c -> isFlat(c, shared)); //recurse
    }

    @Override public String toString() {
        return getClass().getSimpleName();
    }

    /* --- --- --- Internals --- --- ---   */

    private void flattenSameClass(@Nonnull RefSet<Op> shared, InnerOp parent,
                                  @Nonnull TakenChildren children,
                                  @Nonnull Set<String> parentVars) {
        boolean parentProjected = parent.modifiers().projection() != null;
        Class<? extends InnerOp> parentClass = parent.getClass();
        for (int i = 0, size = children.size(); i < size; i++) {
            Op c = children.get(i);
            if (c.getClass().equals(parentClass) && !shared.contains(c)) {
                Iterator<Op> it = c.getChildren().iterator();
                if (it.hasNext()) {
                    try {
                        Set<String> fallback = parentProjected ? emptySet() : c.getResultVars();
                        parent.modifiers().safeMergeWith(c.modifiers(), parentVars, fallback);
                        children.set(i, it.next());
                        while (it.hasNext())
                            children.add(++i, it.next());
                        size += c.getChildren().size() - 1;
                    } catch (UnsafeMergeException e) {
                        // c remains unchanged in the tree
                    }
                } else { // no children in Conj/Union/Cart
                    children.remove(i--);
                    --size;
                }
            }
        }
    }

    private void mergeQueryOps(@Nonnull RefSet<Op> sharedNodes, TakenChildren children) {
        QueryOp queryOp = null;
        for (Iterator<Op> it = children.iterator(); it.hasNext(); ) {
            Op child = it.next();
            if (child instanceof QueryOp && !sharedNodes.contains(child)) {
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
