package br.ufsc.lapesd.riefederator.federation.planner.post.steps;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.PipeOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.riefederator.util.ListIdentityMultimap;
import br.ufsc.lapesd.riefederator.util.RefSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;

public class PipeCleanerStep implements PlannerStep {
    /**
     * Find nodes that are referred by one or more {@link PipeOp}s and if all {@link PipeOp}s
     * have the same modifiers, pushes all modifiers to the node and replace the {@link PipeOp}s
     * with the node directly
     *
     * @param root the input plan (or query)
     * @param locked a set of nodes (by reference) that should not be replaced or altered beyond
     *               addition of modifiers.
     * @return root, unless it was a {@link PipeOp} removed
     */
    @Override
    public @Nonnull Op plan(@Nonnull Op root, @Nonnull RefSet<Op> locked) {
        ListIdentityMultimap<Op, PipeOp> n2pipe = findPipes(root);
        if (n2pipe == null)
            return root; // no work
        removeKeysWithDistinctPipes(n2pipe);
        removeLockedPipeOps(locked, n2pipe);
        return TreeUtils.replaceNodes(root, null, o -> {
            if (!(o instanceof PipeOp)) return o;
            assert o.getChildren().size() == 1;
            Op child = o.getChildren().get(0);
            return n2pipe.containsKey(child) ? child : o;
        });
    }

    @Override
    public @Nonnull String toString() {
        return getClass().getSimpleName();
    }

    private void removeLockedPipeOps(@Nonnull RefSet<Op> locked,
                                     @Nonnull ListIdentityMultimap<Op, PipeOp> n2pipe) {
        for (Op pipe : locked) {
            if (pipe instanceof PipeOp)
                n2pipe.removeValue(pipe.getChildren().get(0), (PipeOp) pipe);
        }
    }

    private @Nullable ListIdentityMultimap<Op, PipeOp> findPipes(@Nonnull Op root) {
        ListIdentityMultimap<Op, PipeOp> n2pipe = null;
        for (Iterator<Op> it = TreeUtils.iteratePreOrder(root); it.hasNext(); ) {
            Op op = it.next();
            if (!(op instanceof PipeOp))
                continue;
            PipeOp pipe = (PipeOp) op;
            assert pipe.getChildren().size() == 1;
            Op child = pipe.getChildren().get(0);
            if (n2pipe == null)
                n2pipe = new ListIdentityMultimap<>();
            n2pipe.putValue(child, pipe);
        }
        return n2pipe;
    }

    private void removeKeysWithDistinctPipes(@Nonnull ListIdentityMultimap<Op, PipeOp> n2pipe) {
        outer:
        for (Iterator<Op> it = n2pipe.keySet().iterator(); it.hasNext(); ) {
            Op op = it.next();
            ModifiersSet prev = null;
            for (PipeOp pipe : n2pipe.get(op)) {
                if (prev == null) {
                    prev = pipe.modifiers();
                } else if (!pipe.modifiers().equals(prev)) {
                    it.remove();
                    continue outer;
                }
            }
            assert prev != null;
            op.modifiers().unsafeMergeWith(prev, op.getPublicVars(), op.getPublicVars());
        }
    }
}
