package br.ufsc.lapesd.riefederator.federation.planner.post.steps;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.PipeOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.federation.planner.phased.PlannerStep;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
    public @Nonnull Op plan(@Nonnull Op root, @Nonnull Set<RefEquals<Op>> locked) {
        ListMultimap<RefEquals<Op>, PipeOp> n2pipe = findPipes(root);
        if (n2pipe == null)
            return root; // no work
        removeKeysWithDistinctPipes(n2pipe);
        removeLockedPipeOps(locked, n2pipe);
        return TreeUtils.replaceNodes(root, null, o -> {
            if (!(o instanceof PipeOp)) return o;
            assert o.getChildren().size() == 1;
            Op child = o.getChildren().get(0);
            return n2pipe.containsKey(RefEquals.of(child)) ? child : o;
        });
    }

    private void removeLockedPipeOps(@Nonnull Set<RefEquals<Op>> locked,
                                    @Nonnull ListMultimap<RefEquals<Op>, PipeOp> n2pipe) {
        for (RefEquals<Op> ref : locked) {
            Op pipe = ref.get();
            if (pipe instanceof PipeOp)
                n2pipe.remove(RefEquals.of(pipe.getChildren().get(0)), pipe);
        }
    }

    private @Nullable ListMultimap<RefEquals<Op>, PipeOp> findPipes(@Nonnull Op root) {
        ListMultimap<RefEquals<Op>, PipeOp> n2pipe = null;
        for (Iterator<Op> it = TreeUtils.iteratePreOrder(root); it.hasNext(); ) {
            Op op = it.next();
            if (!(op instanceof PipeOp))
                continue;
            PipeOp pipe = (PipeOp) op;
            assert pipe.getChildren().size() == 1;
            RefEquals<Op> child = RefEquals.of(pipe.getChildren().get(0));
            (n2pipe == null ? n2pipe = ArrayListMultimap.create() : n2pipe).put(child, pipe);
        }
        return n2pipe;
    }

    private void removeKeysWithDistinctPipes(@Nonnull ListMultimap<RefEquals<Op>, PipeOp> n2pipe) {
        List<RefEquals<Op>> dropKeys = new ArrayList<>();
        outer:
        for (RefEquals<Op> op : n2pipe.keySet()) {
            ModifiersSet prev = null;
            for (PipeOp pipe : n2pipe.get(op)) {
                if (prev == null) {
                    prev = pipe.modifiers();
                } else if (!pipe.modifiers().equals(prev)) {
                    dropKeys.add(op);
                    continue outer;
                }
            }
            assert prev != null;
            op.get().modifiers().unsafeMergeWith(prev, op.get().getPublicVars(),
                                                 op.get().getPublicVars());
        }
        for (RefEquals<Op> k : dropKeys)
            n2pipe.removeAll(k);
    }
}
