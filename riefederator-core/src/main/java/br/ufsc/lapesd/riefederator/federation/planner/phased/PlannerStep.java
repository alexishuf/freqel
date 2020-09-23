package br.ufsc.lapesd.riefederator.federation.planner.phased;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.util.ref.RefSet;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public interface PlannerStep {
    /**
     * Mutates the tree (in-place) to achieve a specific goal (e.g., applying some optimization).
     *
     * @param root the input plan (or query)
     * @param locked a set of nodes (by reference) that should not be replaced or altered beyond
     *               addition of modifiers.
     * @return the same tree root or a replacement Op if the root had to be replaced.
     */
    @Nonnull Op plan(@Nonnull Op root, @Nonnull RefSet<Op> locked);
}
