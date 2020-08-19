package br.ufsc.lapesd.riefederator.federation.planner.outer;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Set;

@Immutable
public interface OuterPlannerStep {
    /**
     * Mutates the tree (in-place) to achieve a specific goal (e.g., applying some optimization).
     *
     * @param root the input plan (or query)
     * @param locked a set of nodes (by reference) that should not be replaced or altered beyond
     *               addition of modifiers.
     * @return the same tree root or a replacement Op if the root had to be replaced.
     */
    @Nonnull Op plan(@Nonnull Op root, @Nonnull Set<RefEquals<Op>> locked);
}
