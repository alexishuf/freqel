package br.ufsc.lapesd.riefederator.federation.planner;

import br.ufsc.lapesd.riefederator.algebra.Op;

import javax.annotation.Nonnull;

/**
 * A planner that takes an executable plan as input and perform final optimizations on the tree.
 */
public interface PostPlanner {
    /**
     * Mutate the given tree with optimizations.
     *
     * @param plan The root of an executable plan
     * @return The same root or a replacement (if required)
     */
    @Nonnull Op plan(@Nonnull Op plan);
}
