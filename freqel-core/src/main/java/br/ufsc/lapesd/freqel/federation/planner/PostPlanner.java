package br.ufsc.lapesd.freqel.federation.planner;

import br.ufsc.lapesd.freqel.algebra.Op;

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
