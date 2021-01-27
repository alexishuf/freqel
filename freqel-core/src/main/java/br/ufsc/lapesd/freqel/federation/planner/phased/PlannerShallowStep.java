package br.ufsc.lapesd.freqel.federation.planner.phased;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.util.ref.RefSet;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public interface PlannerShallowStep {
    /**
     * Mutate or replace a node, without applying this same step in the {@link Op} children.
     *
     * @param op {@link Op} to be modified
     * @param shared Set of nodes that should not be mutated more than receiving new modifiers
     * @return op or a replacement for it
     */
    @Nonnull Op visit(@Nonnull Op op, @Nonnull RefSet<Op> shared);
}
