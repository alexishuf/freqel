package br.ufsc.lapesd.riefederator.federation.planner.phased;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Set;

@Immutable
public interface PlannerShallowStep {
    /**
     * Mutate or replace a node, without applying this same step in the {@link Op} children.
     *
     * @param op {@link Op} to be modified
     * @param locked Set of nodes that should not be mutated more than receiving new modifiers
     * @return op or a replacement for it
     */
    @Nonnull Op visit(@Nonnull Op op, @Nonnull Set<RefEquals<Op>> locked);
}
