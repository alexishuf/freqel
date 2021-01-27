package br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.hash;

import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface HashJoinResultsFactory {
    /**
     * Create a {@link Results} that execute a hash-join of other two results
     *
     * @param left left operand of the join
     * @param right right operand of the join
     * @param joinVars Variables which must be equal on solutions from both sides to yield a match
     * @param resultVars Variables that should remain from the joined solution
     * @throws IllegalArgumentException if there are resultVars or joinVars not in the
     *                                  {@link Results#getVarNames()} from left nor right.
     * @return A new {@link Results} object that yields the join results. Closing this object
     *                               will also close the operands.
     */
    @Nonnull Results createResults(@Nonnull Results left, @Nonnull Results right,
                                   @Nonnull Collection<String> joinVars,
                                   @Nonnull Collection<String> resultVars);
}
