package br.ufsc.lapesd.freqel.query.modifiers.filter;

import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.results.Solution;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface SPARQLFilterExecutor {
    /**
     * Evaluate the filter given assignment for variables.
     *
     * @param filter the filter to evaluate. If null will return true (as if it were satisfied).
     * @param solution A set of values ({@link Term}s) associated to varables. The variables
     *                 of the solution are associated to the {@link Term}s given in the
     *                 constructor which are then mapped to actual variables in the
     *                 filter expression.
     * @return true iff the variable assignment from solution satisfies the filter
     */
    boolean evaluate(@Nullable SPARQLFilter filter, @Nonnull Solution solution);
}
