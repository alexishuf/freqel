package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.model.Triple;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;

public interface TPEndpoint {
    /**
     * Return a {@link SolutionIterator} over the possible bindings for each variable in query.
     *
     * If qurey has no variables (it is fully bound), then the {@link SolutionIterator} will
     * have no result (<code>hasNext()==false</code>) if the endpoint does not contain the
     * triple. Else, the {@link SolutionIterator} will return a single {@link Solution} that has
     * no variables bound.
     *
     * @param query triple pattern
     * @return A new {@link SolutionIterator}
     */
    @Contract("_ -> new")
    @Nonnull SolutionIterator query(@Nonnull Triple query);
}
