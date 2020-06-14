package br.ufsc.lapesd.riefederator.linkedator.strategies;

import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.linkedator.LinkedatorResult;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.Collection;

public interface LinkedatorStrategy {
    /**
     * Generate suggestions of links from the given sources.
     *
     * @param sources sources to evaluate.
     * @return a non-null set of {@link LinkedatorResult} instances.
     */
    @CheckReturnValue
    @Nonnull Collection<LinkedatorResult> getSuggestions(@Nonnull Collection<Source> sources);
}
