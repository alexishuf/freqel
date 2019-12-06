package br.ufsc.lapesd.riefederator.query;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Iterator;
import java.util.Set;

@NotThreadSafe
public interface SolutionIterator extends Iterator<Solution> {
    /**
     * Number of solutions in-memory and ready for consumption with minimal delay.
     */
    int getReadyCount();

    @Override
    @Nonnull Solution next();

    /**
     * Get the cardinality of this iterator.
     */
    @Nonnull Cardinality getCardinality();

    /**
     * Set of variable names (the x in ?x) that {@link Solution}s from next() may contain.
     *
     * @return unmodifiable {@link Set} with var names
     */
    @Nonnull Set<String> getVarNames();
}
