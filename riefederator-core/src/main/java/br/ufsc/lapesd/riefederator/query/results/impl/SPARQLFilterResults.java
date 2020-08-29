package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilterExecutor;
import br.ufsc.lapesd.riefederator.query.results.DelegatingResults;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashSet;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkArgument;

public class SPARQLFilterResults extends DelegatingResults implements Results {
    private static final Logger logger = LoggerFactory.getLogger(SPARQLFilterResults.class);

    private final @Nonnull Collection<SPARQLFilter> filters;
    private final @Nonnull SPARQLFilterExecutor filterExecutor = new SPARQLFilterExecutor();
    private final @Nonnull ArrayDeque<Solution> ready = new ArrayDeque<>();
    private int included = 0, excluded = 0;

    public static @Nonnull Results applyIf(@Nonnull Results in,
                                           @Nonnull Collection<SPARQLFilter> filters) {
        return filters.isEmpty() ? in : new SPARQLFilterResults(in, filters);
    }

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull CQuery query) {
        return applyIf(in, query.getModifiers().filters());
    }

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull Op node) {
        ImmutableList<SPARQLFilter> list = ImmutableList.copyOf(node.modifiers().filters());
        return list.isEmpty() ? in : new SPARQLFilterResults(in, list);
    }

    public SPARQLFilterResults(@Nonnull Results input,
                               @Nonnull Collection<SPARQLFilter> filters) {
        super(input.getVarNames(), input);
        if (SPARQLFilterResults.class.desiredAssertionStatus())
            checkArgument(new HashSet<>(filters).size() == filters.size());
        if (filters.isEmpty())
            logger.warn("Empty filters: SPARQLFilterResults will not filter anything");
        this.filters = filters;
    }

    public @Nonnull Results getIn() {
        return in;
    }

    public @Nonnull Collection<SPARQLFilter> getFilters() {
        return filters;
    }

    public int getIncluded() {
        return included;
    }

    public int getExcluded() {
        return excluded;
    }

    @Override
    public int getReadyCount() {
        return ready.size();
    }

    /**
     * Consumes items from the underlying {@link Results} object until it is exhausted or
     * a result is found. If the {@link Results} object has
     * {@link Results#getReadyCount()}<code> > 0</code>, consume all such items and evaluate the
     * filters on them.
     *
     * @return number of novel results found and placed in the
     *         {@link SPARQLFilterResults#ready} queue.
     */
    private int filter() {
        int found = 0, minConsumption = in.getReadyCount();
        outer:
        for (int i = 0; in.hasNext() && (i < minConsumption || found == 0); i++) {
            Solution solution = in.next();
            for (SPARQLFilter filter : filters) {
                if (!filterExecutor.evaluate(filter, solution)) {
                    ++excluded;
                    continue outer;
                }
            }
            ++included;
            ready.add(solution);
            ++found;
        }
        return found;
    }

    @Override
    public boolean hasNext() {
        return !ready.isEmpty() || filter() > 0;
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext()) throw new NoSuchElementException("No more Solutions!");
        return ready.remove();
    }

    @Override
    public String toString() {
        return String.format("SPARQLFilterResults@%x{node=%s, incl=%d, exc=%d, filters=%s, in=%s}",
                System.identityHashCode(this), getNodeName(),
                getIncluded(), getExcluded(), getFilters(), getIn());
    }
}
