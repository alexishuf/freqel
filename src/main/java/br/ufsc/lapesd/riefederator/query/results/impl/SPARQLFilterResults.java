package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public class SPARQLFilterResults implements Results {
    private static final Logger logger = LoggerFactory.getLogger(SPARQLFilterResults.class);

    private final @Nonnull Results input;
    private final @Nonnull List<SPARQLFilter> filters;
    private final @Nonnull ArrayDeque<Solution> ready = new ArrayDeque<>();
    private int included = 0, excluded = 0;

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull CQuery query) {
        List<SPARQLFilter> list = query.getModifiers().stream()
                .filter(SPARQLFilter.class::isInstance).map(m -> (SPARQLFilter)m).collect(toList());
        return list.isEmpty() ? in : new SPARQLFilterResults(in, list);
    }

    public SPARQLFilterResults(@Nonnull Results input,
                               @Nonnull Collection<SPARQLFilter> filters) {
        this.input = input;
        if (SPARQLFilterResults.class.desiredAssertionStatus())
            checkArgument(new HashSet<>(filters).size() == filters.size());
        if (filters.isEmpty())
            logger.warn("Empty filters: SPARQLFilterResults will not filter anything");
        this.filters = filters instanceof List ? (List<SPARQLFilter>)filters
                                               : new ArrayList<>(filters);
    }

    public @Nonnull Results getInput() {
        return input;
    }

    public @Nonnull List<SPARQLFilter> getFilters() {
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
        if (input.getReadyCount() > 0)
            filter(true);
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
    private int filter(boolean onlyReady) {
        int found = 0, minConsumption = input.getReadyCount();
        outer:
        for (int i = 0; input.hasNext() && (i < minConsumption || onlyReady || found == 0); i++) {
            Solution solution = input.next();
            for (SPARQLFilter filter : filters) {
                if (!filter.evaluate(solution)) {
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
        return !ready.isEmpty() || filter(false) > 0;
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext()) throw new NoSuchElementException("No more Solutions!");
        return ready.remove();
    }

    @Override
    public @Nonnull Cardinality getCardinality() {
        Cardinality inner = this.input.getCardinality();
        Cardinality.Reliability r = inner.getReliability();
        if (r.ordinal() <= Cardinality.Reliability.NON_EMPTY.ordinal())
            return inner;
        return new Cardinality(r, inner.getValue(0) + getReadyCount());
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return input.getVarNames();
    }

    @Override
    public void close() throws ResultsCloseException {
        ready.clear();
        input.close();
    }

    @Override
    public String toString() {
        return String.format("SPARQLFilterResults{incl=%d, exc=%d, filters=%s, inner=%s}",
                getIncluded(), getExcluded(), getFilters(), getInput());
    }
}
