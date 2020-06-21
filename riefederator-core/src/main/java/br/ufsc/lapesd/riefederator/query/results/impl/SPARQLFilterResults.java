package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.DelegatingResults;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public class SPARQLFilterResults extends DelegatingResults implements Results {
    private static final Logger logger = LoggerFactory.getLogger(SPARQLFilterResults.class);

    private final @Nonnull List<SPARQLFilter> filters;
    private final @Nonnull ArrayDeque<Solution> ready = new ArrayDeque<>();
    private int included = 0, excluded = 0;

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull CQuery query) {
        List<SPARQLFilter> list = query.getModifiers().stream()
                .filter(SPARQLFilter.class::isInstance).map(m -> (SPARQLFilter)m).collect(toList());
        return list.isEmpty() ? in : new SPARQLFilterResults(in, list);
    }

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull PlanNode node) {
        ImmutableList<SPARQLFilter> list = ImmutableList.copyOf(node.getFilters());
        return list.isEmpty() ? in : new SPARQLFilterResults(in, list);
    }

    public SPARQLFilterResults(@Nonnull Results input,
                               @Nonnull Collection<SPARQLFilter> filters) {
        super(input.getVarNames(), input);
        if (SPARQLFilterResults.class.desiredAssertionStatus())
            checkArgument(new HashSet<>(filters).size() == filters.size());
        if (filters.isEmpty())
            logger.warn("Empty filters: SPARQLFilterResults will not filter anything");
        this.filters = filters instanceof List ? (List<SPARQLFilter>)filters
                                               : new ArrayList<>(filters);
    }

    public @Nonnull Results getIn() {
        return in;
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
