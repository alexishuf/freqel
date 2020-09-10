package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.results.DelegatingResults;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;

public class WindowDistinctResults extends DelegatingResults {
    private static final Logger logger = LoggerFactory.getLogger(WindowDistinctResults.class);
    private static final int DEF_WINDOW_SIZE = 250000; // >= 20MiB, depends on solution contents

    private final @Nonnull LinkedHashSet<Solution> set = new LinkedHashSet<>();
    private @Nullable Solution next;
    private final int windowSize;
    private boolean warned;

    public WindowDistinctResults(@Nonnull Results in) {
        this(in, DEF_WINDOW_SIZE);
    }

    public WindowDistinctResults(@Nonnull Results in,
                                 int windowSize) {
        super(in.getVarNames(), in);
        this.windowSize = windowSize;
    }

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull CQuery query) {
        if (query.getModifiers().distinct() != null && !in.isDistinct())
            return new WindowDistinctResults(in);
        return in;
    }

    public static @Nonnull Results applyIfNotDistinct(@Nonnull Results in) {
        return in.isDistinct() ? in : new WindowDistinctResults(in);
    }

    @Override
    public boolean isDistinct() {
        return true;
    }

    @Override
    public int getReadyCount() {
        return in.getReadyCount() + (next != null ? 1 : 0);
    }

    @Override
    public boolean hasNext() {
        while (this.next == null && in.hasNext()) {
            Solution solution = in.next();
            if (set.add(solution)) {
                this.next = solution;
                if (set.size() > windowSize) { //remove oldest entry
                    if (!warned) {
                        warned = true;
                        logger.warn("{} reached window limit of {}. Will discard older entries",
                                    this, windowSize);
                    }
                    Iterator<Solution> it = set.iterator();
                    assert it.hasNext();
                    it.next();
                    it.remove();
                }
            }
        }
        return this.next != null;
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();
        Solution solution = this.next;
        assert solution != null;
        this.next = null;
        return solution;
    }
}
