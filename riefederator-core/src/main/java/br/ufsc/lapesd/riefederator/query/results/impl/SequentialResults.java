package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.NoSuchElementException;

import static java.util.stream.Collectors.toSet;

public class SequentialResults extends AbstractResults implements Results {
    private final @Nonnull ResultsList<? extends Results> results;
    private final @Nullable ArraySolution.ValueFactory projector;
    private final boolean distinct;
    private int idx = 0;

    private static @Nonnull Collection<String>
    getVarNames(@Nonnull Collection<? extends Results> results,
                @Nullable Collection<String> names) {
        return names != null ? names
                : results.stream().flatMap(r -> r.getVarNames().stream()).collect(toSet());
    }

    public SequentialResults(@Nonnull Collection<? extends Results> results,
                             @Nullable Collection<String> varNames) {
        super(getVarNames(results, varNames));
        this.results = ResultsList.of(results);
        this.projector = varNames == null ? null : ArraySolution.forVars(getVarNames());
        this.distinct = this.results.size() == 1 && this.results.get(0).isDistinct();
    }

    @Override
    public boolean isDistinct() {
        return distinct;
    }

    @Override
    public int getReadyCount() {
        if (idx >= results.size()) return 0;
        return results.get(idx).getReadyCount();
    }

    @Override
    public boolean hasNext() {
        while (idx < results.size()) {
            if (results.get(idx).hasNext())
                return true;
            else
                ++idx;
        }
        return false;
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();
        Solution next = results.get(idx).next();
        return projector == null ? next : projector.fromSolution(next);
    }

    @Override
    public void close() throws ResultsCloseException {
        results.close();
    }
}
