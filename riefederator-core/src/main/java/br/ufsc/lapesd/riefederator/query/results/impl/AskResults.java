package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.results.DelegatingResults;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.NoSuchElementException;

public class AskResults extends DelegatingResults implements Results {
    public AskResults(@Nonnull Results in) {
        super(Collections.emptySet(), in);
    }

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull CQuery query) {
        if (query.isAsk())
            return new AskResults(in);
        return in;
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();
        in.next();
        return ArraySolution.EMPTY;
    }
}
