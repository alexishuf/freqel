package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.ResultsList;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class SequentialResultsExecutor implements ResultsExecutor {
    @Override
    public @Nonnull Results async(@Nonnull Collection<? extends Results> coll) {
        Set<String> names = coll.stream().flatMap(r -> r.getVarNames().stream()).collect(toSet());
        return new SequentialResults(ResultsList.of(coll), names);
    }

    @Override
    public void close() {
        /* nothing to do */
    }
}
