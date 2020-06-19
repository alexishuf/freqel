package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.DelegatingResults;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public class TransformedResults extends DelegatingResults implements Results {
    private final @Nonnull Function<Solution, Solution> op;

    public TransformedResults(@Nonnull Results in, @Nonnull Set<String> varNames,
                              @Nonnull Function<Solution, Solution> op) {
        super(varNames, in);
        this.op = op;
    }

    @Override
    public @Nonnull Solution next() {
        Solution solution = op.apply(in.next());
        assert getVarNames().equals(new HashSet<>(solution.getVarNames()));
        return solution;
    }
}
