package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.Results;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.query.error.ResultsCloseException;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.Set;

public class ProjectedResults implements Results {
    private final @Nonnull Results input;
    private final @Nonnull Set<String> varNames;

    public ProjectedResults(@Nonnull Results input, @Nonnull Set<String> varNames) {
        Preconditions.checkArgument(input.getVarNames().containsAll(varNames),
                "There are projection variables missing in the input");
        this.input = input;
        this.varNames = varNames;
    }

    @Override
    public int getReadyCount() {
        return input.getReadyCount();
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public @Nonnull Solution next() {
        Solution next = input.next();
        MapSolution.Builder builder = MapSolution.builder();
        for (String name : varNames) {
            Term term = next.get(name);
            if (term != null)
                builder.put(name, term);
        }
        return builder.build();
    }

    @Override
    public @Nonnull Cardinality getCardinality() {
        return input.getCardinality();
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }

    @Override
    public void close() throws ResultsCloseException {
        input.close();
    }
}
