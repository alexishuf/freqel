package br.ufsc.lapesd.freqel.query.results.impl;

import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.freqel.query.modifiers.Projection;
import br.ufsc.lapesd.freqel.query.results.DelegatingResults;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;

import javax.annotation.Nonnull;
import java.util.Set;

public class ProjectingResults extends DelegatingResults implements Results {
    private final @Nonnull ArraySolution.ValueFactory factory;

    public ProjectingResults(@Nonnull Results delegate, @Nonnull Set<String> varNames) {
        super(varNames, delegate);
        this.factory = ArraySolution.forVars(varNames);
    }

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull ModifiersSet modifiers) {
        Projection projection = modifiers.projection();
        if (projection != null && !projection.getVarNames().equals(in.getVarNames()))
            return new ProjectingResults(in, projection.getVarNames());
        return in;
    }

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull CQuery query) {
        return applyIf(in, query.getModifiers());
    }

    @Override
    public @Nonnull Solution next() {
        return factory.fromSolution(in.next());
    }
}
